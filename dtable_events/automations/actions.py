import json
import logging
import re
import time
import os
from copy import deepcopy
from datetime import datetime, date, timedelta
from queue import Full
from urllib.parse import unquote
from uuid import UUID

import jwt
import requests
from dateutil import parser

from seaserv import seafile_api
from dtable_events.automations.models import get_third_party_account
from dtable_events.app.metadata_cache_managers import BaseMetadataCacheManager
from dtable_events.app.event_redis import redis_cache
from dtable_events.app.config import DTABLE_WEB_SERVICE_URL, DTABLE_PRIVATE_KEY, \
    SEATABLE_FAAS_AUTH_TOKEN, SEATABLE_FAAS_URL, INNER_DTABLE_DB_URL
from dtable_events.dtable_io import send_wechat_msg, send_email_msg, send_dingtalk_msg, batch_send_email_msg
from dtable_events.page_design.manager import conver_page_to_pdf_manager
from dtable_events.notification_rules.notification_rules_utils import fill_msg_blanks_with_converted_row, \
    send_notification, fill_msg_blanks_with_sql_row
from dtable_events.utils import uuid_str_to_36_chars, is_valid_email, get_inner_dtable_server_url, \
    normalize_file_path, gen_file_get_url, gen_random_option
from dtable_events.utils.constants import ColumnTypes
from dtable_events.utils.dtable_server_api import DTableServerAPI
from dtable_events.utils.dtable_web_api import DTableWebAPI
from dtable_events.utils.dtable_db_api import DTableDBAPI, RowsQueryError, Request429Error
from dtable_events.notification_rules.utils import get_nickname_by_usernames
from dtable_events.utils.sql_generator import filter2sql, BaseSQLGenerator, ColumnFilterInvalidError
from dtable_events.utils.universal_app_api import UniversalAppAPI


logger = logging.getLogger(__name__)

PER_DAY = 'per_day'
PER_WEEK = 'per_week'
PER_UPDATE = 'per_update'
PER_MONTH = 'per_month'
CRON_CONDITIONS = (PER_DAY, PER_WEEK, PER_MONTH)
ALL_CONDITIONS = (PER_DAY, PER_WEEK, PER_MONTH, PER_UPDATE)

CONDITION_ROWS_MODIFIED = 'rows_modified'
CONDITION_ROWS_ADDED = 'rows_added'
CONDITION_FILTERS_SATISFY = 'filters_satisfy'
CONDITION_NEAR_DEADLINE = 'near_deadline'
CONDITION_PERIODICALLY = 'run_periodically'
CONDITION_PERIODICALLY_BY_CONDITION = 'run_periodically_by_condition'

MESSAGE_TYPE_AUTOMATION_RULE = 'automation_rule'

MINUTE_TIMEOUT = 60

NOTIFICATION_CONDITION_ROWS_LIMIT = 50
EMAIL_CONDITION_ROWS_LIMIT = 50
CONDITION_ROWS_LOCKED_LIMIT = 200
CONDITION_ROWS_UPDATE_LIMIT = 50
WECHAT_CONDITION_ROWS_LIMIT = 20
DINGTALK_CONDITION_ROWS_LIMIT = 20
CONVERT_PAGE_TO_PDF_ROWS_LIMIT = 50

AUTO_RULE_CALCULATE_TYPES = ['calculate_accumulated_value', 'calculate_delta', 'calculate_rank', 'calculate_percentage']


def email2list(email_str, split_pattern='[,，]'):
    email_list = [value.strip() for value in re.split(split_pattern, email_str) if value.strip()]
    return email_list


def is_number_format(column):
    calculate_col_type = column.get('type')
    if calculate_col_type in [ColumnTypes.NUMBER, ColumnTypes.DURATION, ColumnTypes.RATE]:
        return True
    elif calculate_col_type == ColumnTypes.FORMULA and column.get('data').get('result_type') == 'number':
        return True
    elif calculate_col_type == ColumnTypes.LINK_FORMULA:
        if column.get('data').get('result_type') == 'array' and column.get('data').get('array_type') == 'number':
            return True
        elif column.get('data').get('result_type') == 'number':
            return True
    return False


def is_int_str(num):
    return '.' not in str(num)


def convert_formula_number(value, column_data):
    decimal = column_data.get('decimal')
    thousands = column_data.get('thousands')
    precision = column_data.get('precision')
    if decimal == 'comma':
        # decimal maybe dot or comma
        value = value.replace(',', '.')
    if thousands == 'space':
        # thousands maybe space, dot, comma or no
        value = value.replace(' ', '')
    elif thousands == 'dot':
        value = value.replace('.', '')
        if precision > 0 or decimal == 'dot':
            value = value[:-precision] + '.' + value[-precision:]
    elif thousands == 'comma':
        value = value.replace(',', '')

    return value


def parse_formula_number(cell_data, column_data):
    """
    parse formula number to regular format
    :param cell_data: value of cell (e.g. 1.25, ￥12.0, $10.20, €10.2, 0:02 or 10%, etc)
    :param column_data: info of formula column
    """
    src_format = column_data.get('format')
    value = str(cell_data)
    if src_format in ['euro', 'dollar', 'yuan']:
        value = value[1:]
    elif src_format == 'percent':
        value = value[:-1]
    value = convert_formula_number(value, column_data)

    if src_format == 'percent' and isinstance(value, str):
        try:
            value = float(value) / 100
        except Exception as e:
            return 0
    try:
        if is_int_str(value):
            value = int(value)
        else:
            value = float(value)
    except Exception as e:
        return 0
    return value


def cell_data2str(cell_data):
    if isinstance(cell_data, list):
        cell_data.sort()
        return ' '.join(cell_data2str(item) for item in cell_data)
    elif cell_data is None:
        return ''
    else:
        return str(cell_data)


class BaseAction:

    def __init__(self, auto_rule, action_type, data=None):
        self.auto_rule = auto_rule
        self.action_type = action_type or 'base'
        self.data = data

    def do_action(self):
        pass

    def get_need_notify_columns(self, table_id):
        # columns = self.auto_rule.table_info['columns']
        tables = self.auto_rule.dtable_metadata.get('tables') or []
        table = None
        for tmp_table in tables:
            if tmp_table['_id'] == table_id:
                table = tmp_table
                break
        if not table:
            return []
        results = []
        for column in table['columns']:
            if column['type'] != ColumnTypes.COLLABORATOR:
                continue
            data = column.get('data') or {}
            if data.get('enable_send_notification'):
                results.append(column)
        return results

    def send_selected_collaborator_notis(self, row_data):
        """only some kinds of actions can call this
        """
        if self.action_type not in ['update_record', 'add_record', 'add_record_to_other_table']:
            return
        table_id = None
        notify_column_names = []
        if self.action_type in ['update_record', 'add_record']:
            table_id = self.auto_rule.table_info['_id']
            notify_column_names = [column['name'] for column in self.get_need_notify_columns(table_id)]
        elif self.action_type == 'add_record_to_other_table':
            table_id = self.dst_table_id
            notify_column_names = [column['name'] for column in self.get_need_notify_columns(table_id)]
        if not notify_column_names:
            return
        notify_users = []
        row_id = None
        if self.action_type == 'add_record':
            row_id = row_data['_id']
            for column_name, value in row_data.items():
                if column_name not in notify_column_names:
                     continue
                for user in value:
                    if user not in notify_users:
                        notify_users.append(user)
        elif self.action_type == 'update_record':
            converted_row = self.auto_rule.data.get('converted_row')
            row_id = converted_row['_id']
            for column_name, value in row_data.items():
                if column_name not in notify_column_names:
                    continue
                old_value = converted_row.get(column_name) or []
                for user in (set(value) - set(old_value)):
                    if user not in notify_users:
                        notify_users.append(user)
        elif self.action_type == 'add_record_to_other_table':
            row_id = row_data['_id']
            for column_name, value in row_data.items():
                if column_name not in notify_column_names:
                     continue
                for user in value:
                    if user not in notify_users:
                        notify_users.append(user)
        detail = {
            'author': 'Automation Rule',
            'row_id': row_id,
            'table_id': table_id
        }
        user_msg_list = []
        for user in notify_users:
            user_msg_list.append({
                'to_user': user,
                'msg_type': 'selected_collaborator',
                'detail': detail,
            })
        send_notification(self.auto_rule.dtable_uuid, user_msg_list, self.auto_rule.access_token)

    def parse_column_value(self, column, value):
        if column.get('type') == ColumnTypes.SINGLE_SELECT:
            select_options = column.get('data', {}).get('options', [])
            for option in select_options:
                if value == option.get('id'):
                    return option.get('name')

        elif column.get('type') == ColumnTypes.MULTIPLE_SELECT:
            m_select_options = column.get('data', {}).get('options', [])
            if isinstance(value, list):
                parse_value_list = []
                for option in m_select_options:
                    if option.get('id') in value:
                        option_name = option.get('name')
                        parse_value_list.append(option_name)
                return parse_value_list
        elif column.get('type') == ColumnTypes.DATE:
            if value and isinstance(value, str):
                date_value = parser.isoparse(value)
                date_format = column['data']['format']
                if date_format == 'YYYY-MM-DD':
                    return date_value.strftime('%Y-%m-%d')
                return date_value.strftime('%Y-%m-%d %H:%M')
        elif column.get('type') in [ColumnTypes.CTIME, ColumnTypes.MTIME]:
            if value and isinstance(value, str):
                date_value = parser.isoparse(value)
                return date_value.strftime('%Y-%m-%d %H:%M:%S')
        else:
            return value


class UpdateAction(BaseAction):

    VALID_COLUMN_TYPES = [
        ColumnTypes.TEXT,
        ColumnTypes.DATE,
        ColumnTypes.LONG_TEXT,
        ColumnTypes.CHECKBOX,
        ColumnTypes.SINGLE_SELECT,
        ColumnTypes.MULTIPLE_SELECT,
        ColumnTypes.URL,
        ColumnTypes.DURATION,
        ColumnTypes.NUMBER,
        ColumnTypes.COLLABORATOR,
        ColumnTypes.EMAIL,
        ColumnTypes.RATE,
    ]

    def __init__(self, auto_rule, action_type, data, updates):
        """
        auto_rule: instance of AutomationRule
        data: if auto_rule.PER_UPDATE, data is event data from redis
        updates: {'col_1_name: ', value1, 'col_2_name': value2...}
        """
        super().__init__(auto_rule, action_type, data)
        self.updates = updates or {}
        self.update_data = {
            'row': {},
            'table_name': self.auto_rule.table_info['name'],
            'row_id': ''
        }
        self.col_name_dict = {}
        self.init_updates()

    def add_or_create_options(self, column, value):
        table_name = self.update_data['table_name']
        
        select_options = column.get('data', {}).get('options', [])
        for option in select_options:
            if value == option.get('name'):
                return value
        self.auto_rule.dtable_server_api.add_column_options(
            table_name,
            column['name'],
            options = [gen_random_option(value)]
        )
        self.auto_rule.cache_clean()
        
        return value

    def format_time_by_offset(self, offset, format_length):
        cur_datetime = datetime.now()
        cur_datetime_offset = cur_datetime + timedelta(days=offset)
        if format_length == 2:
            return cur_datetime_offset.strftime("%Y-%m-%d %H:%M")
        if format_length == 1:
            return cur_datetime_offset.strftime("%Y-%m-%d")

    def fill_msg_blanks(self, row, text, blanks):
        col_name_dict = self.col_name_dict
        db_session, dtable_metadata = self.auto_rule.db_session, self.auto_rule.dtable_metadata
        return fill_msg_blanks_with_converted_row(text, blanks, col_name_dict, row, db_session)

    
    def fill_msg_blanks_with_sql(self, row, text, blanks):
        col_name_dict = self.col_name_dict
        db_session, dtable_metadata = self.auto_rule.db_session, self.auto_rule.dtable_metadata
        return fill_msg_blanks_with_sql_row(text, blanks, col_name_dict, row, db_session)


    def formate_update_datas(self, converted_row, row, fill_msg_blank_func):
        src_row = converted_row
        # filter columns in view and type of column is in VALID_COLUMN_TYPES
        filtered_updates = {}
        for col in self.auto_rule.table_info['columns']:
            if col.get('type') not in self.VALID_COLUMN_TYPES:
                continue
            col_name = col.get('name')
            col_key = col.get('key')
            col_type = col.get('type')
            if col_key in self.updates.keys():
                if col_type == ColumnTypes.DATE:
                    time_format = col.get('data', {}).get('format', '')
                    format_length = len(time_format.split(" "))
                    try:
                        time_dict = self.updates.get(col_key)
                        set_type = time_dict.get('set_type')
                        if set_type == 'specific_value':
                            time_value = time_dict.get('value')
                            filtered_updates[col_name] = time_value
                        elif set_type == 'relative_date':
                            offset = time_dict.get('offset')
                            filtered_updates[col_name] = self.format_time_by_offset(int(offset), format_length)
                        elif set_type == 'date_column':
                            col_key = time_dict.get('date_column_key')
                            col = self.col_key_dict.get(col_key)
                            value = src_row.get(col['name'])
                            filtered_updates[col_name] = value
                        elif set_type == 'set_empty':
                            filtered_updates[col_name] = None
                    except Exception as e:
                        logger.error(e)
                        filtered_updates[col_name] = self.updates.get(col_key)
                elif col_type == ColumnTypes.SINGLE_SELECT:
                    try:
                        data_dict = self.updates.get(col_key)
                        if not data_dict:
                            continue
                        if isinstance(data_dict, dict):
                            set_type = data_dict.get('set_type')
                            if set_type == 'default':
                                value = data_dict.get('value')
                                filtered_updates[col_name] = self.parse_column_value(col, value)
                            elif set_type == 'column':
                                src_col_key = data_dict.get('value')
                                src_col = self.col_key_dict.get(src_col_key)
                                value = src_row.get(src_col['name'])
                                if value:
                                    filtered_updates[col_name] = self.add_or_create_options(col, value)
                            elif set_type == 'set_empty':
                                filtered_updates[col_name] = None
                        else:
                            value = data_dict  # compatible with the old data strcture
                            filtered_updates[col_name] = self.parse_column_value(col, value)

                    except Exception as e:
                        logger.error(e)
                        filtered_updates[col_name] = self.updates.get(col_key)

                elif col_type == ColumnTypes.COLLABORATOR:
                    try:
                        data_dict = self.updates.get(col_key)
                        if not data_dict:
                            continue
                        if isinstance(data_dict, dict):
                            set_type = data_dict.get('set_type')
                            if set_type == 'default':
                                value = data_dict.get('value')
                                filtered_updates[col_name] = self.parse_column_value(col, value)
                            elif set_type == 'column':
                                src_col_key = data_dict.get('value')
                                src_col = self.col_key_dict.get(src_col_key)
                                value = src_row.get(src_col['name'])
                                if not isinstance(value, list):
                                    value = [value, ]
                                filtered_updates[col_name] = value
                            elif set_type == 'set_empty':
                                filtered_updates[col_name] = None
                        else:
                            value = data_dict  # compatible with the old data strcture
                            filtered_updates[col_name] = self.parse_column_value(col, value)

                    except Exception as e:
                        logger.error(e)
                        filtered_updates[col_name] = self.updates.get(col_key)

                elif col_type in [
                    ColumnTypes.NUMBER,
                ]:
                    try:
                        data_dict = self.updates.get(col_key)
                        if not data_dict:
                            continue
                        if isinstance(data_dict, dict):
                            set_type = data_dict.get('set_type')
                            if set_type == 'default':
                                value = data_dict.get('value')
                                filtered_updates[col_name] = self.parse_column_value(col, value)
                            elif set_type == 'column':
                                src_col_key = data_dict.get('value')
                                src_col = self.col_key_dict.get(src_col_key)
                                value = src_row.get(src_col['name'])
                                filtered_updates[col_name] = value
                            elif set_type == 'set_empty':
                                filtered_updates[col_name] = None
                        else:
                            value = data_dict  # compatible with the old data strcture
                            filtered_updates[col_name] = self.parse_column_value(col, value)

                    except Exception as e:
                        logger.error(e)
                        filtered_updates[col_name] = self.updates.get(col_key)
                else:
                    cell_value = self.updates.get(col_key)
                    if isinstance(cell_value, str):
                        blanks = set(re.findall(r'\{([^{]*?)\}', cell_value))
                        column_blanks = [blank for blank in blanks if blank in self.col_name_dict]
                        cell_value = fill_msg_blank_func(row, cell_value, column_blanks)
                    filtered_updates[col_name] = self.parse_column_value(col, cell_value)
        return filtered_updates
        
    def init_updates(self):
        self.col_name_dict = {col.get('name'): col for col in self.auto_rule.table_info['columns']}
        self.col_key_dict = {col.get('key'):  col for col in self.auto_rule.table_info['columns']}
        if not self.data:
            return None
        src_row = self.data.get('converted_row', None)
        if not src_row:
            return None
        filtered_updates = self.formate_update_datas(src_row, src_row, self.fill_msg_blanks)
        self.update_data['row'] = filtered_updates
        self.update_data['row_id'] = src_row.get('_id')

    def can_do_action(self):
        if not self.update_data.get('row') or not self.update_data.get('row_id'):
            return False

        # if columns in self.updates was updated, forbidden action!!!
        updated_column_keys = self.data.get('updated_column_keys', [])
        to_update_keys = [col['key'] for col in self.auto_rule.table_info['columns'] if col['name'] in self.updates]
        for key in updated_column_keys:
            if key in to_update_keys:
                return False
        return True
    
    def per_update(self):
        table_name = self.auto_rule.table_info['name']
        try:
            self.auto_rule.dtable_server_api.update_row(table_name, self.data['row']['_id'], self.update_data['row'])
        except Exception as e:
            logger.error('update dtable: %s, error: %s', self.auto_rule.dtable_uuid, e)
            return
        self.auto_rule.set_done_actions()
        try:
            self.send_selected_collaborator_notis(self.update_data['row'])
        except Exception as e:
            logger.exception('send selected notifications error: %s', e)


    def condition_cron_update(self):
        triggered_rows = self.auto_rule.get_trigger_conditions_rows(self, warning_rows=CONDITION_ROWS_UPDATE_LIMIT)[:CONDITION_ROWS_UPDATE_LIMIT]
        batch_update_list = []
        for row in triggered_rows:
            converted_row = {self.col_key_dict.get(key).get('name') if self.col_key_dict.get(key) else key:
                             self.parse_column_value(self.col_key_dict.get(key), row.get(key)) if self.col_key_dict.get(key) else row.get(key)
                             for key in row}
            batch_update_list.append({
                'row': self.formate_update_datas(converted_row, row, self.fill_msg_blanks_with_sql),
                'row_id': row.get('_id')
            })
        table_name = self.auto_rule.table_info['name']
        try:
            self.auto_rule.dtable_server_api.batch_update_rows(table_name, batch_update_list)
        except Exception as e:
            logger.error('update dtable: %s, error: %s', self.auto_rule.dtable_uuid, e)
            return

    def do_action(self):
        if self.auto_rule.run_condition == PER_UPDATE:
            if not self.can_do_action():
                return
            self.per_update()
        elif self.auto_rule.run_condition in CRON_CONDITIONS:
            if self.auto_rule.trigger.get('condition') == CONDITION_PERIODICALLY_BY_CONDITION:
                self.condition_cron_update()
            
        self.auto_rule.set_done_actions()

class LockRowAction(BaseAction):


    def __init__(self, auto_rule, action_type, data, trigger):
        """
        auto_rule: instance of AutomationRule
        data: if auto_rule.PER_UPDATE, data is event data from redis
        updates: {'col_1_name: ', value1, 'col_2_name': value2...}
        """
        super().__init__(auto_rule, action_type, data)
        self.update_data = {
            'table_name': self.auto_rule.table_info['name'],
            'row_ids':[],
        }
        self.trigger = trigger
        self.init_updates()

    def init_updates(self):
        # filter columns in view and type of column is in VALID_COLUMN_TYPES
        if self.auto_rule.run_condition == PER_UPDATE:
            row_id = self.data['row']['_id']
            self.update_data['row_ids'].append(row_id)

        if self.auto_rule.run_condition in CRON_CONDITIONS:
            rows_data = self.auto_rule.get_trigger_conditions_rows(self, warning_rows=CONDITION_ROWS_LOCKED_LIMIT)[:CONDITION_ROWS_LOCKED_LIMIT]
            for row in rows_data:
                self.update_data['row_ids'].append(row.get('_id'))

    def can_do_action(self):
        if not self.update_data.get('row_ids'):
            return False

        return True

    def do_action(self):
        if not self.can_do_action():
            return
        table_name = self.auto_rule.table_info['name']
        try:
            self.auto_rule.dtable_server_api.lock_rows(table_name, self.update_data.get('row_ids'))
        except Exception as e:
            logger.error('lock dtable: %s, error: %s', self.auto_rule.dtable_uuid, e)
            return
        else:
            self.auto_rule.set_done_actions()

class AddRowAction(BaseAction):

    VALID_COLUMN_TYPES = [
        ColumnTypes.TEXT,
        ColumnTypes.DATE,
        ColumnTypes.LONG_TEXT,
        ColumnTypes.CHECKBOX,
        ColumnTypes.SINGLE_SELECT,
        ColumnTypes.MULTIPLE_SELECT,
        ColumnTypes.URL,
        ColumnTypes.DURATION,
        ColumnTypes.NUMBER,
        ColumnTypes.COLLABORATOR,
        ColumnTypes.EMAIL,
        ColumnTypes.RATE,
    ]

    def __init__(self, auto_rule, action_type, row):
        """
        auto_rule: instance of AutomationRule
        data: if auto_rule.PER_UPDATE, data is event data from redis
        row: {'col_1_name: ', value1, 'col_2_name': value2...}
        """
        super().__init__(auto_rule, action_type)
        self.row = row or {}
        self.row_data = {
            'row': {},
            'table_name': self.auto_rule.table_info['name']
        }
        self.init_updates()

    def format_time_by_offset(self, offset, format_length):
        cur_datetime = datetime.now()
        cur_datetime_offset = cur_datetime + timedelta(days=offset)
        if format_length == 2:
            return cur_datetime_offset.strftime("%Y-%m-%d %H:%M")
        if format_length == 1:
            return cur_datetime_offset.strftime("%Y-%m-%d")

    def init_updates(self):
        # filter columns in view and type of column is in VALID_COLUMN_TYPES
        filtered_updates = {}
        for col in self.auto_rule.table_info['columns']:
            if col.get('type') not in self.VALID_COLUMN_TYPES:
                continue
            col_name = col.get('name')
            col_type = col.get('type')
            col_key = col.get('key')
            if col_key in self.row.keys():
                if col_type == ColumnTypes.DATE:
                    time_format = col.get('data', {}).get('format', '')
                    format_length = len(time_format.split(" "))
                    try:
                        time_dict = self.row.get(col_key)
                        if not time_dict:
                            continue
                        set_type = time_dict.get('set_type')
                        if set_type == 'specific_value':
                            time_value = time_dict.get('value')
                            filtered_updates[col_name] = time_value
                        elif set_type == 'relative_date':
                            offset = time_dict.get('offset')
                            filtered_updates[col_name] = self.format_time_by_offset(int(offset), format_length)
                    except Exception as e:
                        logger.error(e)
                        filtered_updates[col_name] = self.row.get(col_key)
                else:
                    filtered_updates[col_name] = self.parse_column_value(col, self.row.get(col_key))
        self.row_data['row'] = filtered_updates

    def can_do_action(self):
        if not self.row_data.get('row'):
            return False

        return True

    def do_action(self):
        if not self.can_do_action():
            return
        table_name = self.auto_rule.table_info['name']
        try:
            row = self.auto_rule.dtable_server_api.append_row(table_name, self.row_data['row'])
        except Exception as e:
            logger.error('update dtable: %s, error: %s', self.auto_rule.dtable_uuid, e)
        self.auto_rule.set_done_actions()
        try:
            self.row_data['row']['_id'] = row['_id']
            self.send_selected_collaborator_notis(self.row_data['row'])
        except Exception as e:
            logger.exception('send selected notifications error: %s', e)

class NotifyAction(BaseAction):

    def __init__(self, auto_rule, action_type, data, msg, users, users_column_key):
        """
        auto_rule: instance of AutomationRule
        data: if auto_rule.PER_UPDATE, data is event data from redis
        msg: message set in action
        users: who will receive notification(s)
        """
        super().__init__(auto_rule, action_type, data)
        self.msg = msg or ''
        temp_users = []
        for user in users or []:
            if user and user not in self.auto_rule.related_users_dict:
                error_msg = 'rule: %s notify action has invalid user: %s' % (self.auto_rule.rule_id, user)
                raise RuleInvalidException(error_msg)
            if user:
                temp_users.append(user)
        self.users = temp_users
        self.users_column_key = users_column_key or ''

        self.column_blanks = []
        self.col_name_dict = {}

        self.init_notify(msg)

    def is_valid_username(self, user):
        if not user:
            return False

        return is_valid_email(user)

    def get_user_column_by_key(self):
        dtable_metadata = self.auto_rule.dtable_metadata
        table = None
        for t in dtable_metadata.get('tables', []):
            if t.get('_id') == self.auto_rule.table_id:
                table = t
                break

        if not table:
            return None

        for col in table.get('columns'):
            if col.get('key') == self.users_column_key:
                return col

        return None

    def init_notify(self, msg):
        blanks = set(re.findall(r'\{([^{]*?)\}', msg))
        self.col_name_dict = {col.get('name'): col for col in self.auto_rule.table_info['columns']}
        self.column_blanks = [blank for blank in blanks if blank in self.col_name_dict]

    def fill_msg_blanks(self, row):
        msg, column_blanks, col_name_dict = self.msg, self.column_blanks, self.col_name_dict
        db_session, dtable_metadata = self.auto_rule.db_session, self.auto_rule.dtable_metadata
        return fill_msg_blanks_with_converted_row(msg, column_blanks, col_name_dict, row, db_session)

    def fill_msg_blanks_with_sql(self, row):
        msg, column_blanks, col_name_dict = self.msg, self.column_blanks, self.col_name_dict
        db_session, dtable_metadata = self.auto_rule.db_session, self.auto_rule.dtable_metadata
        return fill_msg_blanks_with_sql_row(msg, column_blanks, col_name_dict, row, db_session)

    def per_update_notify(self):
        dtable_uuid, row, raw_row = self.auto_rule.dtable_uuid, self.data['converted_row'], self.data['row']
        table_id, view_id = self.auto_rule.table_id, self.auto_rule.view_id

        msg = self.msg
        if self.column_blanks:
            msg = self.fill_msg_blanks(row)

        detail = {
            'table_id': table_id,
            'view_id': view_id,
            'condition': self.auto_rule.trigger.get('condition'),
            'rule_id': self.auto_rule.rule_id,
            'rule_name': self.auto_rule.rule_name,
            'msg': msg,
            'row_id_list': [row['_id']],
        }

        user_msg_list = []
        users = self.users
        if self.users_column_key:
            user_column = self.get_user_column_by_key()
            if user_column:
                users_column_name = user_column.get('name')
                users_from_column = row.get(users_column_name, [])
                if not users_from_column:
                    users_from_column = []
                if not isinstance(users_from_column, list):
                    users_from_column = [users_from_column, ]
                users = list(set(self.users + [user for user in users_from_column if user in self.auto_rule.related_users_dict]))
            else:
                logger.warning('automation rule: %s notify action user column: %s invalid', self.auto_rule.rule_id, self.users_column_key)
        for user in users:
            if not self.is_valid_username(user):
                continue
            user_msg_list.append({
                'to_user': user,
                'msg_type': 'notification_rules',
                'detail': detail,
                })
        try:
            send_notification(dtable_uuid, user_msg_list, self.auto_rule.access_token)
        except Exception as e:
            logger.error('send users: %s notifications error: %s', e)

    def cron_notify(self):
        dtable_uuid = self.auto_rule.dtable_uuid
        table_id, view_id = self.auto_rule.table_id, self.auto_rule.view_id
        detail = {
            'table_id': table_id,
            'view_id': view_id,
            'condition': CONDITION_PERIODICALLY,
            'rule_id': self.auto_rule.rule_id,
            'rule_name': self.auto_rule.rule_name,
            'msg': self.msg,
            'row_id_list': []
        }
        user_msg_list = []
        for user in self.users:
            user_msg_list.append({
                'to_user': user,
                'msg_type': 'notification_rules',
                'detail': detail,
            })
        try:
            send_notification(dtable_uuid, user_msg_list, self.auto_rule.access_token)
        except Exception as e:
            logger.error('send users: %s notifications error: %s', e)

    def condition_cron_notify(self):
        table_id, view_id = self.auto_rule.table_id, self.auto_rule.view_id
        dtable_uuid = self.auto_rule.dtable_uuid

        rows_data = self.auto_rule.get_trigger_conditions_rows(self, warning_rows=NOTIFICATION_CONDITION_ROWS_LIMIT)[:NOTIFICATION_CONDITION_ROWS_LIMIT]
        col_key_dict = {col.get('key'): col for col in self.auto_rule.view_columns}

        user_msg_list = []
        for row in rows_data:
            converted_row = {col_key_dict.get(key).get('name') if col_key_dict.get(key) else key:
                             self.parse_column_value(col_key_dict.get(key), row.get(key)) if col_key_dict.get(key) else row.get(key)
                             for key in row}
            msg = self.msg
            if self.column_blanks:
                msg = self.fill_msg_blanks_with_sql(row)

            detail = {
                'table_id': table_id,
                'view_id': view_id,
                'condition': self.auto_rule.trigger.get('condition'),
                'rule_id': self.auto_rule.rule_id,
                'rule_name': self.auto_rule.rule_name,
                'msg': msg,
                'row_id_list': [converted_row['_id']],
            }

            users = self.users
            if self.users_column_key:
                user_column = self.get_user_column_by_key()
                if user_column:
                    users_column_name = user_column.get('name')
                    users_from_column = converted_row.get(users_column_name, [])
                    if not users_from_column:
                        users_from_column = []
                    if not isinstance(users_from_column, list):
                        users_from_column = [users_from_column, ]
                    users = list(set(self.users + users_from_column))
                else:
                    logger.warning('automation rule: %s notify action user column: %s invalid', self.auto_rule.rule_id, self.users_column_key)
            for user in users:
                if not self.is_valid_username(user):
                    continue
                user_msg_list.append({
                    'to_user': user,
                    'msg_type': 'notification_rules',
                    'detail': detail,
                    })
        try:
            send_notification(dtable_uuid, user_msg_list, self.auto_rule.access_token)
        except Exception as e:
            logger.error('send users: %s notifications error: %s', e)

    def do_action(self):
        if self.auto_rule.run_condition == PER_UPDATE:
            self.per_update_notify()
        elif self.auto_rule.run_condition in CRON_CONDITIONS:
            if self.auto_rule.trigger.get('condition') == CONDITION_PERIODICALLY_BY_CONDITION:
                self.condition_cron_notify()
            else:
                self.cron_notify()
        self.auto_rule.set_done_actions()

class AppNotifyAction(BaseAction):

    def __init__(self, auto_rule, action_type, data, msg, users, users_column_key, app_uuid):
        """
        auto_rule: instance of AutomationRule
        data: if auto_rule.PER_UPDATE, data is event data from redis
        msg: message set in action
        users: who will receive notification(s)
        """
        super().__init__(auto_rule, action_type, data)
        self.msg = msg or ''
        self.users = users
        self.users_column_key = users_column_key or ''
        self.app_uuid = app_uuid

        self.column_blanks = []
        self.col_name_dict = {}
        self.notice_api = UniversalAppAPI('notification-rule', app_uuid, DTABLE_WEB_SERVICE_URL)

        self.init_notify(msg)

    def is_valid_username(self, user):
        if not user:
            return False

        return is_valid_email(user)

    def get_user_column_by_key(self):
        dtable_metadata = self.auto_rule.dtable_metadata
        table = None
        for t in dtable_metadata.get('tables', []):
            if t.get('_id') == self.auto_rule.table_id:
                table = t
                break

        if not table:
            return None

        for col in table.get('columns'):
            if col.get('key') == self.users_column_key:
                return col

        return None

    def init_notify(self, msg):
        blanks = set(re.findall(r'\{([^{]*?)\}', msg))
        self.col_name_dict = {col.get('name'): col for col in self.auto_rule.table_info['columns']}
        self.column_blanks = [blank for blank in blanks if blank in self.col_name_dict]

    def fill_msg_blanks(self, row):
        msg, column_blanks, col_name_dict = self.msg, self.column_blanks, self.col_name_dict
        db_session, dtable_metadata = self.auto_rule.db_session, self.auto_rule.dtable_metadata
        return fill_msg_blanks_with_converted_row(msg, column_blanks, col_name_dict, row, db_session)

    def fill_msg_blanks_with_sql(self, row):
        msg, column_blanks, col_name_dict = self.msg, self.column_blanks, self.col_name_dict
        db_session, dtable_metadata = self.auto_rule.db_session, self.auto_rule.dtable_metadata
        return fill_msg_blanks_with_sql_row(msg, column_blanks, col_name_dict, row, db_session)

    def per_update_notify(self):
        dtable_uuid, row, raw_row = self.auto_rule.dtable_uuid, self.data['converted_row'], self.data['row']
        table_id, view_id = self.auto_rule.table_id, self.auto_rule.view_id

        msg = self.msg
        if self.column_blanks:
            msg = self.fill_msg_blanks(row)

        detail = {
            'table_id': table_id,
            'view_id': view_id,
            'condition': self.auto_rule.trigger.get('condition'),
            'rule_id': self.auto_rule.rule_id,
            'rule_name': self.auto_rule.rule_name,
            'msg': msg,
            'row_id_list': [row['_id']],
        }

        user_msg_list = []
        users = self.users
        if self.users_column_key:
            user_column = self.get_user_column_by_key()
            if user_column:
                users_column_name = user_column.get('name')
                users_from_column = row.get(users_column_name, [])
                if not users_from_column:
                    users_from_column = []
                if not isinstance(users_from_column, list):
                    users_from_column = [users_from_column, ]
                users = list(set(self.users + users_from_column))
            else:
                logger.warning('automation rule: %s notify action user column: %s invalid', self.auto_rule.rule_id, self.users_column_key)
        for user in users:
            if not self.is_valid_username(user):
                continue
            user_msg_list.append({
                'to_user': user,
                'msg_type': 'notification_rules',
                'detail': detail,
                })
        try:
            if user_msg_list:
                self.notice_api.batch_send_notification(user_msg_list)
        except Exception as e:
            logger.error('send users: %s notifications error: %s', e)

    def cron_notify(self):
        dtable_uuid = self.auto_rule.dtable_uuid
        table_id, view_id = self.auto_rule.table_id, self.auto_rule.view_id
        detail = {
            'table_id': table_id,
            'view_id': view_id,
            'condition': CONDITION_PERIODICALLY,
            'rule_id': self.auto_rule.rule_id,
            'rule_name': self.auto_rule.rule_name,
            'msg': self.msg,
            'row_id_list': []
        }
        user_msg_list = []
        for user in self.users:
            user_msg_list.append({
                'to_user': user,
                'msg_type': 'notification_rules',
                'detail': detail,
            })
        try:
            self.notice_api.batch_send_notification(user_msg_list)
        except Exception as e:
            logger.error('send users: %s notifications error: %s', e)

    def condition_cron_notify(self):
        table_id, view_id = self.auto_rule.table_id, self.auto_rule.view_id
        dtable_uuid = self.auto_rule.dtable_uuid

        rows_data = self.auto_rule.get_trigger_conditions_rows(self, warning_rows=NOTIFICATION_CONDITION_ROWS_LIMIT)[:NOTIFICATION_CONDITION_ROWS_LIMIT]
        col_key_dict = {col.get('key'): col for col in self.auto_rule.view_columns}

        user_msg_list = []
        for row in rows_data:
            converted_row = {col_key_dict.get(key).get('name') if col_key_dict.get(key) else key:
                             self.parse_column_value(col_key_dict.get(key), row.get(key)) if col_key_dict.get(key) else row.get(key)
                             for key in row}
            msg = self.msg
            if self.column_blanks:
                msg = self.fill_msg_blanks_with_sql(row)

            detail = {
                'table_id': table_id,
                'view_id': view_id,
                'condition': self.auto_rule.trigger.get('condition'),
                'rule_id': self.auto_rule.rule_id,
                'rule_name': self.auto_rule.rule_name,
                'msg': msg,
                'row_id_list': [converted_row['_id']],
            }

            users = self.users
            if self.users_column_key:
                user_column = self.get_user_column_by_key()
                if user_column:
                    users_column_name = user_column.get('name')
                    users_from_column = converted_row.get(users_column_name, [])
                    if not users_from_column:
                        users_from_column = []
                    if not isinstance(users_from_column, list):
                        users_from_column = [users_from_column, ]
                    users = list(set(self.users + users_from_column))
                else:
                    logger.warning('automation rule: %s notify action user column: %s invalid', self.auto_rule.rule_id, self.users_column_key)
            for user in users:
                if not self.is_valid_username(user):
                    continue
                user_msg_list.append({
                    'to_user': user,
                    'msg_type': 'notification_rules',
                    'detail': detail,
                    })
        try:
            self.notice_api.batch_send_notification(user_msg_list)
        except Exception as e:
            logger.error('send users: %s notifications error: %s', e)

    def do_action(self):
        if self.auto_rule.run_condition == PER_UPDATE:
            self.per_update_notify()
        elif self.auto_rule.run_condition in CRON_CONDITIONS:
            if self.auto_rule.trigger.get('condition') == CONDITION_PERIODICALLY_BY_CONDITION:
                self.condition_cron_notify()
            else:
                self.cron_notify()
        self.auto_rule.set_done_actions()


class SendWechatAction(BaseAction):

    def __init__(self, auto_rule, action_type, data, msg, account_id, msg_type):

        super().__init__(auto_rule, action_type, data)
        self.msg = msg or ''
        self.msg_type = msg_type or 'text'
        self.account_id = account_id or ''

        self.webhook_url = ''
        self.column_blanks = []
        self.col_name_dict = {}

        self.init_notify(msg)

    def init_notify(self, msg):
        account_dict = get_third_party_account(self.auto_rule.db_session, self.account_id)
        if not account_dict:
            raise RuleInvalidException('Send wechat no account')
        blanks = set(re.findall(r'\{([^{]*?)\}', msg))
        self.col_name_dict = {col.get('name'): col for col in self.auto_rule.table_info['columns']}
        self.column_blanks = [blank for blank in blanks if blank in self.col_name_dict]
        self.webhook_url = account_dict.get('detail', {}).get('webhook_url', '')

    def fill_msg_blanks(self, row):
        msg, column_blanks, col_name_dict = self.msg, self.column_blanks, self.col_name_dict
        db_session, dtable_metadata = self.auto_rule.db_session, self.auto_rule.dtable_metadata
        return fill_msg_blanks_with_converted_row(msg, column_blanks, col_name_dict, row, db_session)

    def fill_msg_blanks_with_sql(self, row):
        msg, column_blanks, col_name_dict = self.msg, self.column_blanks, self.col_name_dict
        db_session, dtable_metadata = self.auto_rule.db_session, self.auto_rule.dtable_metadata
        return fill_msg_blanks_with_sql_row(msg, column_blanks, col_name_dict, row, db_session)

    def per_update_notify(self):
        row = self.data['converted_row']
        msg = self.msg
        if self.column_blanks:
            msg = self.fill_msg_blanks(row)
        try:
            send_wechat_msg(self.webhook_url, msg, self.msg_type)
        except Exception as e:
            logger.error('send wechat error: %s', e)

    def cron_notify(self):
        try:
            send_wechat_msg(self.webhook_url, self.msg, self.msg_type)
        except Exception as e:
            logger.error('send wechat error: %s', e)

    def condition_cron_notify(self):
        rows_data = self.auto_rule.get_trigger_conditions_rows(self, warning_rows=WECHAT_CONDITION_ROWS_LIMIT)[:WECHAT_CONDITION_ROWS_LIMIT]
        for row in rows_data:
            msg = self.msg
            if self.column_blanks:
                msg = self.fill_msg_blanks_with_sql(row)
            try:
                send_wechat_msg(self.webhook_url, msg, self.msg_type)
                time.sleep(0.01)
            except Exception as e:
                logger.error('send wechat error: %s', e)

    def do_action(self):
        if not self.auto_rule.current_valid:
            return
        if self.auto_rule.run_condition == PER_UPDATE:
            self.per_update_notify()
        elif self.auto_rule.run_condition in CRON_CONDITIONS:
            if self.auto_rule.trigger.get('condition') == CONDITION_PERIODICALLY_BY_CONDITION:
                self.condition_cron_notify()
            else:
                self.cron_notify()
        self.auto_rule.set_done_actions()


class SendDingtalkAction(BaseAction):

    def __init__(self, auto_rule, action_type, data, msg, account_id, msg_type, msg_title):

        super().__init__(auto_rule, action_type, data)
        self.msg = msg or ''
        self.msg_type = msg_type or 'text'
        self.account_id = account_id or ''
        self.msg_title = msg_title or ''

        self.webhook_url = ''
        self.column_blanks = []
        self.col_name_dict = {}

        self.init_notify(msg)

    def init_notify(self, msg):
        account_dict = get_third_party_account(self.auto_rule.db_session, self.account_id)
        if not account_dict:
            raise RuleInvalidException('Send dingtalk no account')
        blanks = set(re.findall(r'\{([^{]*?)\}', msg))
        self.col_name_dict = {col.get('name'): col for col in self.auto_rule.table_info['columns']}
        self.column_blanks = [blank for blank in blanks if blank in self.col_name_dict]
        self.webhook_url = account_dict.get('detail', {}).get('webhook_url', '')

    def fill_msg_blanks(self, row):
        msg, column_blanks, col_name_dict = self.msg, self.column_blanks, self.col_name_dict
        db_session, dtable_metadata = self.auto_rule.db_session, self.auto_rule.dtable_metadata
        return fill_msg_blanks_with_converted_row(msg, column_blanks, col_name_dict, row, db_session)

    def fill_msg_blanks_with_sql(self, row):
        msg, column_blanks, col_name_dict = self.msg, self.column_blanks, self.col_name_dict
        db_session, dtable_metadata = self.auto_rule.db_session, self.auto_rule.dtable_metadata
        return fill_msg_blanks_with_sql_row(msg, column_blanks, col_name_dict, row, db_session)

    def per_update_notify(self):
        row = self.data['converted_row']
        msg = self.msg
        if self.column_blanks:
            msg = self.fill_msg_blanks(row)
        try:
            send_dingtalk_msg(self.webhook_url, msg, self.msg_type, self.msg_title)
        except Exception as e:
            logger.error('send dingtalk error: %s', e)

    def cron_notify(self):
        try:
            send_dingtalk_msg(self.webhook_url, self.msg, self.msg_type, self.msg_title)
        except Exception as e:
            logger.error('send dingtalk error: %s', e)

    def condition_cron_notify(self):
        rows_data = self.auto_rule.get_trigger_conditions_rows(self, warning_rows=DINGTALK_CONDITION_ROWS_LIMIT)[:DINGTALK_CONDITION_ROWS_LIMIT]
        for row in rows_data:
            msg = self.msg
            if self.column_blanks:
                msg = self.fill_msg_blanks_with_sql(row)
            try:
                send_dingtalk_msg(self.webhook_url, msg, self.msg_type, self.msg_title)
                time.sleep(0.01)
            except Exception as e:
                logger.error('send dingtalk error: %s', e)

    def do_action(self):
        if not self.auto_rule.current_valid:
            return
        if self.auto_rule.run_condition == PER_UPDATE:
            self.per_update_notify()
        elif self.auto_rule.run_condition in CRON_CONDITIONS:
            if self.auto_rule.trigger.get('condition') == CONDITION_PERIODICALLY_BY_CONDITION:
                self.condition_cron_notify()
            else:
                self.cron_notify()
        self.auto_rule.set_done_actions()


class SendEmailAction(BaseAction):

    def is_valid_email(self, email):
        """A heavy email format validation.
        """
        return is_valid_email(email)

    def __init__(self, auto_rule, action_type, data, send_info, account_id, repo_id):

        super().__init__(auto_rule, action_type, data)
        self.account_id = account_id

        # send info
        self.send_info = send_info

        # auth info
        self.auth_info = {}

        self.column_blanks = []
        self.column_blanks_send_to = []
        self.column_blanks_copy_to = []
        self.column_blanks_subject = []
        self.col_name_dict = {}
        self.repo_id = repo_id
        self.image_cid_url_map = {}

        self.init_notify()

    def init_notify_msg(self):
        if self.send_info.get('is_plain_text', True):
            msg = self.send_info.get('message')
            blanks = set(re.findall(r'\{([^{]*?)\}', msg))
        else:
            html_msg = self.send_info.get('html_message')
            blanks = set(re.findall(r'\{([^{]*?)\}', html_msg))
        self.column_blanks = [blank for blank in blanks if blank in self.col_name_dict]

    def init_notify_send_to(self):
        send_to_list = self.send_info.get('send_to')
        blanks = []
        for send_to in send_to_list:
            res = re.findall(r'\{([^{]*?)\}', send_to)
            if res:
                blanks.extend(res)
        self.column_blanks_send_to = [blank for blank in blanks if blank in self.col_name_dict]

    def init_notify_copy_to(self):
        copy_to_list = self.send_info.get('copy_to')
        blanks = []
        for copy_to in copy_to_list:
            res = re.findall(r'\{([^{]*?)\}', copy_to)
            if res:
                blanks.extend(res)
        self.column_blanks_copy_to = [blank for blank in blanks if blank in self.col_name_dict]

    def init_notify_subject(self):
        subject = self.send_info.get('subject')
        blanks = set(re.findall(r'\{([^{]*?)\}', subject))
        self.column_blanks_subject = [blank for blank in blanks if blank in self.col_name_dict]

    def handle_file_path(self, dtable_uuid, repo_id, file_path):
        asset_path = normalize_file_path(os.path.join('/asset', uuid_str_to_36_chars(dtable_uuid), file_path))
        asset_id = seafile_api.get_file_id_by_path(repo_id, asset_path)
        asset_name = os.path.basename(normalize_file_path(file_path))
        if not asset_id:
            return None, None

        token = seafile_api.get_fileserver_access_token(
            repo_id, asset_id, 'download', '', use_onetime=False
        )

        url = gen_file_get_url(token, asset_name)
        return  asset_name, url

    def init_notify_images(self):
        images_info = self.send_info.get('images_info', {})
        for cid, image_path in images_info.items():
            image_name, image_url = self.handle_file_path(self.auto_rule.dtable_uuid, self.repo_id, image_path)
            if not image_name or not image_url:
                continue
            self.image_cid_url_map[cid] = image_url

    def init_notify(self):
        account_dict = get_third_party_account(self.auto_rule.db_session, self.account_id)
        if not account_dict:
            raise RuleInvalidException('Send email no account')
        self.col_name_dict = {col.get('name'): col for col in self.auto_rule.table_info['columns']}
        self.init_notify_msg()
        self.init_notify_send_to()
        self.init_notify_copy_to()
        self.init_notify_subject()
        self.init_notify_images()

        account_detail = account_dict.get('detail', {})

        self.auth_info = account_detail

    def fill_msg_blanks(self, row, text, blanks):
        col_name_dict = self.col_name_dict
        db_session, dtable_metadata = self.auto_rule.db_session, self.auto_rule.dtable_metadata
        return fill_msg_blanks_with_converted_row(text, blanks, col_name_dict, row, db_session)


    def fill_msg_blanks_with_sql(self, row, text, blanks):
        col_name_dict = self.col_name_dict
        db_session, dtable_metadata = self.auto_rule.db_session, self.auto_rule.dtable_metadata
        return fill_msg_blanks_with_sql_row(text, blanks, col_name_dict, row, db_session)

    def get_file_down_url(self, file_url):
        file_path = unquote('/'.join(file_url.split('/')[7:]).strip())

        asset_path = normalize_file_path(os.path.join('/asset', uuid_str_to_36_chars(self.auto_rule.dtable_uuid), file_path))
        asset_id = seafile_api.get_file_id_by_path(self.repo_id, asset_path)
        asset_name = os.path.basename(normalize_file_path(file_path))
        if not asset_id:
            logger.warning('automation rule: %s, send email asset file %s does not exist.', asset_name)
            return None

        token = seafile_api.get_fileserver_access_token(
            self.repo_id, asset_id, 'download', '', use_onetime=False
        )

        url = gen_file_get_url(token, asset_name)
        return url

    def get_file_download_urls(self, attachment_list, row):
        file_download_urls_dict = {}
        if not self.repo_id:
            logger.warning('automation rule: %s, send email repo_id invalid', self.auto_rule.rule_id)
            return None

        for file_column_id in attachment_list:
            files = row.get(file_column_id)
            if not files:
                continue
            for file in files:
                file_url = self.get_file_down_url(file.get('url', ''))
                if not file_url:
                    continue
                file_download_urls_dict[file.get('name')] = file_url
        return file_download_urls_dict

    def per_update_notify(self):
        row = self.data['converted_row']
        msg = self.send_info.get('message', '')
        is_plain_text = self.send_info.get('is_plain_text', True)
        html_msg = self.send_info.get('html_message', '')
        subject = self.send_info.get('subject', '')
        send_to_list = self.send_info.get('send_to', [])
        copy_to_list = self.send_info.get('copy_to', [])
        attachment_list = self.send_info.get('attachment_list', [])

        if self.column_blanks:
            if is_plain_text and msg:
                msg = self.fill_msg_blanks(row, msg, self.column_blanks)
            if not is_plain_text and html_msg:
                html_msg = self.fill_msg_blanks(row, html_msg, self.column_blanks)
        if self.column_blanks_send_to:
            send_to_list = [self.fill_msg_blanks(row, send_to, self.column_blanks_send_to) for send_to in send_to_list]
        if self.column_blanks_copy_to:
            copy_to_list = [self.fill_msg_blanks(row, copy_to, self.column_blanks_copy_to) for copy_to in copy_to_list]

        file_download_urls = self.get_file_download_urls(attachment_list, self.data['row'])

        if self.column_blanks_subject:
            subject = self.fill_msg_blanks(row, subject, self.column_blanks_subject)

        send_info = deepcopy(self.send_info)
        if is_plain_text:
            send_info['message'] = msg
            send_info.pop('html_message', None)
        else:
            send_info['html_message'] = html_msg
            send_info['image_cid_url_map'] = self.image_cid_url_map
            send_info.pop('message', None)
        send_info.update({
            'subject': subject,
            'send_to': [send_to for send_to in send_to_list if self.is_valid_email(send_to)],
            'copy_to': [copy_to for copy_to in copy_to_list if self.is_valid_email(copy_to)],
            'file_download_urls': file_download_urls,
        })
        try:
            send_email_msg(
                auth_info=self.auth_info,
                send_info=send_info,
                username='automation-rules',  # username send by automation rules,
                db_session=self.auto_rule.db_session
            )
        except Exception as e:
            logger.error('send email error: %s', e)

    def cron_notify(self):
        send_info = deepcopy(self.send_info)
        if send_info.get('is_plain_text', True):
            send_info.pop('html_message', None)
        else:
            send_info['image_cid_url_map'] = self.image_cid_url_map
            send_info.pop('message', None)
        try:
            send_email_msg(
                auth_info=self.auth_info,
                send_info=send_info,
                username='automation-rules',  # username send by automation rules,
                db_session=self.auto_rule.db_session
            )
        except Exception as e:
            logger.error('send email error: %s', e)

    def condition_cron_notify(self):
        rows_data = self.auto_rule.get_trigger_conditions_rows(self, warning_rows=EMAIL_CONDITION_ROWS_LIMIT)[:EMAIL_CONDITION_ROWS_LIMIT]
        col_key_dict = {col.get('key'): col for col in self.auto_rule.view_columns}
        send_info_list = []
        for row in rows_data:
            converted_row = {col_key_dict.get(key).get('name') if col_key_dict.get(key) else key:
                             self.parse_column_value(col_key_dict.get(key), row.get(key)) if col_key_dict.get(key) else row.get(key)
                             for key in row}
            send_info = deepcopy(self.send_info)
            msg = send_info.get('message', '')
            is_plain_text = send_info.get('is_plain_text', True)
            html_msg = send_info.get('html_message', '')
            subject = send_info.get('subject', '')
            send_to_list = send_info.get('send_to', [])
            copy_to_list = send_info.get('copy_to', [])
            attachment_list = send_info.get('attachment_list', [])
            if self.column_blanks:
                if is_plain_text and msg:
                    msg = self.fill_msg_blanks_with_sql(row, msg, self.column_blanks)
                if not is_plain_text and html_msg:
                    html_msg = self.fill_msg_blanks_with_sql(row, html_msg, self.column_blanks)
            if self.column_blanks_send_to:
                send_to_list = [self.fill_msg_blanks(converted_row, send_to, self.column_blanks_send_to) for send_to in send_to_list]
            if self.column_blanks_copy_to:
                copy_to_list = [self.fill_msg_blanks(converted_row, copy_to, self.column_blanks_copy_to) for copy_to in copy_to_list]

            file_download_urls = self.get_file_download_urls(attachment_list, row)

            if self.column_blanks_subject:
                subject = self.fill_msg_blanks(converted_row, subject, self.column_blanks_subject)

            if is_plain_text:
                send_info['message'] = msg
                send_info.pop('html_message', None)
            else:
                send_info['html_message'] = html_msg
                send_info['image_cid_url_map'] = self.image_cid_url_map
                send_info.pop('message', None)

            send_info.update({
                'subject': subject,
                'send_to': [send_to for send_to in send_to_list if self.is_valid_email(send_to)],
                'copy_to': [copy_to for copy_to in copy_to_list if self.is_valid_email(copy_to)],
                'file_download_urls': file_download_urls,
            })

            send_info_list.append(send_info)

        step = 10
        for i in range(0, len(send_info_list), step):
            try:
                batch_send_email_msg(
                    auth_info=self.auth_info,
                    send_info_list=send_info_list[i: i+step],
                    username='automation-rules',  # username send by automation rules,
                    db_session=self.auto_rule.db_session
                )
            except Exception as e:
                logger.error('batch send email error: %s', e)

    def do_action(self):
        if not self.auto_rule.current_valid:
            return
        if self.auto_rule.run_condition == PER_UPDATE:
            self.per_update_notify()
        elif self.auto_rule.run_condition in CRON_CONDITIONS:
            if self.auto_rule.trigger.get('condition') == CONDITION_PERIODICALLY_BY_CONDITION:
                self.condition_cron_notify()
            else:
                self.cron_notify()
        self.auto_rule.set_done_actions()


class RunPythonScriptAction(BaseAction):

    def __init__(self, auto_rule, action_type, data, script_name, workspace_id, owner, org_id, repo_id):
        super().__init__(auto_rule, action_type, data=data)
        self.script_name = script_name
        self.workspace_id = workspace_id
        self.owner = owner
        self.org_id = org_id
        self.repo_id = repo_id

    def can_do_action(self):
        if not SEATABLE_FAAS_URL:
            return False

        script_file_path = os.path.join('/asset', uuid_str_to_36_chars(self.auto_rule.dtable_uuid), 'scripts', self.script_name)
        try:
            repo = seafile_api.get_repo(self.repo_id)
            if not repo:
                logger.warning('rule: %s script: %s repo: %s not found', self.auto_rule.rule_id, self.script_name, self.repo_id)
                raise RuleInvalidException('rule: %s script: %s repo: %s not found' % (self.auto_rule.rule_id, self.script_name, self.repo_id))
            script_file_id = seafile_api.get_file_id_by_path(self.repo_id, script_file_path)
            if not script_file_id:
                logger.warning('rule: %s script: %s repo: %s file: %s not found', self.auto_rule.rule_id, self.script_name, self.repo_id, script_file_path)
                raise RuleInvalidException('rule: %s script: %s repo: %s file: %s not found' % (self.auto_rule.rule_id, self.script_name, self.repo_id, script_file_path))
        except RuleInvalidException as e:
            raise e
        except Exception as e:
            logger.exception('access repo: %s path: %s error: %s', self.repo_id, script_file_path, e)
            return False

        if self.auto_rule.can_run_python is not None:
            return self.auto_rule.can_run_python

        permission_url = DTABLE_WEB_SERVICE_URL.strip('/') + '/api/v2.1/script-permissions/'
        headers = {'Authorization': 'Token ' + SEATABLE_FAAS_AUTH_TOKEN}
        if self.org_id != -1:
            json_data = {'org_ids': [self.org_id]}
        elif self.org_id == -1 and '@seafile_group' not in self.owner:
            json_data = {'users': [self.owner]}
        else:
            return True
        try:
            resp = requests.get(permission_url, headers=headers, json=json_data)
            if resp.status_code != 200:
                logger.error('check run script permission error response: %s', resp.status_code)
                return False
            permission_dict = resp.json()
        except Exception as e:
            logger.error('check run script permission error: %s', e)
            return False

        # response dict like
        # {
        #   'user_script_permissions': {username1: {'can_run_python_script': True/False}}
        #   'can_schedule_run_script': {org1: {'can_run_python_script': True/False}}
        # }
        if self.org_id != -1:
            can_run_python = permission_dict['org_script_permissions'][str(self.org_id)]['can_run_python_script']
        else:
            can_run_python = permission_dict['user_script_permissions'][self.owner]['can_run_python_script']

        self.auto_rule.can_run_python = can_run_python
        return can_run_python

    def get_scripts_running_limit(self):
        if self.auto_rule.scripts_running_limit is not None:
            return self.auto_rule.scripts_running_limit
        if self.org_id != -1:
            params = {'org_id': self.org_id}
        elif self.org_id == -1 and '@seafile_group' not in self.owner:
            params = {'username': self.owner}
        else:
            return -1
        url = DTABLE_WEB_SERVICE_URL.strip('/') + '/api/v2.1/scripts-running-limit/'
        headers = {'Authorization': 'Token ' + SEATABLE_FAAS_AUTH_TOKEN}
        try:
            resp = requests.get(url, headers=headers, params=params)
            if resp.status_code != 200:
                logger.error('get scripts running limit error response: %s', resp.status_code)
                return 0
            scripts_running_limit = resp.json()['scripts_running_limit']
        except Exception as e:
            logger.error('get script running limit error: %s', e)
            return 0
        self.auto_rule.scripts_running_limit = scripts_running_limit
        return scripts_running_limit

    def do_action(self):
        if not self.can_do_action():
            return

        context_data = {'table': self.auto_rule.table_info['name']}
        if self.auto_rule.run_condition == PER_UPDATE:
            context_data['row'] = self.data['converted_row']
        scripts_running_limit = self.get_scripts_running_limit()

        # request faas url
        headers = {'Authorization': 'Token ' + SEATABLE_FAAS_AUTH_TOKEN}
        url = SEATABLE_FAAS_URL.strip('/') + '/run-script/'
        try:
            response = requests.post(url, json={
                'dtable_uuid': str(UUID(self.auto_rule.dtable_uuid)),
                'script_name': self.script_name,
                'context_data': context_data,
                'owner': self.owner,
                'org_id': self.org_id,
                'temp_api_token': self.auto_rule.get_temp_api_token(app_name=self.script_name),
                'scripts_running_limit': scripts_running_limit,
                'operate_from': 'automation-rule',
                'operator': self.auto_rule.rule_id
            }, headers=headers, timeout=10)
        except Exception as e:
            logger.exception(e)
            logger.error(e)
            return

        if response.status_code != 200:
            logger.warning('run script error status code: %s', response.status_code)
        else:
            self.auto_rule.set_done_actions()


class LinkRecordsAction(BaseAction):

    COLUMN_FILTER_PREDICATE_MAPPING = {
        ColumnTypes.TEXT: "is",
        ColumnTypes.DATE: "is",
        ColumnTypes.LONG_TEXT: "is",
        ColumnTypes.CHECKBOX: "is",
        ColumnTypes.SINGLE_SELECT: "is",
        ColumnTypes.MULTIPLE_SELECT: "is_exactly",
        ColumnTypes.URL: "is",
        ColumnTypes.DURATION: "equal",
        ColumnTypes.NUMBER: "equal",
        ColumnTypes.COLLABORATOR: "is_exactly",
        ColumnTypes.EMAIL: "is",
        ColumnTypes.RATE: "equal",
    }

    VALID_COLUMN_TYPES = [
        ColumnTypes.TEXT,
        ColumnTypes.NUMBER,
        ColumnTypes.CHECKBOX,
        ColumnTypes.DATE,
        ColumnTypes.COLLABORATOR,
        ColumnTypes.URL,
        ColumnTypes.DURATION,
        ColumnTypes.EMAIL,
        ColumnTypes.RATE,
        ColumnTypes.FORMULA,
        ColumnTypes.AUTO_NUMBER,
        ColumnTypes.SINGLE_SELECT,
        ColumnTypes.MULTIPLE_SELECT,
        ColumnTypes.DEPARTMENT_SINGLE_SELECT
    ]

    def __init__(self, auto_rule, action_type, data, linked_table_id, link_id, match_conditions):
        super().__init__(auto_rule, action_type, data=data)
        self.linked_table_id = linked_table_id
        self.link_id = link_id
        self.match_conditions = match_conditions or []
        self.linked_row_ids = []

    def parse_column_value(self, column, value):
        if column.get('type') == ColumnTypes.SINGLE_SELECT:
            select_options = column.get('data', {}).get('options', [])
            for option in select_options:
                if value == option.get('name'):
                    return option.get('id')

        elif column.get('type') == ColumnTypes.MULTIPLE_SELECT:
            m_select_options = column.get('data', {}).get('options', [])
            if isinstance(value, list):
                parse_value_list = []
                for option in m_select_options:
                    if option.get('name') in value:
                        option_id = option.get('id')
                        parse_value_list.append(option_id)
                return parse_value_list
        elif column.get('type') in [ColumnTypes.CREATOR, ColumnTypes.LAST_MODIFIER]:
            return [value]
        else:
            return value

    def format_filter_groups(self):
        filters = []
        column_names = []
        for match_condition in self.match_conditions:
            column_key = match_condition.get("column_key")
            column = self.get_column(self.auto_rule.table_id, column_key)
            if not column:
                raise RuleInvalidException('match column not found')
            row_value = self.data['converted_row'].get(column.get('name'))
            if not row_value:
                return [], []
            other_column_key = match_condition.get("other_column_key")
            other_column = self.get_column(self.linked_table_id, other_column_key)
            if not other_column:
                raise RuleInvalidException('match other column not found')
            column_names.append(other_column['name'])
            parsed_row_value = self.parse_column_value(other_column, row_value)
            if not parsed_row_value and other_column['type'] in [ColumnTypes.SINGLE_SELECT, ColumnTypes.MULTIPLE_SELECT]:
                raise RuleInvalidException('match other single/multi-select column options: %s not found' % row_value)
            filter_item = {
                "column_key": other_column_key,
                "filter_predicate": self.COLUMN_FILTER_PREDICATE_MAPPING.get(other_column.get('type', ''), 'is'),
                "filter_term": parsed_row_value,
                "filter_term_modifier":"exact_date"
            }

            filters.append(filter_item)
        if filters:
            return [{"filters": filters, "filter_conjunction": "And"}], column_names
        return [], column_names


    def get_table_name(self, table_id):
        dtable_metadata = self.auto_rule.dtable_metadata
        tables = dtable_metadata.get('tables', [])
        for table in tables:
            if table.get('_id') == table_id:
                return table.get('name')

    def get_table_by_name(self, table_name):
        dtable_metadata = self.auto_rule.dtable_metadata
        tables = dtable_metadata.get('tables', [])
        for table in tables:
            if table.get('name') == table_name:
                return table

    def get_column(self, table_id, column_key):
        for col in self.get_columns(table_id):
            if col.get('key') == column_key:
                return col
        return None

    def get_columns(self, table_id):
        dtable_metadata = self.auto_rule.dtable_metadata
        for table in dtable_metadata.get('tables', []):
            if table.get('_id') == table_id:
                return table.get('columns', [])
        return []

    def get_linked_table_rows(self):
        """return linked table rows ids

        return a list, meaning a list of rows ids
        return None, no need to update
        """
        filter_groups, column_names = self.format_filter_groups()
        if not filter_groups:
            return []

        filter_conditions = {
            'filter_groups': filter_groups,
            'group_conjunction': 'And',
            'start': 0,
            'limit': 500,
        }
        table_name = self.get_table_name(self.linked_table_id)
        columns = self.get_columns(self.linked_table_id)

        try:
            sql = filter2sql(table_name, columns, filter_conditions, by_group=True)
        except (ValueError, ColumnFilterInvalidError) as e:
            logger.warning('wrong filter in rule: %s linked-table filter_conditions: %s error: %s', self.auto_rule.rule_id, filter_conditions, e)
            raise RuleInvalidException('wrong filter in rule: %s linked-table error: %s' % (self.auto_rule.rule_id, e))
        query_clause = "*"
        if column_names:
            if "_id" not in column_names:
                column_names.append("_id")
            query_clause = ",".join(["`%s`" % n for n in column_names])
        try:
            sql = sql.replace("*", query_clause, 1)
            rows_data, _ = self.auto_rule.dtable_db_api.query(sql, convert=False)
        except RowsQueryError:
            raise RuleInvalidException('wrong filter in filters in link-records')
        except Request429Error:
            logger.warning('rule: %s query row too many', self.auto_rule.rule_id)
            return None
        except Exception as e:
            logger.exception(e)
            logger.error('rule: %s request filter rows error: %s', self.auto_rule.rule_id, e)
            return None

        logger.debug('Number of linking dtable rows by auto-rule %s is: %s, dtable_uuid: %s, details: %s' % (
            self.auto_rule.rule_id,
            rows_data and len(rows_data) or 0,
            self.auto_rule.dtable_uuid,
            json.dumps(filter_conditions)
        ))

        return rows_data or []

    def init_linked_row_ids(self):
        linked_rows_data = self.get_linked_table_rows()
        if linked_rows_data is None:
            self.linked_row_ids = None
            return
        self.linked_row_ids = linked_rows_data and [row.get('_id') for row in linked_rows_data] or []

    def per_update_can_do_action(self):
        linked_table_name = self.get_table_name(self.linked_table_id)
        if not linked_table_name:
            raise RuleInvalidException('link-records link_table_id table not found')

        self.init_linked_row_ids()

        if self.linked_row_ids is None:  # means query failed, dont do anything
            return

        table_columns = self.get_columns(self.auto_rule.table_id)
        link_col_name = ''
        for col in table_columns:
            if col.get('type') == 'link' and col.get('data', {}).get('link_id') == self.link_id:
                link_col_name = col.get('name')
        if link_col_name:
            linked_rows = self.data.get('converted_row', {}).get(link_col_name, {})
            table_linked_rows = {row.get('row_id'): True for row in linked_rows}
            if len(self.linked_row_ids) == len(table_linked_rows):
                for row_id in self.linked_row_ids:
                    if not table_linked_rows.get(row_id):
                        return True
                return False
        return True

    def per_update_link_records(self):
        if not self.per_update_can_do_action():
            return

        try:
            self.auto_rule.dtable_server_api.update_link(self.link_id, self.auto_rule.table_id, self.linked_table_id, self.data['row']['_id'], self.linked_row_ids)
        except Exception as e:
            logger.error('link dtable: %s, error: %s', self.auto_rule.dtable_uuid, e)
            return

    def get_columns_dict(self, table_id):
        dtable_metadata = self.auto_rule.dtable_metadata
        column_dict = {}
        for table in dtable_metadata.get('tables', []):
            if table.get('_id') == table_id:
                for col in table.get('columns'):
                    column_dict[col.get('key')] = col
        return column_dict

    def query_table_rows(self, table_name, filter_conditions=None, query_columns=None):
        start = 0
        step = 10000
        result_rows = []
        filter_clause = ''
        query_clause = "*"
        if query_columns:
            query_clause = ",".join(["`%s`" % cn for cn in query_columns])
        if filter_conditions:
            table = self.get_table_by_name(table_name)
            filter_clause = BaseSQLGenerator(table_name, table['columns'], filter_conditions=filter_conditions)._filter2sql()
        while True:
            sql = f"select {query_clause} from `{table_name}` {filter_clause} limit {start}, {step}"
            try:
                results, _ = self.auto_rule.dtable_db_api.query(sql)
            except Exception as e:
                logger.exception(e)
                logger.error('query dtable: %s, sql: %s, filters: %s, error: %s', self.auto_rule.dtable_uuid, sql, filter_conditions, e)
                return result_rows
            result_rows += results
            start += step
            if len(results) < step:
                break
        return result_rows

    def cron_link_records(self):
        table_id = self.auto_rule.table_id
        other_table_id = self.linked_table_id

        table_name = self.get_table_name(table_id)
        other_table_name = self.get_table_name(other_table_id)

        if not table_name or not other_table_name:
            raise RuleInvalidException('table_name or other_table_name not found')

        column_dict = self.get_columns_dict(table_id)
        other_column_dict = self.get_columns_dict(other_table_id)

        link_column = None
        for col in column_dict.values():
            if col['type'] != 'link':
                continue
            if col.get('data', {}).get('link_id') != self.link_id:
                continue
            link_column = col
            break
        if not link_column:
            raise RuleInvalidException('link column not found')

        equal_columns = []
        equal_other_columns = []
        filter_columns = []
        # check column valid
        for condition in self.match_conditions:
            if not condition.get('column_key') or not condition.get('other_column_key'):
                raise RuleInvalidException('column or other_column invalid')
            column = column_dict.get(condition['column_key'])
            other_column = other_column_dict.get(condition['other_column_key'])
            if not column or not other_column:
                raise RuleInvalidException('column or other_column not found')
            if column.get('type') not in self.VALID_COLUMN_TYPES or other_column.get('type') not in self.VALID_COLUMN_TYPES:
                raise RuleInvalidException('column or other_column type invalid')
            equal_columns.append(column.get('name'))
            equal_other_columns.append(other_column.get('name'))

        view_filters = self.auto_rule.view_info.get('filters', [])
        for f in view_filters:
            column_key = f.get('column_key')
            column = column_dict.get(column_key)
            if not column:
                raise RuleInvalidException('column not found')
            filter_columns.append(column.get('name'))


        view_filter_conditions = {
            'filters': view_filters,
            'filter_conjunction': self.auto_rule.view_info.get('filter_conjunction', 'And')
        }

        if "_id" not in equal_columns:
            equal_columns.append("_id")

        if "_id" not in equal_other_columns:
            equal_other_columns.append("_id")

        table_rows = self.query_table_rows(table_name, filter_conditions=view_filter_conditions, query_columns=equal_columns)
        other_table_rows = self.query_table_rows(other_table_name, query_columns=equal_other_columns)

        table_rows_dict = {}
        row_id_list, other_rows_ids_map = [], {}
        for row in table_rows:
            key = '-'
            for equal_condition in self.match_conditions:
                column_key = equal_condition['column_key']
                column = column_dict[column_key]
                column_name = column.get('name')
                value = row.get(column_name)
                value = cell_data2str(value)
                key += value + column_key + '-'
            key = str(hash(key))
            if key in table_rows_dict:
                table_rows_dict[key].append(row['_id'])
            else:
                table_rows_dict[key] = [row['_id']]

        for other_row in other_table_rows:
            other_key = '-'
            is_valid = False
            for equal_condition in self.match_conditions:
                column_key = equal_condition['column_key']
                other_column_key = equal_condition['other_column_key']
                other_column = other_column_dict[other_column_key]
                other_column_name = other_column['name']
                other_value = other_row.get(other_column_name)
                other_value = cell_data2str(other_value)
                if other_value:
                    is_valid = True
                other_key += other_value + column_key + '-'
            if not is_valid:
                continue
            other_key = str(hash(other_key))
            row_ids = table_rows_dict.get(other_key)
            if not row_ids:
                continue
            # add link rows
            for row_id in row_ids:
                if row_id in other_rows_ids_map:
                    other_rows_ids_map[row_id].append(other_row['_id'])
                else:
                    row_id_list.append(row_id)
                    other_rows_ids_map[row_id] = [other_row['_id']]
        # update links
        step = 1000
        for i in range(0, len(row_id_list), step):
            try:
                self.auto_rule.dtable_server_api.batch_update_links(self.link_id, table_id, other_table_id, row_id_list[i: i+step], {key: value for key, value in other_rows_ids_map.items() if key in row_id_list[i: i+step]})
            except Exception as e:
                logger.error('batch update links: %s, error: %s', self.auto_rule.dtable_uuid, e)
                return

    def do_action(self):
        if self.auto_rule.run_condition == PER_UPDATE:
            self.per_update_link_records()
        elif self.auto_rule.run_condition in CRON_CONDITIONS:
            if self.auto_rule.trigger['condition'] == CONDITION_PERIODICALLY:
                self.cron_link_records()

        self.auto_rule.set_done_actions()


class AddRecordToOtherTableAction(BaseAction):

    VALID_COLUMN_TYPES = [
        ColumnTypes.TEXT,
        ColumnTypes.DATE,
        ColumnTypes.LONG_TEXT,
        ColumnTypes.CHECKBOX,
        ColumnTypes.SINGLE_SELECT,
        ColumnTypes.MULTIPLE_SELECT,
        ColumnTypes.URL,
        ColumnTypes.DURATION,
        ColumnTypes.NUMBER,
        ColumnTypes.COLLABORATOR,
        ColumnTypes.EMAIL,
        ColumnTypes.RATE,
    ]

    def __init__(self, auto_rule, action_type, data, row, dst_table_id):
        """
        auto_rule: instance of AutomationRule
        data: data is event data from redis
        row: {'col_1_name: ', value1, 'col_2_name': value2...}
        dst_table_id: id of table that record to be added
        """
        super().__init__(auto_rule, action_type, data)
        self.row = row or {}
        self.col_name_dict = {}
        self.dst_table_id = dst_table_id
        self.row_data = {
            'row': {},
            'table_name': self.get_table_name(dst_table_id)
        }

    def get_table_name(self, table_id):
        dtable_metadata = self.auto_rule.dtable_metadata
        tables = dtable_metadata.get('tables', [])
        for table in tables:
            if table.get('_id') == table_id:
                return table.get('name')

    def get_columns(self, table_id):
        dtable_metadata = self.auto_rule.dtable_metadata
        for table in dtable_metadata.get('tables', []):
            if table.get('_id') == table_id:
                return table.get('columns', [])
        return []

    def fill_msg_blanks(self, row, text, blanks):
        col_name_dict = self.col_name_dict
        db_session, dtable_metadata = self.auto_rule.db_session, self.auto_rule.dtable_metadata
        return fill_msg_blanks_with_converted_row(text, blanks, col_name_dict, row, db_session)

    def format_time_by_offset(self, offset, format_length):
        cur_datetime = datetime.now()
        cur_datetime_offset = cur_datetime + timedelta(days=offset)
        if format_length == 2:
            return cur_datetime_offset.strftime("%Y-%m-%d %H:%M")
        if format_length == 1:
            return cur_datetime_offset.strftime("%Y-%m-%d")

    def add_or_create_options(self, column, value):
        table_name = self.row_data['table_name']
        select_options = column.get('data', {}).get('options', [])
        for option in select_options:
            if value == option.get('name'):
                return value
        self.auto_rule.dtable_server_api.add_column_options(
            table_name,
            column['name'],
            options = [gen_random_option(value)]
        )
        return value

    def init_append_rows(self):
        src_row = self.data['converted_row']
        src_columns = self.auto_rule.table_info['columns']
        self.col_name_dict = {col.get('name'): col for col in src_columns}
        self.col_key_dict = {col.get('key'): col for col in src_columns}

        for row_id in self.row:
            cell_value = self.row.get(row_id)
            # cell_value may be dict if the column type is date
            if not isinstance(cell_value, str):
                continue
            blanks = set(re.findall(r'\{([^{]*?)\}', cell_value))
            self.column_blanks = [blank for blank in blanks if blank in self.col_name_dict]
            self.row[row_id] = self.fill_msg_blanks(src_row, cell_value, self.column_blanks)

        dst_columns = self.get_columns(self.dst_table_id)

        filtered_updates = {}
        for col in dst_columns:
            if col.get('type') not in self.VALID_COLUMN_TYPES:
                continue
            col_name = col.get('name')
            col_type = col.get('type')
            col_key = col.get('key')
            if col_key in self.row.keys():
                if col_type == ColumnTypes.DATE:
                    time_format = col.get('data', {}).get('format', '')
                    format_length = len(time_format.split(" "))
                    try:
                        time_dict = self.row.get(col_key)
                        if not time_dict:
                            continue
                        set_type = time_dict.get('set_type')
                        if set_type == 'specific_value':
                            time_value = time_dict.get('value')
                            filtered_updates[col_name] = time_value
                        elif set_type == 'relative_date':
                            offset = time_dict.get('offset')
                            filtered_updates[col_name] = self.format_time_by_offset(int(offset), format_length)
                        elif set_type == 'date_column':
                            date_column_key = time_dict.get('date_column_key')
                            src_col = self.col_key_dict.get(date_column_key)
                            filtered_updates[col_name] = src_row.get(src_col['name'])
                    except Exception as e:
                        logger.error(e)
                        filtered_updates[col_name] = self.row.get(col_key)

                elif col_type == ColumnTypes.SINGLE_SELECT:
                    try:
                        data_dict = self.row.get(col_key)
                        if not data_dict:
                            continue
                        if isinstance(data_dict, dict):
                            set_type = data_dict.get('set_type')
                            if set_type == 'default':
                                value = data_dict.get('value')
                                filtered_updates[col_name] = self.parse_column_value(col, value)
                            elif set_type == 'column':
                                src_col_key = data_dict.get('value')
                                src_col = self.col_key_dict.get(src_col_key)
                                value = src_row.get(src_col['name'])
                                if value:
                                    filtered_updates[col_name] = self.add_or_create_options(col, value)
                        else:
                            value = data_dict # compatible with the old data strcture
                            filtered_updates[col_name] = self.parse_column_value(col, value)
                
                    except Exception as e:
                        logger.error(e)
                        filtered_updates[col_name] = self.row.get(col_key)

                elif col_type == ColumnTypes.COLLABORATOR:
                    try:
                        data_dict = self.row.get(col_key)
                        if not data_dict:
                            continue
                        if isinstance(data_dict, dict):
                            set_type = data_dict.get('set_type')
                            if set_type == 'default':
                                value = data_dict.get('value')
                                filtered_updates[col_name] = self.parse_column_value(col, value)
                            elif set_type == 'column':
                                src_col_key = data_dict.get('value')
                                src_col = self.col_key_dict.get(src_col_key)
                                value = src_row.get(src_col['name'])
                                if not isinstance(value, list):
                                    value = [value, ]
                                filtered_updates[col_name] = value
                        else:
                            value = data_dict # compatible with the old data strcture
                            filtered_updates[col_name] = self.parse_column_value(col, value)
                
                    except Exception as e:
                        logger.error(e)
                        filtered_updates[col_name] = self.row.get(col_key)

                elif col_type in [
                        ColumnTypes.NUMBER, 
                    ]:
                    try:
                        data_dict = self.row.get(col_key)
                        if not data_dict:
                            continue
                        if isinstance(data_dict, dict):
                            set_type = data_dict.get('set_type')
                            if set_type == 'default':
                                value = data_dict.get('value')
                                filtered_updates[col_name] = self.parse_column_value(col, value)
                            elif set_type == 'column':
                                src_col_key = data_dict.get('value')
                                src_col = self.col_key_dict.get(src_col_key)
                                value = src_row.get(src_col['name'])
                                filtered_updates[col_name] = value
                        else:
                            value = data_dict # compatible with the old data strcture
                            filtered_updates[col_name] = self.parse_column_value(col, value)
                
                    except Exception as e:
                        logger.error(e)
                        filtered_updates[col_name] = self.row.get(col_key)
                else:
                    filtered_updates[col_name] = self.parse_column_value(col, self.row.get(col_key))

        self.row_data['row'] = filtered_updates

    def do_action(self):

        table_name = self.get_table_name(self.dst_table_id)
        if not table_name:
            raise RuleInvalidException('add-record dst_table_id table not found')

        self.init_append_rows()
        if not self.row_data.get('row'):
            return

        try:
            row = self.auto_rule.dtable_server_api.append_row(self.get_table_name(self.dst_table_id), self.row_data['row'])
        except Exception as e:
            logger.error('update dtable: %s, error: %s', self.auto_rule.dtable_uuid, e)
            return
        self.auto_rule.set_done_actions()

        try:
            self.row_data['row']['_id'] = row['_id']
            self.send_selected_collaborator_notis(self.row_data['row'])
        except Exception as e:
            logger.exception('send selected notifications error: %s', e)


class TriggerWorkflowAction(BaseAction):

    VALID_COLUMN_TYPES = [
        ColumnTypes.TEXT,
        ColumnTypes.DATE,
        ColumnTypes.LONG_TEXT,
        ColumnTypes.CHECKBOX,
        ColumnTypes.SINGLE_SELECT,
        ColumnTypes.MULTIPLE_SELECT,
        ColumnTypes.URL,
        ColumnTypes.DURATION,
        ColumnTypes.NUMBER,
        ColumnTypes.COLLABORATOR,
        ColumnTypes.EMAIL,
        ColumnTypes.RATE,
    ]

    def __init__(self, auto_rule, action_type, row, token):
        super().__init__(auto_rule, action_type, None)
        self.row = row or {}
        self.row_data = {
            'row': {}
        }
        self.token = token
        self.is_valid = True
        self.init_updates()

    def format_time_by_offset(self, offset, format_length):
        cur_datetime = datetime.now()
        cur_datetime_offset = cur_datetime + timedelta(days=offset)
        if format_length == 2:
            return cur_datetime_offset.strftime("%Y-%m-%d %H:%M")
        if format_length == 1:
            return cur_datetime_offset.strftime("%Y-%m-%d")

    def is_workflow_valid(self):
        sql = 'SELECT workflow_config FROM dtable_workflows WHERE token=:token AND dtable_uuid=:dtable_uuid'
        try:
            result = self.auto_rule.db_session.execute(sql, {'token': self.token, 'dtable_uuid': self.auto_rule.dtable_uuid.replace('-', '')}).fetchone()
            if not result:
                return False
            workflow_config = json.loads(result[0])
        except Exception as e:
            logger.warning('checkout workflow: %s of dtable: %s error: %s', self.token, self.auto_rule.dtable_uuid)
            return False
        workflow_table_id = workflow_config.get('table_id')
        return workflow_table_id == self.auto_rule.table_id

    def init_updates(self):
        self.is_valid = self.is_workflow_valid()
        if not self.is_valid:
            return
        # filter columns in view and type of column is in VALID_COLUMN_TYPES
        filtered_updates = {}
        for col in self.auto_rule.view_columns:
            if col.get('type') not in self.VALID_COLUMN_TYPES:
                continue
            col_name = col.get('name')
            col_type = col.get('type')
            col_key = col.get('key')
            if col_key in self.row.keys():
                if col_type == ColumnTypes.DATE:
                    time_format = col.get('data', {}).get('format', '')
                    format_length = len(time_format.split(" "))
                    try:
                        time_dict = self.row.get(col_key)
                        if not time_dict:
                            continue
                        set_type = time_dict.get('set_type')
                        if set_type == 'specific_value':
                            time_value = time_dict.get('value')
                            filtered_updates[col_name] = time_value
                        elif set_type == 'relative_date':
                            offset = time_dict.get('offset')
                            filtered_updates[col_name] = self.format_time_by_offset(int(offset), format_length)
                    except Exception as e:
                        logger.error(e)
                        filtered_updates[col_name] = self.row.get(col_key)
                else:
                    filtered_updates[col_name] = self.parse_column_value(col, self.row.get(col_key))
        self.row_data['row'] = filtered_updates

    def do_action(self):
        if not self.is_valid:
            return
        try:
            logger.debug('rule: %s new workflow: %s task row data: %s', self.auto_rule.rule_id, self.token, self.row_data)
            resp_data = self.auto_rule.dtable_server_api.append_row(self.auto_rule.table_info['name'], self.row_data['row'])
            row_id = resp_data['_id']
            logger.debug('rule: %s new workflow: %s task row_id: %s', self.auto_rule.rule_id, self.token, row_id)
        except Exception as e:
            logger.error('rule: %s submit workflow: %s append row dtable: %s, error: %s', self.auto_rule.rule_id, self.token, self.auto_rule.dtable_uuid, e)
            return

        internal_submit_workflow_url = DTABLE_WEB_SERVICE_URL.strip('/') + '/api/v2.1/workflows/%s/internal-task-submit/' % self.token
        data = {
            'row_id': row_id,
            'replace': 'true',
            'submit_from': 'Automation Rule',
            'automation_rule_id': self.auto_rule.rule_id
        }
        logger.debug('trigger workflow data: %s', data)
        try:
            header_token = 'Token ' + jwt.encode({'token': self.token}, DTABLE_PRIVATE_KEY, 'HS256')
            resp = requests.post(internal_submit_workflow_url, data=data, headers={'Authorization': header_token})
            if resp.status_code != 200:
                logger.error('rule: %s row_id: %s new workflow: %s task error status code: %s content: %s', self.auto_rule.rule_id, row_id, self.token, resp.status_code, resp.content)
            self.auto_rule.set_done_actions()
        except Exception as e:
            logger.error('submit workflow: %s row_id: %s error: %s', self.token, row_id, e)


class CalculateAction(BaseAction):
    VALID_CALCULATE_COLUMN_TYPES = [
        ColumnTypes.NUMBER,
        ColumnTypes.DURATION,
        ColumnTypes.FORMULA,
        ColumnTypes.LINK_FORMULA
    ]
    VALID_RANK_COLUMN_TYPES = [
        ColumnTypes.NUMBER,
        ColumnTypes.DURATION,
        ColumnTypes.DATE,
        ColumnTypes.RATE,
        ColumnTypes.FORMULA,
        ColumnTypes.LINK_FORMULA
    ]
    VALID_RESULT_COLUMN_TYPES = [ColumnTypes.NUMBER]

    def __init__(self, auto_rule, action_type, data, calculate_column_key, result_column_key):
        super().__init__(auto_rule, action_type, data)
        # this action contains calculate_accumulated_value, calculate_delta, calculate_rank and calculate_percentage
        self.calculate_column_key = calculate_column_key
        self.result_column_key = result_column_key
        self.column_key_dict = {col.get('key'): col for col in self.auto_rule.view_columns}
        self.update_rows = []
        self.rank_rows = []
        self.is_group_view = False

    def parse_group_rows(self, view_rows):
        for group in view_rows:
            group_subgroups = group.get('subgroups')
            group_rows = group.get('rows')
            if group_rows is None and group_subgroups:
                self.parse_group_rows(group.get('subgroups'))
            else:
                self.parse_rows(group_rows)

    def get_row_value(self, row, column):
        col_name = column.get('name')
        value = row.get(col_name)
        if self.is_group_view and column.get('type') in [ColumnTypes.FORMULA, ColumnTypes.LINK_FORMULA]:
            value = parse_formula_number(value, column.get('data'))
        try:
            return float(value)
        except:
            return 0

    def get_date_value(self, row, col_name):
        return parser.parse(row.get(col_name))

    def parse_rows(self, rows):
        calculate_col = self.column_key_dict.get(self.calculate_column_key, {})
        result_col = self.column_key_dict.get(self.result_column_key, {})
        result_col_name = result_col.get('name')
        result_value = 0

        if self.action_type == 'calculate_accumulated_value':
            for index in range(len(rows)):
                row_id = rows[index].get('_id')
                result_value += self.get_row_value(rows[index], calculate_col)
                result_row = {result_col_name: result_value}
                self.update_rows.append({'row_id': row_id, 'row': result_row})

        elif self.action_type == 'calculate_delta':
            for index in range(len(rows)):
                row_id = rows[index].get('_id')
                if index > 0:
                    pre_value = self.get_row_value(rows[index], calculate_col)
                    next_value = self.get_row_value(rows[index-1], calculate_col)
                    result_value = pre_value - next_value
                    result_row = {result_col_name: result_value}
                    self.update_rows.append({'row_id': row_id, 'row': result_row})

        elif self.action_type == 'calculate_percentage':
            sum_calculate = sum([float(self.get_row_value(row, calculate_col)) for row in rows])
            for row in rows:
                row_id = row.get('_id')
                try:
                    result_value = float(self.get_row_value(row, calculate_col)) / sum_calculate
                except ZeroDivisionError:
                    result_value = None
                self.update_rows.append({'row_id': row_id, 'row': {result_col_name: result_value}})

        elif self.action_type == 'calculate_rank':
            self.rank_rows.extend(rows)

    def query_table_rows(self, table_name, columns, filter_conditions, query_columns):
        offset = 10000
        start = 0
        rows = []
        query_clause = "*"
        if query_columns:
            if "_id" not in query_columns:
                query_columns.append("_id")
            query_clause = ",".join(["`%s`" % cn for cn in query_columns])
        while True:
            filter_conditions['start'] = start
            filter_conditions['limit'] = offset

            sql = filter2sql(table_name, columns, filter_conditions, by_group=False)
            sql = sql.replace("*", query_clause, 1)
            response_rows, _ = self.auto_rule.dtable_db_api.query(sql)
            rows.extend(response_rows)

            start += offset
            if len(response_rows) < offset:
                break
        return rows

    def can_rank_date(self, column):
        column_type = column.get('type')
        if column_type == ColumnTypes.DATE:
            return True
        elif column_type == ColumnTypes.FORMULA and column.get('data').get('result_type') == 'date':
            return True
        elif column_type == ColumnTypes.LINK_FORMULA and column.get('data').get('result_type') == 'date':
            return True
        return False

    
    def get_columns(self, table_name):
        dtable_metadata = self.auto_rule.dtable_metadata
        for table in dtable_metadata.get('tables', []):
            if table.get('name') == table_name:
                return table.get('columns', [])
        return []

    def init_updates(self):
        calculate_col = self.column_key_dict.get(self.calculate_column_key, {})
        result_col = self.column_key_dict.get(self.result_column_key, {})
        if not calculate_col or not result_col or result_col.get('type') not in self.VALID_RESULT_COLUMN_TYPES:
            raise RuleInvalidException('calculate_col not found, result_col not found or result_col type invalid')
        if self.action_type == 'calculate_rank':
            if calculate_col.get('type') not in self.VALID_RANK_COLUMN_TYPES:
                raise RuleInvalidException('calculate_rank calculate_col type invalid')
        else:
            if calculate_col.get('type') not in self.VALID_CALCULATE_COLUMN_TYPES:
                raise RuleInvalidException('calculate_col type invalid')

        calculate_col_name = calculate_col.get('name')
        result_col_name = result_col.get('name')
        table_name = self.auto_rule.table_info['name']
        view_name = self.auto_rule.view_info['name']

        columns = self.get_columns(table_name)

        self.is_group_view = True if self.auto_rule.view_info.get('groupbys') else False

        if self.is_group_view:
            view_rows = self.auto_rule.dtable_server_api.view_rows(table_name, view_name, True)
        else:
            filter_conditions = {
                'sorts': self.auto_rule.view_info.get('sorts'),
                'filters': self.auto_rule.view_info.get('filters'),
                'filter_conjunction': self.auto_rule.view_info.get('filter_conjunction'),
            }
            view_rows = self.query_table_rows(table_name, columns, filter_conditions, [calculate_col_name])

        if view_rows and ('rows' in view_rows[0] or 'subgroups' in view_rows[0]):
            self.parse_group_rows(view_rows)
        else:
            self.parse_rows(view_rows)

        if self.action_type == 'calculate_rank':
            to_be_sorted_rows = []
            for row in self.rank_rows:
                if row.get(calculate_col_name):
                    to_be_sorted_rows.append(row)
                    continue
                self.update_rows.append({'row_id': row.get('_id'), 'row': {result_col_name: None}})

            if is_number_format(calculate_col):
                to_be_sorted_rows = sorted(to_be_sorted_rows, key=lambda x: float(self.get_row_value(x, calculate_col)), reverse=True)

            elif self.can_rank_date(calculate_col):
                to_be_sorted_rows = sorted(to_be_sorted_rows, key=lambda x: self.get_date_value(x, calculate_col_name), reverse=True)

            rank = 0
            real_rank = 0
            pre_value = None
            for row in to_be_sorted_rows:
                cal_value = row.get(calculate_col_name)
                row_id = row.get('_id')
                real_rank += 1
                if rank == 0 or cal_value != pre_value:
                    rank = real_rank
                    pre_value = cal_value
                result_row = {result_col_name: rank}
                self.update_rows.append({'row_id': row_id, 'row': result_row})

    def can_do_action(self):
        if not self.auto_rule.current_valid:
            return False
        if not self.calculate_column_key or not self.result_column_key:
            return False
        return True

    def do_action(self):
        if not self.can_do_action():
            return

        self.init_updates()

        table_name = self.auto_rule.table_info.get('name')
        step = 1000
        for i in range(0, len(self.update_rows), step):
            try:
                self.auto_rule.dtable_server_api.batch_update_rows(table_name, self.update_rows[i: i+step])
            except Exception as e:
                logger.error('batch update dtable: %s, error: %s', self.auto_rule.dtable_uuid, e)
                return
        self.auto_rule.set_done_actions()


class LookupAndCopyAction(BaseAction):
    VALID_COLUMN_TYPES = [
        ColumnTypes.TEXT,
        ColumnTypes.NUMBER,
        ColumnTypes.CHECKBOX,
        ColumnTypes.DATE,
        ColumnTypes.LONG_TEXT,
        ColumnTypes.COLLABORATOR,
        ColumnTypes.GEOLOCATION,
        ColumnTypes.URL,
        ColumnTypes.DURATION,
        ColumnTypes.EMAIL,
        ColumnTypes.RATE,
        ColumnTypes.FORMULA,
        ColumnTypes.AUTO_NUMBER,
    ]

    def __init__(self, auto_rule, action_type, data, table_condition, equal_column_conditions, fill_column_conditions):
        super().__init__(auto_rule, action_type, data=data)

        self.table_condition = table_condition
        self.equal_column_conditions = equal_column_conditions
        self.fill_column_conditions = fill_column_conditions
        self.from_table_name = ''
        self.copy_to_table_name = ''

        self.update_rows = []

    def get_table_names_dict(self):
        dtable_metadata = self.auto_rule.dtable_metadata
        tables = dtable_metadata.get('tables', [])
        return {table.get('_id'): table.get('name') for table in tables}

    def get_columns_dict(self, table_id):
        dtable_metadata = self.auto_rule.dtable_metadata
        column_dict = {}
        for table in dtable_metadata.get('tables', []):
            if table.get('_id') == table_id:
                for col in table.get('columns'):
                    column_dict[col.get('key')] = col
        return column_dict

    def query_table_rows(self, table_name, column_names):
        start = 0
        step = 10000
        result_rows = []
        query_clause = '*'
        if column_names:
            query_columns = list(set(column_names))
            if "_id" not in query_columns:
                query_columns.append("_id")
            query_clause = ",".join(["`%s`" % cn for cn in query_columns])
            
        while True:
            sql = f"select {query_clause} from `{table_name}` limit {start}, {step}"
            try:
                results, _ = self.auto_rule.dtable_db_api.query(sql)
            except Exception as e:
                logger.exception(e)
                logger.error('query dtable: %s, table name: %s, error: %s', self.auto_rule.dtable_uuid, table_name, e)
                return []
            result_rows += results
            start += step
            if len(results) < step:
                break
        return result_rows

    def init_updates(self):
        from_table_id = self.table_condition.get('from_table_id')
        copy_to_table_id = self.table_condition.get('copy_to_table_id')

        from_column_dict = self.get_columns_dict(from_table_id)
        copy_to_column_dict = self.get_columns_dict(copy_to_table_id)
        table_name_dict = self.get_table_names_dict()

        self.from_table_name = table_name_dict.get(from_table_id)
        self.copy_to_table_name = table_name_dict.get(copy_to_table_id)

        if not self.from_table_name or not self.copy_to_table_name:
            raise RuleInvalidException('from_table_name or copy_to_table_name not found')

        equal_from_columns = []
        equal_copy_to_columns = []
        fill_from_columns = []
        fill_copy_to_columns = []
        # check column valid
        try:
            for col in self.equal_column_conditions:
                from_column = from_column_dict[col['from_column_key']]
                copy_to_column = copy_to_column_dict[col['copy_to_column_key']]
                if from_column.get('type') not in self.VALID_COLUMN_TYPES or copy_to_column.get('type') not in self.VALID_COLUMN_TYPES:
                    raise RuleInvalidException('from_column or copy_to_column type invalid')
                equal_from_columns.append(from_column.get('name'))
                equal_copy_to_columns.append(copy_to_column.get('name'))

            for col in self.fill_column_conditions:
                from_column = from_column_dict[col['from_column_key']]
                copy_to_column = copy_to_column_dict[col['copy_to_column_key']]
                if from_column.get('type') not in self.VALID_COLUMN_TYPES or copy_to_column.get('type') not in self.VALID_COLUMN_TYPES:
                    raise RuleInvalidException('from_column or copy_to_column type invalid')
                fill_from_columns.append(from_column.get('name'))
                fill_copy_to_columns.append(copy_to_column.get('name'))
        except KeyError as e:
            logger.error('dtable: %s, from_table: %s or copy_to_table:%s column key error: %s', self.auto_rule.dtable_uuid, self.from_table_name, self.copy_to_table_name, e)
            raise RuleInvalidException('from_column or copy_to_column not found')

        from_columns = equal_from_columns + fill_from_columns
        copy_to_columns = equal_copy_to_columns + fill_copy_to_columns
        from_table_rows = self.query_table_rows(self.from_table_name, from_columns)
        copy_to_table_rows = self.query_table_rows(self.copy_to_table_name, copy_to_columns)

        from_table_rows_dict = {}
        for from_row in from_table_rows:
            from_key = '-'
            for equal_condition in self.equal_column_conditions:
                from_column_key = equal_condition['from_column_key']
                from_column = from_column_dict[from_column_key]
                from_column_name = from_column.get('name')
                from_value = from_row.get(from_column_name)
                from_value = cell_data2str(from_value)
                from_key += from_value + from_column_key + '-'
            from_key = str(hash(from_key))
            from_table_rows_dict[from_key] = from_row

        for copy_to_row in copy_to_table_rows:
            copy_to_key = '-'
            for equal_condition in self.equal_column_conditions:
                from_column_key = equal_condition['from_column_key']
                copy_to_column_key = equal_condition['copy_to_column_key']
                copy_to_column = copy_to_column_dict[copy_to_column_key]
                copy_to_column_name = copy_to_column.get('name')
                copy_to_value = copy_to_row.get(copy_to_column_name)
                copy_to_value = cell_data2str(copy_to_value)
                copy_to_key += copy_to_value + from_column_key + '-'
            copy_to_key = str(hash(copy_to_key))
            from_row = from_table_rows_dict.get(copy_to_key)
            if not from_table_rows_dict.get(copy_to_key):
                continue
            row = {}
            for fill_condition in self.fill_column_conditions:
                from_column_key = fill_condition.get('from_column_key')
                from_column = from_column_dict[from_column_key]
                from_column_name = from_column.get('name')
                copy_to_column_key = fill_condition.get('copy_to_column_key')
                copy_to_column = copy_to_column_dict[copy_to_column_key]
                copy_to_column_name = copy_to_column.get('name')
                from_value = from_row.get(from_column_name, '')
                copy_to_value = copy_to_row.get(copy_to_column_name, '')

                # do not need convert value to str because column type may be different
                if from_value == copy_to_value:
                    continue

                copy_to_column_name = copy_to_column_dict[copy_to_column_key].get('name')
                copy_to_column_type = copy_to_column_dict[copy_to_column_key].get('type')

                if copy_to_column_type == ColumnTypes.CHECKBOX:
                    from_value = True if from_value else False
                elif copy_to_column_type == ColumnTypes.DATE:
                    if isinstance(from_value, str) and 'T' in from_value:
                        d = from_value.split('T')
                        from_value = d[0] + ' ' + d[1].split('+')[0]
                row[copy_to_column_name] = from_value

            self.update_rows.append({'row_id': copy_to_row['_id'], 'row': row})

    def can_do_action(self):
        if not self.auto_rule.current_valid:
            return False
        if not self.table_condition or not self.equal_column_conditions or not self.fill_column_conditions:
            return False
        return True

    def do_action(self):
        if not self.can_do_action():
            return
        self.init_updates()

        step = 1000
        for i in range(0, len(self.update_rows), step):
            try:
                self.auto_rule.dtable_server_api.batch_update_rows(self.copy_to_table_name, self.update_rows[i: i + step])
            except Exception as e:
                logger.error('batch update dtable: %s, error: %s', self.auto_rule.dtable_uuid, e)
                return
        self.auto_rule.set_done_actions()


class ExtractUserNameAction(BaseAction):
    VALID_EXTRACT_COLUMN_TYPES = [
        ColumnTypes.CREATOR,
        ColumnTypes.LAST_MODIFIER,
        ColumnTypes.COLLABORATOR
    ]
    VALID_RESULT_COLUMN_TYPES = [
        ColumnTypes.TEXT
    ]

    def __init__(self, auto_rule, action_type, data, extract_column_key, result_column_key):
        super().__init__(auto_rule, action_type, data)
        self.extract_column_key = extract_column_key
        self.result_column_key = result_column_key

        self.column_key_dict = {col.get('key'): col for col in self.auto_rule.view_columns}
        self.update_rows = []

    def query_user_rows(self, table_name, extract_column_name, result_column_name):
        start = 0
        step = 10000
        result_rows = []
        while True:
            sql = f"select `_id`, `{extract_column_name}`, `{result_column_name}` from `{table_name}` limit {start},{step}"
            try:
                results, _ = self.auto_rule.dtable_db_api.query(sql)
            except Exception as e:
                logger.error('query dtable: %s, table name: %s, error: %s', self.auto_rule.dtable_uuid, table_name, e)
                return []
            result_rows += results
            start += step
            if len(results) < step:
                break
        return result_rows

    def init_updates(self):
        extract_column = self.column_key_dict.get(self.extract_column_key, {})
        result_column = self.column_key_dict.get(self.result_column_key, {})
        result_column_type = result_column.get('type')
        extract_column_type = extract_column.get('type')
        if not extract_column or not result_column or result_column_type not in self.VALID_RESULT_COLUMN_TYPES \
                or extract_column_type not in self.VALID_EXTRACT_COLUMN_TYPES:
            raise RuleInvalidException('extract_column not found, result_column not found, result_column_type invalid or extract_column_type invalid')

        extract_column_name = extract_column.get('name')
        result_column_name = result_column.get('name')
        table_name = self.auto_rule.table_info.get('name')
        user_rows = self.query_user_rows(table_name, extract_column_name, result_column_name)
        unknown_user_id_set = set()
        unknown_user_rows = []
        related_users_dict = self.auto_rule.related_users_dict
        for row in user_rows:
            result_col_value = row.get(result_column_name)
            if extract_column_type == ColumnTypes.COLLABORATOR:
                user_ids = row.get(extract_column_name, [])
                if not user_ids:
                    if result_col_value:
                        self.update_rows.append({'row_id': row.get('_id'), 'row': {result_column_name: ''}})
                    continue
                is_all_related_user = True
                nicknames = []
                for user_id in user_ids:
                    related_user = related_users_dict.get(user_id)
                    if not related_user:
                        unknown_user_id_set.add(user_id)
                        if is_all_related_user:
                            unknown_user_rows.append(row)
                        is_all_related_user = False
                    else:
                        nickname = related_user.get('name')
                        nicknames.append(nickname)

                nicknames_str = ','.join(nicknames)
                if is_all_related_user and result_col_value != nicknames_str:
                    self.update_rows.append({'row_id': row.get('_id'), 'row': {result_column_name: nicknames_str}})
            else:
                user_id = row.get(extract_column_name)
                if not user_id:
                    if result_col_value:
                        self.update_rows.append({'row_id': row.get('_id'), 'row': {result_column_name: ''}})
                    continue

                related_user = related_users_dict.get(user_id, '')
                if related_user:
                    nickname = related_user.get('name')
                    if nickname != result_col_value:
                        self.update_rows.append({'row_id': row.get('_id'), 'row': {result_column_name: nickname}})
                else:
                    unknown_user_id_set.add(user_id)
                    unknown_user_rows.append(row)

        email2nickname = {}
        if unknown_user_rows:
            unknown_user_id_list = list(unknown_user_id_set)
            step = 1000
            start = 0
            for i in range(0, len(unknown_user_id_list), step):
                users_dict = get_nickname_by_usernames(unknown_user_id_list[start: start + step], self.auto_rule.db_session)
                email2nickname.update(users_dict)
                start += step

        for user_row in unknown_user_rows:
            result_col_value = user_row.get(result_column_name)
            if extract_column_type == ColumnTypes.COLLABORATOR:
                user_ids = user_row.get(extract_column_name)
                nickname_list = []
                for user_id in user_ids:
                    related_user = related_users_dict.get(user_id)
                    if not related_user:
                        nickname = email2nickname.get(user_id)
                    else:
                        nickname = related_user.get('name')
                    nickname_list.append(nickname)
                update_result_value = ','.join(nickname_list)
            else:
                user_id = user_row.get(extract_column_name)
                nickname = email2nickname.get(user_id)
                update_result_value = nickname
            if result_col_value != update_result_value:
                self.update_rows.append({'row_id': user_row.get('_id'), 'row': {result_column_name: update_result_value}})

    def can_do_action(self):
        if not self.auto_rule.current_valid:
            return False
        if not self.extract_column_key or not self.result_column_key:
            return False
        return True

    def do_action(self):
        if not self.can_do_action():
            return

        self.init_updates()

        table_name = self.auto_rule.table_info.get('name')
        step = 1000
        for i in range(0, len(self.update_rows), step):
            try:
                self.auto_rule.dtable_server_api.batch_update_rows(table_name, self.update_rows[i: i+step])
            except Exception as e:
                logger.error('batch update dtable: %s, error: %s', self.auto_rule.dtable_uuid, e)
                return
        self.auto_rule.set_done_actions()


class ConvertPageToPDFAction(BaseAction):

    def __init__(self, auto_rule, action_type, data, page_id, file_name, target_column_key, repo_id, workspace_id):
        super().__init__(auto_rule, action_type, data)
        self.page_id = page_id
        self.page = None
        self.file_name = file_name
        self.target_column_key = target_column_key
        self.target_column = None
        self.repo_id = repo_id
        self.workspace_id = workspace_id

    def can_do_action(self):
        if not self.auto_rule.current_valid:
            return False
        return True

    def fill_msg_blanks_with_sql(self, column_blanks, col_name_dict, row):
        return fill_msg_blanks_with_sql_row(self.file_name, column_blanks, col_name_dict, row, self.auto_rule.db_session)

    def do_action(self):
        if not self.can_do_action():
            return
        rows = self.auto_rule.get_trigger_conditions_rows(self, warning_rows=CONVERT_PAGE_TO_PDF_ROWS_LIMIT)[:CONVERT_PAGE_TO_PDF_ROWS_LIMIT]
        if not rows:
            return
        file_names_dict = {}
        blanks = set(re.findall(r'\{([^{]*?)\}', self.file_name))
        col_name_dict = {col.get('name'): col for col in self.auto_rule.table_info['columns']}
        column_blanks = [blank for blank in blanks if blank in col_name_dict]
        for row in rows:
            file_name = self.fill_msg_blanks_with_sql(column_blanks, col_name_dict, row)
            file_names_dict[row['_id']] = file_name
        try:
            # put resources check to the place before convert page,
            # because there is a distance between put task to queue and convert page
            conver_page_to_pdf_manager.add_task({
                'dtable_uuid': self.auto_rule.dtable_uuid,
                'page_id': self.page_id,
                'row_ids': [row['_id'] for row in rows],
                'repo_id': self.repo_id,
                'workspace_id': self.workspace_id,
                'file_names_dict': file_names_dict,
                'target_column_key': self.target_column_key,
                'table_id': self.auto_rule.table_id
            })
        except Full:
            self.auto_rule.append_warning({
                'type': 'convert_page_to_pdf_server_busy',
                'page_id': self.page_id,
                'page_name': self.page['page_name']
            })
        self.auto_rule.set_done_actions()


class RuleInvalidException(Exception):
    """
    Exception which indicates rule need to be set is_valid=Fasle
    """
    pass


class AutomationRule:

    def __init__(self, data, db_session, raw_trigger, raw_actions, options, metadata_cache_manager: BaseMetadataCacheManager, per_minute_trigger_limit=None):
        self.rule_id = options.get('rule_id', None)
        self.rule_name = ''
        self.run_condition = options.get('run_condition', None)
        self.dtable_uuid = options.get('dtable_uuid', None)
        self.trigger = None
        self.action_infos = []
        self.last_trigger_time = options.get('last_trigger_time', None)
        self.trigger_count = options.get('trigger_count', None)
        self.org_id = options.get('org_id', None)
        self.creator = options.get('creator', None)
        self.data = data
        self.db_session = db_session

        self.dtable_server_api = DTableServerAPI('Automation Rule', str(UUID(self.dtable_uuid)), get_inner_dtable_server_url())
        self.dtable_db_api = DTableDBAPI('Automation Rule', str(UUID(self.dtable_uuid)), INNER_DTABLE_DB_URL)
        self.dtable_web_api = DTableWebAPI(DTABLE_WEB_SERVICE_URL)

        self.table_id = None
        self.view_id = None

        self._table_info = None
        self._view_info = None
        self._dtable_metadata = None
        self._access_token = None
        self._view_columns = None
        self.can_run_python = None
        self.scripts_running_limit = None
        self._related_users = None
        self._related_users_dict = None
        self._trigger_conditions_rows = None

        self.metadata_cache_manager = metadata_cache_manager

        self.cache_key = 'AUTOMATION_RULE:%s' % uuid_str_to_36_chars(self.dtable_uuid)
        self.task_run_success = True

        self.done_actions = False
        self.load_trigger_and_actions(raw_trigger, raw_actions)

        self.current_valid = True

        self.per_minute_trigger_limit = per_minute_trigger_limit or 1000

        self.warnings = []

    def load_trigger_and_actions(self, raw_trigger, raw_actions):
        self.trigger = json.loads(raw_trigger)

        self.table_id = self.trigger.get('table_id')
        if self.run_condition == PER_UPDATE:
            self._table_name = self.data.get('table_name', '')
        self.view_id = self.trigger.get('view_id')

        self.rule_name = self.trigger.get('rule_name', '')
        self.action_infos = json.loads(raw_actions)

    @property
    def access_token(self):

        if not self._access_token:
            self._access_token = jwt.encode(
                payload={
                    'exp': int(time.time()) + 300,
                    'dtable_uuid': uuid_str_to_36_chars(self.dtable_uuid),
                    'username': 'Automation Rule',
                    'permission': 'rw',
                },
                key=DTABLE_PRIVATE_KEY
            )
        return self._access_token

    @property
    def headers(self):
        return self.dtable_server_api.headers
    

    def cache_clean(self):
        # when some attribute changes, such as option added in single-select column
        # the cache should be cleared
        self._table_info = None
        self._view_info = None
        self._dtable_metadata = None
        self._view_columns = None
        self.metadata_cache_manager.clean_metadata(self.dtable_uuid)

    @property
    def dtable_metadata(self):
        if not self._dtable_metadata:
            self._dtable_metadata = self.metadata_cache_manager.get_metadata(self.dtable_uuid)
        return self._dtable_metadata

    @property
    def view_columns(self):
        """
        columns of the view defined in trigger
        """
        if not self._view_columns:
            table_name = self.table_info['name']
            view_name = self.view_info['name']
            self._view_columns = self.dtable_server_api.list_columns(table_name, view_name=view_name)
        return self._view_columns

    @property
    def table_info(self):
        """
        name of table defined in rule
        """
        if not self._table_info:
            dtable_metadata = self.dtable_metadata
            tables = dtable_metadata.get('tables', [])
            for table in tables:
                if table.get('_id') == self.table_id:
                    self._table_info = table
                    break
            if not self._table_info:
                raise RuleInvalidException('table not found')
        return self._table_info

    @property
    def view_info(self):
        table_info = self.table_info
        if not self.view_id:
            self._view_info = table_info['views'][0]
            return self._view_info
        for view in table_info['views']:
            if view['_id'] == self.view_id:
                self._view_info = view
                break
        if not self._view_info:
            raise RuleInvalidException('view not found')
        return self._view_info

    @property
    def related_users(self):
        if not self._related_users:
            try:
                self._related_users = self.dtable_web_api.get_related_users(self.dtable_uuid)
            except Exception as e:
                logger.error('rule: %s uuid: %srequest related users error: %s', self.rule_id, self.dtable_uuid, e)
                raise RuleInvalidException('rule: %s uuid: %srequest related users error: %s' % (self.rule_id, self.dtable_uuid, e))
        return self._related_users

    @property
    def related_users_dict(self):
        if not self._related_users_dict:
            self._related_users_dict = {user['email']: user for user in self.related_users}
        return self._related_users_dict


    def get_temp_api_token(self, username=None, app_name=None):
        payload = {
            'dtable_uuid': self.dtable_uuid,
            'exp': int(time.time()) + 60 * 60,
        }
        if username:
            payload['username'] = username
        if app_name:
            payload['app_name'] = app_name
        temp_api_token = jwt.encode(payload, SEATABLE_FAAS_AUTH_TOKEN, algorithm='HS256')
        return temp_api_token

    def get_trigger_conditions_rows(self, action: BaseAction, warning_rows=50):
        if self._trigger_conditions_rows is not None:
            if len(self._trigger_conditions_rows) > warning_rows:
                self.append_warning({
                    'type': 'condition_rows_exceed',
                    'condition_rows_limit': warning_rows,
                    'action_type': action.action_type
                })
            return self._trigger_conditions_rows
        filters = self.trigger.get('filters', [])
        filter_conjunction = self.trigger.get('filter_conjunction', 'And')
        view_info = self.view_info
        view_filters = view_info.get('filters', [])
        view_filter_conjunction = view_info.get('filter_conjunction', 'And')
        filter_groups = []

        if view_filters:
            for filter_item in view_filters:
                if filter_item.get('filter_predicate') in ('include_me', 'is_current_user_ID'):
                    raise RuleInvalidException('view filter has invalid filter')
            filter_groups.append({'filters': view_filters, 'filter_conjunction': view_filter_conjunction})

        if filters:
            # remove the duplicate filter which may already exist in view filter
            trigger_filters = []
            for filter_item in filters:
                if filter_item.get('filter_predicate') in ('include_me', 'is_current_user_ID'):
                    raise RuleInvalidException('rule filter has invalid filter')
                if filter_item not in view_filters:
                    trigger_filters.append(filter_item)
            if trigger_filters:
                filter_groups.append({'filters': trigger_filters, 'filter_conjunction': filter_conjunction})

        filter_conditions = {
                'filter_groups': filter_groups,
                'group_conjunction': 'And',
                'start': 0,
                'limit': 500,
            }
        table_name = self.table_info.get('name')
        columns = self.table_info.get('columns')

        try:
            sql = filter2sql(table_name, columns, filter_conditions, by_group=True)
        except (ValueError, ColumnFilterInvalidError) as e:
            logger.warning('wrong filter in rule: %s trigger filters filter_conditions: %s error: %s', self.rule_id, filter_conditions, e)
            raise RuleInvalidException('wrong filter in rule: %s trigger filters error: %s' % (self.rule_id, e))
        except Exception as e:
            logger.exception(e)
            logger.error('rule: %s filter_conditions: %s filter2sql error: %s', self.rule_id, filter_conditions, e)
            self._trigger_conditions_rows = []
            return self._trigger_conditions_rows
        try:
            rows_data, _ = self.dtable_db_api.query(sql, convert=False)
        except RowsQueryError:
            raise RuleInvalidException('wrong filter in rule: %s trigger filters' % self.rule_id)
        except Exception as e:
            logger.error('request filter rows error: %s', e)
            self._trigger_conditions_rows = []
            return self._trigger_conditions_rows
        logger.debug('Number of filter rows by auto-rule %s is: %s, dtable_uuid: %s, details: %s' % (
            self.rule_id,
            len(rows_data),
            self.dtable_uuid,
            json.dumps(filter_conditions)
        ))
        self._trigger_conditions_rows = rows_data
        if len(self._trigger_conditions_rows) > warning_rows:
            self.append_warning({
                'type': 'condition_rows_exceed',
                'condition_rows_limit': warning_rows,
                'action_type': action.action_type
            })
        return self._trigger_conditions_rows

    def append_warning(self, warning_detail):
        self.warnings.append(warning_detail)

    def can_do_actions(self):
        if self.trigger.get('condition') not in (CONDITION_FILTERS_SATISFY, CONDITION_PERIODICALLY, CONDITION_ROWS_ADDED, CONDITION_PERIODICALLY_BY_CONDITION):
            return False

        if self.trigger.get('condition') == CONDITION_ROWS_ADDED:
            if self.data.get('op_type') not in ['insert_row', 'append_rows', 'insert_rows']:
                return False

        if self.trigger.get('condition') in [CONDITION_FILTERS_SATISFY, CONDITION_ROWS_MODIFIED]:
            if self.data.get('op_type') not in ['modify_row', 'modify_rows', 'add_link', 'update_links', 'update_rows_links', 'remove_link', 'move_group_rows']:
                return False

        if self.run_condition == PER_UPDATE:
            # automation rule triggered by human or code, perhaps triggered quite quickly
            if self.per_minute_trigger_limit <= 0:
                return True
            trigger_times = redis_cache.get(self.cache_key)
            if not trigger_times:
                return True
            trigger_times = trigger_times.split(',')
            if len(trigger_times) >= self.per_minute_trigger_limit and time.time() - int(trigger_times[0]) < 60:
                logger.warning('automation rule: %s exceed the trigger limit (%s times) within 1 minute', self.rule_id, self.per_minute_trigger_limit)
                return False
            return True

        elif self.run_condition in CRON_CONDITIONS:
            cur_datetime = datetime.now()
            cur_hour = cur_datetime.hour
            cur_week_day = cur_datetime.isoweekday()
            cur_month_day = cur_datetime.day
            if self.run_condition == PER_DAY:
                trigger_hour = self.trigger.get('notify_hour', 12)
                if cur_hour != trigger_hour:
                    return False
            elif self.run_condition == PER_WEEK:
                trigger_hour = self.trigger.get('notify_week_hour', 12)
                trigger_day = self.trigger.get('notify_week_day', 7)
                if cur_hour != trigger_hour or cur_week_day != trigger_day:
                    return False
            else:
                trigger_hour = self.trigger.get('notify_month_hour', 12)
                trigger_day = self.trigger.get('notify_month_day', 1)
                if cur_hour != trigger_hour or cur_month_day != trigger_day:
                    return False
            return True

        return False


    def can_condition_trigger_action(self, action):
        action_type = action.get('type')
        run_condition = self.run_condition
        trigger_condition = self.trigger.get('condition')
        if action_type in ('notify', 'app_notify'):
            return True
        elif action_type == 'update_record':
            if run_condition == PER_UPDATE:
                return True
            if run_condition in CRON_CONDITIONS and trigger_condition == CONDITION_PERIODICALLY_BY_CONDITION:
                return True
            return False
        elif action_type == 'add_record':
            if run_condition == PER_UPDATE:
                return True
            if run_condition in CRON_CONDITIONS and trigger_condition == CONDITION_PERIODICALLY:
                return True
            return False
        elif action_type == 'lock_record':
            if run_condition == PER_UPDATE:
                return True
            if run_condition in CRON_CONDITIONS and trigger_condition == CONDITION_PERIODICALLY_BY_CONDITION:
                return True
            return False
        elif action_type == 'send_wechat':
            return True
        elif action_type == 'send_dingtalk':
            return True
        elif action_type == 'send_email':
            return True
        elif action_type == 'run_python_script':
            if run_condition == PER_UPDATE:
                return True
            if run_condition in CRON_CONDITIONS and trigger_condition == CONDITION_PERIODICALLY:
                return True
            return False
        elif action_type == 'link_records':
            if run_condition == PER_UPDATE:
                return True
            if run_condition in CRON_CONDITIONS and trigger_condition == CONDITION_PERIODICALLY:
                return True
            return False
        elif action_type == 'add_record_to_other_table':
            if run_condition == PER_UPDATE:
                return True
            return False
        elif action_type == 'trigger_workflow':
            if run_condition in CRON_CONDITIONS and trigger_condition == CONDITION_PERIODICALLY:
                return True
            return False
        elif action_type in AUTO_RULE_CALCULATE_TYPES:
            if run_condition in CRON_CONDITIONS and trigger_condition == CONDITION_PERIODICALLY:
                return True
            return False
        elif action_type in ['lookup_and_copy', 'extract_user_name']:
            if run_condition in CRON_CONDITIONS and trigger_condition == CONDITION_PERIODICALLY:
                return True
            return False
        elif action_type == 'convert_page_to_pdf':
            if run_condition in CRON_CONDITIONS and trigger_condition == CONDITION_PERIODICALLY_BY_CONDITION:
                return True
            return False
        return False

    def do_actions(self, with_test=False):
        if (not self.can_do_actions()) and (not with_test):
            return

        for action_info in self.action_infos:
            logger.debug('rule: %s start action: %s type: %s', self.rule_id, action_info.get('_id'), action_info['type'])
            if not self.can_condition_trigger_action(action_info):
                logger.debug('rule: %s forbidden trigger action: %s type: %s when run_condition: %s trigger_condition: %s', self.rule_id, action_info.get('_id'), action_info['type'], self.run_condition, self.trigger.get('condition'))
                continue
            if not self.current_valid:
                break
            try:
                if action_info.get('type') == 'update_record':
                    updates = action_info.get('updates')
                    UpdateAction(self, action_info.get('type'), self.data, updates).do_action()

                if action_info.get('type') == 'add_record':
                    row = action_info.get('row')
                    AddRowAction(self, action_info.get('type'), row).do_action()

                elif action_info.get('type') == 'notify':
                    default_msg = action_info.get('default_msg', '')
                    users = action_info.get('users', [])
                    users_column_key = action_info.get('users_column_key', '')
                    NotifyAction(self, action_info.get('type'), self.data, default_msg, users, users_column_key).do_action()

                elif action_info.get('type') == 'lock_record':
                    LockRowAction(self, action_info.get('type'), self.data, self.trigger).do_action()

                elif action_info.get('type') == 'send_wechat':
                    account_id = int(action_info.get('account_id'))
                    default_msg = action_info.get('default_msg', '')
                    msg_type = action_info.get('msg_type', 'text')
                    SendWechatAction(self, action_info.get('type'), self.data, default_msg, account_id, msg_type).do_action()

                elif action_info.get('type') == 'send_dingtalk':
                    account_id = int(action_info.get('account_id'))
                    default_msg = action_info.get('default_msg', '')
                    default_title = action_info.get('default_title', '')
                    msg_type = action_info.get('msg_type', 'text')
                    SendDingtalkAction(self, action_info.get('type'), self.data, default_msg, account_id, msg_type, default_title).do_action()

                elif action_info.get('type') == 'send_email':
                    account_id = int(action_info.get('account_id'))
                    msg = action_info.get('default_msg', '')
                    is_plain_text = action_info.get('is_plain_text', True)
                    html_message = action_info.get('html_message', '')
                    images_info = action_info.get('images_info', {})
                    subject = action_info.get('subject', '')
                    send_to_list = email2list(action_info.get('send_to', ''))
                    copy_to_list = email2list(action_info.get('copy_to', ''))
                    attachment_list = email2list(action_info.get('attachments', ''))
                    repo_id = action_info.get('repo_id')

                    send_info = {
                        'message': msg,
                        'is_plain_text': is_plain_text,
                        'html_message': html_message,
                        'images_info': images_info,
                        'send_to': send_to_list,
                        'copy_to': copy_to_list,
                        'subject': subject,
                        'attachment_list': attachment_list,
                    }
                    SendEmailAction(self, action_info.get('type'), self.data, send_info, account_id, repo_id).do_action()

                elif action_info.get('type') == 'run_python_script':
                    script_name = action_info.get('script_name')
                    workspace_id = action_info.get('workspace_id')
                    owner = action_info.get('owner')
                    org_id = action_info.get('org_id')
                    repo_id = action_info.get('repo_id')
                    RunPythonScriptAction(self, action_info.get('type'), self.data, script_name, workspace_id, owner, org_id, repo_id).do_action()

                elif action_info.get('type') == 'link_records':
                    linked_table_id = action_info.get('linked_table_id')
                    link_id = action_info.get('link_id')
                    match_conditions = action_info.get('match_conditions')
                    LinkRecordsAction(self, action_info.get('type'), self.data, linked_table_id, link_id, match_conditions).do_action()

                elif action_info.get('type') == 'add_record_to_other_table':
                    row = action_info.get('row')
                    dst_table_id = action_info.get('dst_table_id')
                    AddRecordToOtherTableAction(self, action_info.get('type'), self.data, row, dst_table_id).do_action()

                elif action_info.get('type') == 'trigger_workflow':
                    token = action_info.get('token')
                    row = action_info.get('row')
                    TriggerWorkflowAction(self, action_info.get('type'), row, token).do_action()

                elif action_info.get('type') in AUTO_RULE_CALCULATE_TYPES:
                    calculate_column_key = action_info.get('calculate_column')
                    result_column_key = action_info.get('result_column')
                    CalculateAction(self, action_info.get('type'), self.data, calculate_column_key, result_column_key).do_action()

                elif action_info.get('type') == 'lookup_and_copy':
                    table_condition = action_info.get('table_condition')
                    equal_column_conditions = action_info.get('equal_column_conditions')
                    fill_column_conditions = action_info.get('fill_column_conditions')
                    LookupAndCopyAction(self, action_info.get('type'), self.data, table_condition, equal_column_conditions, fill_column_conditions).do_action()

                elif action_info.get('type') == 'extract_user_name':
                    extract_column_key = action_info.get('extract_column_key')
                    result_column_key = action_info.get('result_column_key')
                    ExtractUserNameAction(self, action_info.get('type'), self.data, extract_column_key, result_column_key).do_action()

                elif action_info.get('type') == 'app_notify':
                    default_msg = action_info.get('default_msg', '')
                    users = action_info.get('users', [])
                    users_column_key = action_info.get('users_column_key', '')
                    app_uuid = action_info.get('app_token', None) or action_info.get('app_uuid', None)
                    AppNotifyAction(self, action_info.get('type'), self.data, default_msg, users, users_column_key, app_uuid).do_action()

                elif action_info.get('type') == 'convert_page_to_pdf':
                    page_id = action_info.get('page_id')
                    file_name = action_info.get('file_name')
                    target_column_key = action_info.get('target_column_key')
                    repo_id = action_info.get('repo_id')
                    workspace_id = action_info.get('workspace_id')
                    ConvertPageToPDFAction(self, action_info.get('type'), self.data, page_id, file_name, target_column_key, repo_id, workspace_id).do_action()

            except RuleInvalidException as e:
                logger.warning('auto rule: %s, invalid error: %s', self.rule_id, e)
                self.task_run_success = False
                self.set_invalid()
                break
            except Exception as e:
                logger.exception(e)
                self.task_run_success = False
                logger.error('rule: %s, do action: %s error: %s', self.rule_id, action_info, e)

        if self.done_actions and not with_test:
            self.update_last_trigger_time()

        if not with_test:
            self.add_task_log()

    def set_done_actions(self, done=True):
        self.done_actions = done

    def add_task_log(self):
        if not self.org_id:
            return
        try:
            set_task_log_sql = """
                INSERT INTO auto_rules_task_log (trigger_time, success, rule_id, run_condition, dtable_uuid, org_id, owner, warnings) VALUES
                (:trigger_time, :success, :rule_id, :run_condition, :dtable_uuid, :org_id, :owner, :warnings)
            """
            if self.run_condition in ALL_CONDITIONS:
                self.db_session.execute(set_task_log_sql, {
                    'trigger_time': datetime.utcnow(),
                    'success': self.task_run_success,
                    'rule_id': self.rule_id,
                    'run_condition': self.run_condition,
                    'dtable_uuid': self.dtable_uuid,
                    'org_id': self.org_id,
                    'owner': self.creator,
                    'warnings': json.dumps(self.warnings) if self.warnings else None
                })
                self.db_session.commit()
        except Exception as e:
            logger.error('set rule task log: %s error: %s', self.rule_id, e)

    def update_last_trigger_time(self):
        try:
            set_statistic_sql_user = '''
                INSERT INTO user_auto_rules_statistics (username, trigger_date, trigger_count, update_at) VALUES 
                (:username, :trigger_date, 1, :trigger_time)
                ON DUPLICATE KEY UPDATE
                trigger_count=trigger_count+1,
                update_at=:trigger_time
            '''
            set_statistic_sql_org = '''
                INSERT INTO org_auto_rules_statistics (org_id, trigger_date, trigger_count, update_at) VALUES
                (:org_id, :trigger_date, 1, :trigger_time)
                ON DUPLICATE KEY UPDATE
                trigger_count=trigger_count+1,
                update_at=:trigger_time
            '''
            set_statistic_sql_user_per_month = '''
                INSERT INTO user_auto_rules_statistics_per_month(username, trigger_count, month, updated_at) VALUES
                (:username, 1, :month, :trigger_time)
                ON DUPLICATE KEY UPDATE
                trigger_count=trigger_count+1,
                updated_at=:trigger_time
            '''
            set_statistic_sql_org_per_month = '''
                INSERT INTO org_auto_rules_statistics_per_month(org_id, trigger_count, month, updated_at) VALUES
                (:org_id, 1, :month, :trigger_time)
                ON DUPLICATE KEY UPDATE
                trigger_count=trigger_count+1,
                updated_at=:trigger_time
            '''
            set_last_trigger_time_sql = '''
                UPDATE dtable_automation_rules SET last_trigger_time=:trigger_time, trigger_count=:trigger_count WHERE id=:rule_id;
            '''

            sqls = [set_last_trigger_time_sql]
            if self.org_id:
                if self.org_id == -1:
                    sqls.append(set_statistic_sql_user)
                    sqls.append(set_statistic_sql_user_per_month)
                else:
                    sqls.append(set_statistic_sql_org)
                    sqls.append(set_statistic_sql_org_per_month)

            cur_date = datetime.now().date()
            cur_year, cur_month = cur_date.year, cur_date.month
            trigger_date = date(year=cur_year, month=cur_month, day=1)
            for sql in sqls:
                self.db_session.execute(sql, {
                    'rule_id': self.rule_id,
                    'trigger_time': datetime.utcnow(),
                    'trigger_date': trigger_date,
                    'trigger_count': self.trigger_count + 1,
                    'username': self.creator,
                    'org_id': self.org_id,
                    'month': str(date.today())[:7]
                })
            self.db_session.commit()
        except Exception as e:
            logger.exception('set rule: %s error: %s', self.rule_id, e)

        if self.run_condition == PER_UPDATE and self.per_minute_trigger_limit > 0:
            trigger_times = redis_cache.get(self.cache_key)
            if not trigger_times:
                redis_cache.set(self.cache_key, int(time.time()), timeout=MINUTE_TIMEOUT)
            else:
                trigger_times = trigger_times.split(',')
                trigger_times.append(str(int(time.time())))
                trigger_times = trigger_times[-self.per_minute_trigger_limit:]
                redis_cache.set(self.cache_key, ','.join([t for t in trigger_times]), timeout=MINUTE_TIMEOUT)

    def set_invalid(self):
        try:
            self.current_valid = False
            set_invalid_sql = '''
                UPDATE dtable_automation_rules SET is_valid=0 WHERE id=:rule_id
            '''
            self.db_session.execute(set_invalid_sql, {'rule_id': self.rule_id})
            self.db_session.commit()
        except Exception as e:
            logger.error('set rule: %s invalid error: %s', self.rule_id, e)

