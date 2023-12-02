import json
import jwt
import logging

from flask import Flask, request, make_response

from dtable_events.app.config import DTABLE_PRIVATE_KEY
from dtable_events.dtable_io.task_manager import task_manager
from dtable_events.dtable_io.task_message_manager import message_task_manager
from dtable_events.dtable_io.task_data_sync_manager import data_sync_task_manager
from dtable_events.dtable_io.task_plugin_email_manager import plugin_email_task_manager
from dtable_events.dtable_io.task_big_data_manager import big_data_task_manager
from dtable_events.dtable_io.utils import to_python_boolean

app = Flask(__name__)
logger = logging.getLogger(__name__)


def check_auth_token(req):
    auth = req.headers.get('Authorization', '').split()
    if not auth or auth[0].lower() != 'token' or len(auth) != 2:
        return False, 'Token invalid.'

    token = auth[1]
    if not token:
        return False, 'Token invalid.'

    private_key = DTABLE_PRIVATE_KEY
    try:
        jwt.decode(token, private_key, algorithms=['HS256'])
    except (jwt.ExpiredSignatureError, jwt.InvalidSignatureError) as e:
        return False, e

    return True, None


@app.route('/add-export-task', methods=['GET'])
def add_export_task():
    from dtable_events.utils import parse_bool
    is_valid, error = check_auth_token(request)
    if not is_valid:
        return make_response((error, 403))

    if task_manager.tasks_queue.full():
        from dtable_events.dtable_io import dtable_io_logger
        dtable_io_logger.warning('dtable io server busy, queue size: %d, current tasks: %s, threads is_alive: %s'
                                 % (task_manager.tasks_queue.qsize(), task_manager.current_task_info,
                                    task_manager.threads_is_alive()))
        return make_response(('dtable io server busy.', 400))

    username = request.args.get('username')
    repo_id = request.args.get('repo_id')
    workspace_id = request.args.get('workspace_id')
    table_name = request.args.get('table_name')
    dtable_uuid = request.args.get('dtable_uuid')
    ignore_asset = parse_bool(request.args.get('ignore_asset', default=False))

    try:
        task_id = task_manager.add_export_task(
            username, repo_id, workspace_id, dtable_uuid, table_name, ignore_asset)
    except Exception as e:
        logger.error(e)
        return make_response((e, 500))

    return make_response(({'task_id': task_id}, 200))


@app.route('/add-import-task', methods=['GET'])
def add_import_task():
    from dtable_events.utils import parse_bool
    is_valid, error = check_auth_token(request)
    if not is_valid:
        return make_response((error, 403))

    if task_manager.tasks_queue.full():
        from dtable_events.dtable_io import dtable_io_logger
        dtable_io_logger.warning('dtable io server busy, queue size: %d, current tasks: %s, threads is_alive: %s'
                                 % (task_manager.tasks_queue.qsize(), task_manager.current_task_info,
                                    task_manager.threads_is_alive()))
        return make_response(('dtable io server busy.', 400))

    username = request.args.get('username')
    repo_id = request.args.get('repo_id')
    workspace_id = request.args.get('workspace_id')
    dtable_uuid = request.args.get('dtable_uuid')
    dtable_file_name = request.args.get('dtable_file_name')
    in_storage = parse_bool(request.args.get('in_storage'))
    can_use_automation_rules = parse_bool(request.args.get('can_use_automation_rules'))
    can_use_workflows = parse_bool(request.args.get('can_use_workflows'))
    can_use_external_apps = parse_bool(request.args.get('can_use_external_apps'))
    owner = request.args.get('owner')
    org_id = request.args.get('org_id')

    try:
        task_id = task_manager.add_import_task(
            username, repo_id, workspace_id, dtable_uuid, dtable_file_name, in_storage, can_use_automation_rules,
            can_use_workflows, can_use_external_apps, owner, org_id)
    except Exception as e:
        logger.error(e)
        return make_response((e, 500))

    return make_response(({'task_id': task_id}, 200))

@app.route('/add-big-data-screen-export-task', methods=['GET'])
def add_big_data_screen_export_task():
    is_valid, error = check_auth_token(request)
    if not is_valid:
        return make_response((error, 403))

    if task_manager.tasks_queue.full():
        from dtable_events.dtable_io import dtable_io_logger
        dtable_io_logger.warning('dtable io server busy, queue size: %d, current tasks: %s, threads is_alive: %s'
                                 % (task_manager.tasks_queue.qsize(), task_manager.current_task_info,
                                    task_manager.threads_is_alive()))
        return make_response(('dtable io server busy.', 400))

    username = request.args.get('username')
    repo_id = request.args.get('repo_id')
    dtable_uuid = request.args.get('dtable_uuid')
    page_id = request.args.get('page_id')

    try:
        task_id = task_manager.add_export_dtable_big_data_screen_task(
            username, repo_id, dtable_uuid, page_id)
    except Exception as e:
        logger.error(e)
        return make_response((e, 500))

    return make_response(({'task_id': task_id}, 200))

@app.route('/add-big-data-screen-import-task', methods=['GET'])
def add_big_data_screen_import_task():
    is_valid, error = check_auth_token(request)
    if not is_valid:
        return make_response((error, 403))

    if task_manager.tasks_queue.full():
        from dtable_events.dtable_io import dtable_io_logger
        dtable_io_logger.warning('dtable io server busy, queue size: %d, current tasks: %s, threads is_alive: %s'
                                 % (task_manager.tasks_queue.qsize(), task_manager.current_task_info,
                                    task_manager.threads_is_alive()))
        return make_response(('dtable io server busy.', 400))

    username = request.args.get('username')
    repo_id = request.args.get('repo_id')
    dtable_uuid = request.args.get('dtable_uuid')
    page_id = request.args.get('page_id')

    try:
        task_id = task_manager.add_import_dtable_big_data_screen_task(
            username, repo_id, dtable_uuid, page_id)
    except Exception as e:
        logger.error(e)
        return make_response((e, 500))

    return make_response(({'task_id': task_id}, 200))


@app.route('/add-parse-excel-csv-task', methods=['GET'])
def add_parse_excel_csv_task():
    is_valid, error = check_auth_token(request)
    if not is_valid:
        return make_response((error, 403))

    if task_manager.tasks_queue.full():
        from dtable_events.dtable_io import dtable_io_logger
        dtable_io_logger.warning('dtable io server busy, queue size: %d, current tasks: %s, threads is_alive: %s'
                                 % (task_manager.tasks_queue.qsize(), task_manager.current_task_info,
                                    task_manager.threads_is_alive()))
        return make_response(('dtable io server busy.', 400))

    username = request.args.get('username')
    repo_id = request.args.get('repo_id')
    workspace_id = request.args.get('workspace_id')
    dtable_name = request.args.get('dtable_name')
    custom = request.args.get('custom')
    file_type = request.args.get('file_type')
    custom = bool(int(custom))

    try:
        task_id = task_manager.add_parse_excel_csv_task(
            username, repo_id, workspace_id, dtable_name, file_type, custom)
    except Exception as e:
        logger.error(e)
        return make_response((e, 500))

    return make_response(({'task_id': task_id}, 200))


@app.route('/add-import-excel-csv-task', methods=['GET'])
def add_import_excel_csv_task():
    is_valid, error = check_auth_token(request)
    if not is_valid:
        return make_response((error, 403))

    if task_manager.tasks_queue.full():
        from dtable_events.dtable_io import dtable_io_logger
        dtable_io_logger.warning('dtable io server busy, queue size: %d, current tasks: %s, threads is_alive: %s'
                                 % (task_manager.tasks_queue.qsize(), task_manager.current_task_info,
                                    task_manager.threads_is_alive()))
        return make_response(('dtable io server busy.', 400))

    username = request.args.get('username')
    repo_id = request.args.get('repo_id')
    workspace_id = request.args.get('workspace_id')
    dtable_uuid = request.args.get('dtable_uuid')
    dtable_name = request.args.get('dtable_name')
    lang = request.args.get('lang')

    try:
        task_id = task_manager.add_import_excel_csv_task(
            username, repo_id, workspace_id, dtable_uuid, dtable_name, lang)
    except Exception as e:
        logger.error(e)
        return make_response((e, 500))

    return make_response(({'task_id': task_id}, 200))


@app.route('/add-import-excel-csv-add-table-task', methods=['GET'])
def add_import_excel_csv_add_table_task():
    is_valid, error = check_auth_token(request)
    if not is_valid:
        return make_response((error, 403))

    if task_manager.tasks_queue.full():
        from dtable_events.dtable_io import dtable_io_logger
        dtable_io_logger.warning('dtable io server busy, queue size: %d, current tasks: %s, threads is_alive: %s'
                                 % (task_manager.tasks_queue.qsize(), task_manager.current_task_info,
                                    task_manager.threads_is_alive()))
        return make_response(('dtable io server busy.', 400))

    username = request.args.get('username')
    repo_id = request.args.get('repo_id')
    workspace_id = request.args.get('workspace_id')
    dtable_uuid = request.args.get('dtable_uuid')
    dtable_name = request.args.get('dtable_name')
    lang = request.args.get('lang')

    try:
        task_id = task_manager.add_import_excel_csv_add_table_task(
            username, repo_id, workspace_id, dtable_uuid, dtable_name, lang)
    except Exception as e:
        logger.error(e)
        return make_response((e, 500))

    return make_response(({'task_id': task_id}, 200))


@app.route('/add-append-excel-csv-append-parsed-file-task', methods=['GET'])
def add_append_excel_csv_append_parsed_file_task():
    is_valid, error = check_auth_token(request)
    if not is_valid:
        return make_response((error, 403))

    if task_manager.tasks_queue.full():
        from dtable_events.dtable_io import dtable_io_logger
        dtable_io_logger.warning('dtable io server busy, queue size: %d, current tasks: %s, threads is_alive: %s'
                                 % (task_manager.tasks_queue.qsize(), task_manager.current_task_info,
                                    task_manager.threads_is_alive()))
        return make_response(('dtable io server busy.', 400))

    username = request.args.get('username')
    repo_id = request.args.get('repo_id')
    dtable_uuid = request.args.get('dtable_uuid')
    file_name = request.args.get('file_name')
    table_name = request.args.get('table_name')

    try:
        task_id = task_manager.add_append_excel_csv_append_parsed_file_task(
            username, repo_id, dtable_uuid, file_name, table_name)
    except Exception as e:
        logger.error(e)
        return make_response((e, 500))

    return make_response(({'task_id': task_id}, 200))


@app.route('/add-append-excel-csv-upload-file-task', methods=['GET'])
def add_append_excel_csv_upload_file_task():
    is_valid, error = check_auth_token(request)
    if not is_valid:
        return make_response((error, 403))

    if task_manager.tasks_queue.full():
        from dtable_events.dtable_io import dtable_io_logger
        dtable_io_logger.warning('dtable io server busy, queue size: %d, current tasks: %s, threads is_alive: %s'
                                 % (task_manager.tasks_queue.qsize(), task_manager.current_task_info,
                                    task_manager.threads_is_alive()))
        return make_response(('dtable io server busy.', 400))

    username = request.args.get('username')
    repo_id = request.args.get('repo_id')
    file_name = request.args.get('file_name')
    dtable_uuid = request.args.get('dtable_uuid')
    table_name = request.args.get('table_name')
    file_type = request.args.get('file_type')

    try:
        task_id = task_manager.add_append_excel_csv_upload_file_task(
            username, repo_id, file_name, dtable_uuid, table_name, file_type)
    except Exception as e:
        logger.error(e)
        return make_response((e, 500))

    return make_response(({'task_id': task_id}, 200))


@app.route('/query-status', methods=['GET'])
def query_status():
    is_valid, error = check_auth_token(request)
    if not is_valid:
        return make_response((error, 403))

    task_id = request.args.get('task_id')
    if not task_manager.is_valid_task_id(task_id):
        return make_response(('task_id not found.', 404))

    try:
        is_finished, error = task_manager.query_status(task_id)
    except Exception as e:
        logger.debug(e)
        return make_response((e, 500))

    if error:
        return make_response((error, 500))

    return make_response(({'is_finished': is_finished}, 200))


@app.route('/cancel-task', methods=['GET'])
def cancel_status():
    is_valid, error = check_auth_token(request)
    if not is_valid:
        return make_response((error, 403))

    task_id = request.args.get('task_id')
    if not task_manager.is_valid_task_id(task_id):
        return make_response(('task_id invalid.', 400))

    try:
        task_manager.cancel_task(task_id)
    except Exception as e:
        logger.error(e)
        return make_response((e, 500))

    return make_response(({'success': True}, 200))


@app.route('/query-message-send-status', methods=['GET'])
def query_message_send_status():
    is_valid, error = check_auth_token(request)
    if not is_valid:
        return make_response((error, 403))

    task_id = request.args.get('task_id')
    if not message_task_manager.is_valid_task_id(task_id):
        return make_response(('task_id invalid.', 400))

    try:
        is_finished, result = message_task_manager.query_status(task_id)
    except Exception as e:
        logger.debug(e)  # task_id not found
        return make_response((e, 500))

    resp = dict(is_finished=is_finished)
    resp['result'] = result if result else {}
    return make_response((resp, 200))


@app.route('/cancel-message-send-task', methods=['GET'])
def cancel_message_send_status():
    is_valid, error = check_auth_token(request)
    if not is_valid:
        return make_response((error, 403))

    task_id = request.args.get('task_id')
    if not message_task_manager.is_valid_task_id(task_id):
        return make_response(('task_id invalid.', 400))

    try:
        message_task_manager.cancel_task(task_id)
    except Exception as e:
        logger.error(e)
        return make_response((e, 500))

    return make_response(({'success': True}, 200))


@app.route('/convert-page-to-pdf', methods=['GET'])
def convert_page_to_pdf():
    is_valid, error = check_auth_token(request)
    if not is_valid:
        return make_response((error, 403))

    if task_manager.tasks_queue.full():
        from dtable_events.dtable_io import dtable_io_logger
        dtable_io_logger.warning('dtable io server busy, queue size: %d, current tasks: %s, threads is_alive: %s'
                                 % (task_manager.tasks_queue.qsize(), task_manager.current_task_info,
                                    task_manager.threads_is_alive()))
        return make_response(('dtable io server busy.', 400))

    dtable_uuid = request.args.get('dtable_uuid')
    page_id = request.args.get('page_id')
    row_id = request.args.get('row_id')

    try:
        task_id = task_manager.convert_page_to_pdf(
            dtable_uuid, page_id, row_id)
    except Exception as e:
        logger.error(e)
        return make_response((e, 500))

    return make_response(({'task_id': task_id}, 200))


@app.route('/dtable-asset-files', methods=['POST'])
def dtable_asset_files():
    is_valid, error = check_auth_token(request)
    if not is_valid:
        return make_response((error, 403))

    if task_manager.tasks_queue.full():
        from dtable_events.dtable_io import dtable_io_logger
        dtable_io_logger.warning('dtable io server busy, queue size: %d, current tasks: %s, threads is_alive: %s'
                                 % (task_manager.tasks_queue.qsize(), task_manager.current_task_info,
                                    task_manager.threads_is_alive()))
        return make_response(('dtable io server busy.', 400))

    data = request.form
    if not isinstance(data, dict):
        return make_response(('Bad request', 400))

    username = data.get('username')
    repo_id = data.get('repo_id')
    dtable_uuid = data.get('dtable_uuid')
    files = data.getlist('files')
    files_map = data.get('files_map')

    if not isinstance(files, list):
        files = [files]
    if not isinstance(files_map, dict):
        files_map = json.loads(files_map)

    try:
        task_id = task_manager.add_export_dtable_asset_files_task(
            username, repo_id, dtable_uuid, files, files_map)
    except Exception as e:
        logger.error(e)
        return make_response((e, 500))

    return make_response(({'task_id': task_id}, 200))


@app.route('/transfer-dtable-asset-files', methods=['POST'])
def transfer_dtable_asset_files():
    is_valid, error = check_auth_token(request)
    if not is_valid:
        return make_response((error, 403))

    if task_manager.tasks_queue.full():
        from dtable_events.dtable_io import dtable_io_logger
        dtable_io_logger.warning('dtable io server busy, queue size: %d, current tasks: %s, threads is_alive: %s'
                                 % (task_manager.tasks_queue.qsize(), task_manager.current_task_info,
                                    task_manager.threads_is_alive()))
        return make_response(('dtable io server busy.', 400))

    data = request.form
    if not isinstance(data, dict):
        return make_response(('Bad request', 400))

    username = data.get('username')
    repo_id = data.get('repo_id')
    dtable_uuid = data.get('dtable_uuid')
    files = data.getlist('files')
    files_map = data.get('files_map')
    repo_api_token = data.get('repo_api_token')
    seafile_server_url = data.get('seafile_server_url')
    parent_dir = data.get('parent_dir')
    relative_path = data.get('relative_path')
    replace = data.get('replace')

    if not isinstance(files, list):
        files = [files]
    if not isinstance(files_map, dict):
        files_map = json.loads(files_map)

    try:
        task_id = task_manager.add_transfer_dtable_asset_files_task(
            username, repo_id, dtable_uuid, files, files_map, parent_dir,
            relative_path, replace, repo_api_token, seafile_server_url)
    except Exception as e:
        logger.error(e)
        return make_response((e, 500))

    return make_response(({'task_id': task_id}, 200))


@app.route('/add-wechat-sending-task', methods=['POST'])
def add_wechat_sending_task():
    is_valid, error = check_auth_token(request)
    if not is_valid:
        return make_response((error, 403))

    if message_task_manager.tasks_queue.full():
        return make_response(('dtable io server busy.', 400))

    data = request.form
    if not isinstance(data, dict):
        return make_response(('Bad request', 400))

    webhook_url = data.get('webhook_url')
    msg = data.get('msg')
    msg_type = data.get('msg_type')

    try:
        task_id = message_task_manager.add_wechat_sending_task(webhook_url, msg, msg_type)
    except Exception as e:
        logger.error(e)
        return make_response((e, 500))

    return make_response(({'task_id': task_id}, 200))


@app.route('/add-dingtalk-sending-task', methods=['POST'])
def add_dingtalk_sending_task():
    is_valid, error = check_auth_token(request)
    if not is_valid:
        return make_response((error, 403))

    if message_task_manager.tasks_queue.full():
        return make_response(('dtable io server busy.', 400))

    data = request.form
    if not isinstance(data, dict):
        return make_response(('Bad request', 400))

    webhook_url = data.get('webhook_url')
    msg = data.get('msg')

    try:
        task_id = message_task_manager.add_dingtalk_sending_task(webhook_url, msg)
    except Exception as e:
        logger.error(e)
        return make_response((e, 500))

    return make_response(({'task_id': task_id}, 200))

@app.route('/add-email-sending-task', methods=['POST'])
def add_email_sending_task():
    is_valid, error = check_auth_token(request)
    if not is_valid:
        return make_response((error, 403))

    if message_task_manager.tasks_queue.full():
        return make_response(('dtable io server busy.', 400))

    data = request.form
    if not isinstance(data, dict):
        return make_response(('Bad request', 400))

    username = data.get('username')
    send_to = data.getlist('send_to')
    copy_to = data.getlist('copy_to')

    if not isinstance(send_to, list):
        send_to = [send_to]
    if copy_to and not isinstance(copy_to, list):
        copy_to = [copy_to]

    file_download_urls = data.get('file_download_urls', )
    if file_download_urls and not isinstance(file_download_urls, dict):
        file_download_urls = json.loads(file_download_urls)

    image_cid_url_map = data.get('image_cid_url_map', {})
    if image_cid_url_map and not isinstance(image_cid_url_map, dict):
        image_cid_url_map = json.loads(image_cid_url_map)

    auth_info = {
        'email_host': data.get('email_host'),
        'email_port': data.get('email_port'),
        'host_user': data.get('host_user'),
        'password': data.get('password'),
        'sender_name': data.get('sender_name')
    }

    send_info = {
        'message': data.get('message'),
        'html_message': data.get('html_message'),
        'send_to': send_to,
        'subject': data.get('subject'),
        'source': data.get('source'),
        'copy_to': copy_to,
        'reply_to': data.get('reply_to'),
        'file_download_urls': file_download_urls,
        'message_id': data.get('message_id'),
        'in_reply_to': data.get('in_reply_to'),
        'image_cid_url_map': image_cid_url_map,
    }

    try:
        task_id = message_task_manager.add_email_sending_task(
            auth_info, send_info, username)
    except Exception as e:
        logger.error(e)
        return make_response((e, 500))

    return make_response(({'task_id': task_id}, 200))

@app.route('/add-notification-sending-task', methods=['POST'])
def add_notification_sending_task():
    is_valid, error = check_auth_token(request)
    if not is_valid:
        return make_response((error, 403))

    if message_task_manager.tasks_queue.full():
        return make_response(('dtable io server busy.', 400))

    data = request.form
    if not isinstance(data, dict):
        return make_response(('Bad request', 400))

    emails = data.get('emails')
    email_list = emails and emails.split(',') or []
    msg = data.get('msg')
    table_id = data.get('table_id')
    row_id = data.get('row_id')
    username = data.get('username')
    dtable_uuid = data.get('dtable_uuid')
    user_col_key = data.get('user_col_key')

    try:
        task_id = message_task_manager.add_notification_sending_task(
            email_list, user_col_key, msg, dtable_uuid, username, table_id, row_id
        )
    except Exception as e:
        logger.error(e)
        return make_response((e, 500))

    return make_response(({'task_id': task_id}, 200))


@app.route('/add-run-auto-rule-task', methods=['POST'])
def add_run_auto_rule_task():
    is_valid, error = check_auth_token(request)
    if not is_valid:
        return make_response((error, 403))

    if message_task_manager.tasks_queue.full():
        return make_response(('dtable io server busy.', 400))

    data = request.form
    if not isinstance(data, dict):
        return make_response(('Bad request', 400))

    creator = data.get('creator')
    owner = data.get('owner')
    org_id = data.get('org_id')
    run_condition = data.get('run_condition')
    trigger = data.get('trigger')
    dtable_uuid = data.get('dtable_uuid')
    actions = data.get('actions')
    automation_rule_id = data.get('automation_rule_id')

    try:
        task_id = task_manager.add_run_auto_rule_task(
            automation_rule_id, creator, owner, org_id, dtable_uuid, run_condition, trigger, actions)
    except Exception as e:
        logger.error(e)
        return make_response((e, 500))

    return make_response(({'task_id': task_id}, 200))


@app.route('/add-update-excel-upload-excel-task', methods=['GET'])
def add_update_excel_upload_excel_task():
    is_valid, error = check_auth_token(request)
    if not is_valid:
        return make_response((error, 403))

    if task_manager.tasks_queue.full():
        from dtable_events.dtable_io import dtable_io_logger
        dtable_io_logger.warning('dtable io server busy, queue size: %d, current tasks: %s, threads is_alive: %s'
                                 % (task_manager.tasks_queue.qsize(), task_manager.current_task_info,
                                    task_manager.threads_is_alive()))
        return make_response(('dtable io server busy.', 400))

    username = request.args.get('username')
    repo_id = request.args.get('repo_id')
    file_name = request.args.get('file_name')
    dtable_uuid = request.args.get('dtable_uuid')
    table_name = request.args.get('table_name')

    try:
        task_id = task_manager.add_update_excel_upload_excel_task(
            username, repo_id, file_name, dtable_uuid, table_name, )
    except Exception as e:
        logger.error(e)
        return make_response((e, 500))

    return make_response(({'task_id': task_id}, 200))


@app.route('/add-update-excel-csv-update-parsed-file-task', methods=['GET'])
def add_update_excel_csv_update_parsed_file_task():
    is_valid, error = check_auth_token(request)
    if not is_valid:
        return make_response((error, 403))

    if task_manager.tasks_queue.full():
        from dtable_events.dtable_io import dtable_io_logger
        dtable_io_logger.warning('dtable io server busy, queue size: %d, current tasks: %s, threads is_alive: %s'
                                 % (task_manager.tasks_queue.qsize(), task_manager.current_task_info,
                                    task_manager.threads_is_alive()))
        return make_response(('dtable io server busy.', 400))

    username = request.args.get('username')
    repo_id = request.args.get('repo_id')
    dtable_uuid = request.args.get('dtable_uuid')
    file_name = request.args.get('file_name')
    table_name = request.args.get('table_name')
    selected_columns = request.args.get('selected_columns')

    try:
        task_id = task_manager.add_update_excel_csv_update_parsed_file_task(
            username, repo_id, dtable_uuid, file_name, table_name, selected_columns)
    except Exception as e:
        logger.error(e)
        return make_response((e, 500))

    return make_response(({'task_id': task_id}, 200))


@app.route('/add-update-csv-upload-csv-task', methods=['GET'])
def add_update_csv_upload_csv_task():
    is_valid, error = check_auth_token(request)
    if not is_valid:
        return make_response((error, 403))

    if task_manager.tasks_queue.full():
        from dtable_events.dtable_io import dtable_io_logger
        dtable_io_logger.warning('dtable io server busy, queue size: %d, current tasks: %s, threads is_alive: %s'
                                 % (task_manager.tasks_queue.qsize(), task_manager.current_task_info,
                                    task_manager.threads_is_alive()))
        return make_response(('dtable io server busy.', 400))

    username = request.args.get('username')
    repo_id = request.args.get('repo_id')
    dtable_uuid = request.args.get('dtable_uuid')
    file_name = request.args.get('file_name')
    table_name = request.args.get('table_name')

    try:
        task_id = task_manager.add_update_csv_upload_csv_task(
            username, repo_id, file_name, dtable_uuid, table_name)
    except Exception as e:
        logger.error(e)
        return make_response((e, 500))

    return make_response(({'task_id': task_id}, 200))


@app.route('/add-import-excel-csv-to-dtable-task', methods=['GET'])
def add_import_excel_csv_to_dtable_task():
    is_valid, error = check_auth_token(request)
    if not is_valid:
        return make_response((error, 403))

    if task_manager.tasks_queue.full():
        from dtable_events.dtable_io import dtable_io_logger
        dtable_io_logger.warning('dtable io server busy, queue size: %d, current tasks: %s, threads is_alive: %s'
                                 % (task_manager.tasks_queue.qsize(), task_manager.current_task_info,
                                    task_manager.threads_is_alive()))
        return make_response(('dtable io server busy.', 400))

    username = request.args.get('username')
    repo_id = request.args.get('repo_id')
    workspace_id = request.args.get('workspace_id')
    dtable_name = request.args.get('dtable_name')
    dtable_uuid = request.args.get('dtable_uuid')
    file_type = request.args.get('file_type')
    lang = request.args.get('lang')

    try:
        task_id = task_manager.add_import_excel_csv_to_dtable_task(username, repo_id, workspace_id, dtable_name, dtable_uuid, file_type, lang)
    except Exception as e:
        logger.error(e)
        return make_response((e, 500))

    return make_response(({'task_id': task_id}, 200))


@app.route('/add-import-excel-csv-to-table-task', methods=['GET'])
def add_import_excel_to_table_task():
    is_valid, error = check_auth_token(request)
    if not is_valid:
        return make_response((error, 403))

    if task_manager.tasks_queue.full():
        from dtable_events.dtable_io import dtable_io_logger
        dtable_io_logger.warning('dtable io server busy, queue size: %d, current tasks: %s, threads is_alive: %s'
                                 % (task_manager.tasks_queue.qsize(), task_manager.current_task_info,
                                    task_manager.threads_is_alive()))
        return make_response(('dtable io server busy.', 400))

    username = request.args.get('username')
    repo_id = request.args.get('repo_id')
    workspace_id = request.args.get('workspace_id')
    file_name = request.args.get('file_name')
    dtable_uuid = request.args.get('dtable_uuid')
    file_type = request.args.get('file_type')
    lang = request.args.get('lang')

    try:
        task_id = task_manager.add_import_excel_csv_to_table_task(username, repo_id, workspace_id, file_name, dtable_uuid, file_type, lang)
    except Exception as e:
        logger.error(e)
        return make_response((e, 500))

    return make_response(({'task_id': task_id}, 200))


@app.route('/add-update-table-via-excel-csv-task', methods=['GET'])
def add_update_table_via_excel_csv_task():
    is_valid, error = check_auth_token(request)
    if not is_valid:
        return make_response((error, 403))

    if task_manager.tasks_queue.full():
        from dtable_events.dtable_io import dtable_io_logger
        dtable_io_logger.warning('dtable io server busy, queue size: %d, current tasks: %s, threads is_alive: %s'
                                 % (task_manager.tasks_queue.qsize(), task_manager.current_task_info,
                                    task_manager.threads_is_alive()))
        return make_response(('dtable io server busy.', 400))

    username = request.args.get('username')
    repo_id = request.args.get('repo_id')
    file_name = request.args.get('file_name')
    dtable_uuid = request.args.get('dtable_uuid')
    table_name = request.args.get('table_name')
    selected_columns = request.args.get('selected_columns')
    file_type = request.args.get('file_type')

    try:
        task_id = task_manager.add_update_table_via_excel_csv_task(
            username, repo_id, file_name, dtable_uuid, table_name, selected_columns, file_type)
    except Exception as e:
        logger.error(e)
        return make_response((e, 500))

    return make_response(({'task_id': task_id}, 200))


@app.route('/add-append-excel-csv-to-table-task', methods=['GET'])
def add_append_excel_csv_to_table_task():
    is_valid, error = check_auth_token(request)
    if not is_valid:
        return make_response((error, 403))

    if task_manager.tasks_queue.full():
        from dtable_events.dtable_io import dtable_io_logger
        dtable_io_logger.warning('dtable io server busy, queue size: %d, current tasks: %s, threads is_alive: %s'
                                 % (task_manager.tasks_queue.qsize(), task_manager.current_task_info,
                                    task_manager.threads_is_alive()))
        return make_response(('dtable io server busy.', 400))

    username = request.args.get('username')
    repo_id = request.args.get('repo_id')
    file_name = request.args.get('file_name')
    dtable_uuid = request.args.get('dtable_uuid')
    table_name = request.args.get('table_name')
    file_type = request.args.get('file_type')

    try:
        task_id = task_manager.add_append_excel_csv_to_table_task(
            username, repo_id, file_name, dtable_uuid, table_name, file_type)
    except Exception as e:
        logger.error(e)
        return make_response((e, 500))

    return make_response(({'task_id': task_id}, 200))


@app.route('/import-table-from-base', methods=['POST'])
def import_table_from_base():
    is_valid, error = check_auth_token(request)
    if not is_valid:
        return make_response((error, 403))

    if task_manager.tasks_queue.full():
        from dtable_events.dtable_io import dtable_io_logger
        dtable_io_logger.warning('dtable io server busy, queue size: %d, current tasks: %s, threads is_alive: %s'
                                 % (task_manager.tasks_queue.qsize(), task_manager.current_task_info,
                                    task_manager.threads_is_alive()))
        return make_response(('dtable io server busy.', 400))

    try:
        context = json.loads(request.data)
    except Exception as e:
        return make_response(('context invalid, error: %s' % e, 400))

    try:
        task_id = task_manager.add_import_table_from_base_task(context)
    except Exception as e:
        logger.error(e)
        return make_response((e, 500))

    return make_response(({'task_id': task_id}, 200))


@app.route('/import-common-dataset', methods=['POST'])
def import_common_dataset():
    is_valid, error = check_auth_token(request)
    if not is_valid:
        return make_response((error, 403))
    if task_manager.tasks_queue.full():
        from dtable_events.dtable_io import dtable_io_logger
        dtable_io_logger.warning('dtable io server busy, queue size: %d, current tasks: %s, threads is_alive: %s'
                                 % (task_manager.tasks_queue.qsize(), task_manager.current_task_info,
                                    task_manager.threads_is_alive()))
        return make_response(('dtable io server busy.', 400))
    try:
        context = json.loads(request.data)
    except:
        return make_response(('import common dataset context invalid.', 400))

    try:
        task_id = task_manager.add_import_common_dataset_task(context)
    except Exception as e:
        logger.error(e)
        return make_response((e, 500))

    return make_response(({'task_id': task_id}, 200))


@app.route('/sync-common-dataset', methods=['POST'])
def sync_common_data():
    is_valid, error = check_auth_token(request)
    if not is_valid:
        return make_response((error, 403))
    if task_manager.tasks_queue.full():
        from dtable_events.dtable_io import dtable_io_logger
        dtable_io_logger.warning('dtable io server busy, queue size: %d, current tasks: %s, threads is_alive: %s'
                                 % (task_manager.tasks_queue.qsize(), task_manager.current_task_info,
                                    task_manager.threads_is_alive()))
        return make_response(('dtable io server busy.', 400))
    try:
        context = json.loads(request.data)
    except:
        return make_response(('sync common dataset context invalid.', 400))

    try:
        task_id, error_type = task_manager.add_sync_common_dataset_task(context)
        if error_type == 'syncing':
            return make_response({'error_msg': 'Dataset is syncing'}, 429)
    except Exception as e:
        logger.error(e)
        return make_response((e, 500))

    return make_response(({'task_id': task_id}, 200))


@app.route('/convert-view-to-excel', methods=['POST'])
def convert_view_to_excel():
    is_valid, error = check_auth_token(request)
    if not is_valid:
        return make_response((error, 403))

    if task_manager.tasks_queue.full():
        from dtable_events.dtable_io import dtable_io_logger
        dtable_io_logger.warning('dtable io server busy, queue size: %d, current tasks: %s, threads is_alive: %s'
                                 % (task_manager.tasks_queue.qsize(), task_manager.current_task_info,
                                    task_manager.threads_is_alive()))
        return make_response(('dtable io server busy.', 400))

    try:
        context = json.loads(request.data)
    except:
        return make_response(('convert view to excel.', 400))

    dtable_uuid = context.get('dtable_uuid')
    table_id = context.get('table_id')
    view_id = context.get('view_id')
    username = context.get('username')
    id_in_org = context.get('id_in_org')
    user_department_ids_map = context.get('user_department_ids_map')
    permission = context.get('permission')
    name = context.get('name')
    repo_id = context.get('repo_id')
    is_support_image = to_python_boolean(context.get('is_support_image', 'false'))

    try:
        task_id = task_manager.add_convert_view_to_execl_task(dtable_uuid, table_id, view_id, username, id_in_org, user_department_ids_map, permission, name, repo_id, is_support_image)
    except Exception as e:
        logger.error(e)
        return make_response((e, 500))

    return make_response(({'task_id': task_id}, 200))


@app.route('/convert-table-to-excel', methods=['GET'])
def convert_table_to_excel():
    is_valid, error = check_auth_token(request)
    if not is_valid:
        return make_response((error, 403))

    if task_manager.tasks_queue.full():
        from dtable_events.dtable_io import dtable_io_logger
        dtable_io_logger.warning('dtable io server busy, queue size: %d, current tasks: %s, threads is_alive: %s'
                                 % (task_manager.tasks_queue.qsize(), task_manager.current_task_info,
                                    task_manager.threads_is_alive()))
        return make_response(('dtable io server busy.', 400))

    dtable_uuid = request.args.get('dtable_uuid')
    table_id = request.args.get('table_id')
    username = request.args.get('username')
    permission = request.args.get('permission')
    name = request.args.get('name')
    repo_id = request.args.get('repo_id')
    is_support_image = to_python_boolean(request.args.get('is_support_image', 'false'))

    try:
        task_id = task_manager.add_convert_table_to_execl_task(dtable_uuid, table_id, username, permission, name, repo_id, is_support_image)
    except Exception as e:
        logger.error(e)
        return make_response((e, 500))

    return make_response(({'task_id': task_id}, 200))


@app.route('/add-app-users-sync-task', methods=['POST'])
def add_app_users_sync_task():
    is_valid, error = check_auth_token(request)
    if not is_valid:
        return make_response((error, 403))

    if task_manager.tasks_queue.full():
        return make_response(('dtable io server busy.', 400))

    data = request.form
    if not isinstance(data, dict):
        return make_response(('Bad request', 400))

    username = data.get('username')
    table_name = data.get('table_name')
    dtable_uuid = data.get('dtable_uuid')
    app_name = data.get('app_name')
    table_id = data.get('table_id')
    app_id = data.get('app_id')

    try:
        task_id = task_manager.add_app_users_sync_task(dtable_uuid, app_name, app_id, table_name, table_id, username)
    except Exception as e:
        logger.error(e)
        return make_response((e, 500))

    return make_response(({'task_id': task_id}, 200))


@app.route('/add-sync-email-task', methods=['POST'])
def sync_email():
    is_valid, error = check_auth_token(request)
    if not is_valid:
        return make_response((error, 403))
    if data_sync_task_manager.tasks_queue.full():
        return make_response(('dtable io server busy.', 400))
    try:
        context = json.loads(request.data)
    except:
        return make_response(('sync email context invalid.', 400))

    try:
        task_id = data_sync_task_manager.add_sync_email_task(context)
    except Exception as e:
        logger.error(e)
        return make_response((e, 500))

    return make_response(({'task_id': task_id}, 200))


@app.route('/add-convert-big-data-view-to-excel-task', methods=['POST'])
def convert_big_data_view_to_excel():
    is_valid, error = check_auth_token(request)
    if not is_valid:
        return make_response((error, 403))

    if big_data_task_manager.tasks_queue.full():
        from dtable_events.dtable_io import dtable_io_logger
        dtable_io_logger.warning('dtable io server busy, queue size: %d, current tasks: %s, threads is_alive: %s'
                                 % (big_data_task_manager.tasks_queue.qsize(), big_data_task_manager.current_task_info,
                                    big_data_task_manager.threads_is_alive()))
        return make_response(('dtable io server busy.', 400))

    data = request.form
    dtable_uuid = data.get('dtable_uuid')
    table_id = data.get('table_id')
    view_id = data.get('view_id')
    username = data.get('username')
    name = data.get('name')
    repo_id = data.get('repo_id')
    is_support_image = to_python_boolean(data.get('is_support_image', 'false'))

    try:
        task_id = big_data_task_manager.add_convert_big_data_view_to_execl_task(dtable_uuid, table_id, view_id, username, name, repo_id, is_support_image)
    except Exception as e:
        logger.error(e)
        return make_response((e, 500))

    return make_response(({'task_id': task_id}, 200))


@app.route('/add-import-big-excel-task', methods=['POST'])
def add_import_big_excel_task():
    is_valid, error = check_auth_token(request)
    if not is_valid:
        return make_response((error, 403))

    if big_data_task_manager.tasks_queue.full():
        from dtable_events.dtable_io import dtable_io_logger
        dtable_io_logger.warning('dtable io server busy, queue size: %d, current tasks: %s, threads is_alive: %s'
                                 % (big_data_task_manager.tasks_queue.qsize(), big_data_task_manager.current_task_info,
                                    big_data_task_manager.threads_is_alive()))
        return make_response(('dtable io server busy.', 400))

    data = request.form

    username = data.get('username')
    dtable_uuid = data.get('dtable_uuid')
    file_path = data.get('file_path')
    table_name = data.get('table_name')
    try:
        task_id = big_data_task_manager.add_import_big_excel_task(
            username, dtable_uuid, table_name, file_path)
    except Exception as e:
        logger.error(e)
        return make_response((e, 500))
    return make_response(({'task_id': task_id}, 200))

@app.route('/add-update-big-excel-task', methods=['POST'])
def add_update_big_excel_task():
    is_valid, error = check_auth_token(request)
    if not is_valid:
        return make_response((error, 403))

    if big_data_task_manager.tasks_queue.full():
        from dtable_events.dtable_io import dtable_io_logger
        dtable_io_logger.warning('dtable io server busy, queue size: %d, current tasks: %s, threads is_alive: %s'
                                 % (big_data_task_manager.tasks_queue.qsize(), big_data_task_manager.current_task_info,
                                    big_data_task_manager.threads_is_alive()))
        return make_response(('dtable io server busy.', 400))

    data = request.form

    username = data.get('username')
    dtable_uuid = data.get('dtable_uuid')
    file_path = data.get('file_path')
    table_name = data.get('table_name')
    ref_columns = data.get('ref_columns')
    is_insert_new_data = data.get('is_insert_new_data', 'false')
    is_insert_new_data = to_python_boolean(is_insert_new_data)
    try:
        task_id = big_data_task_manager.add_update_big_excel_task(
            username, dtable_uuid, table_name, file_path, ref_columns, is_insert_new_data)
    except Exception as e:
        logger.error(e)
        return make_response((e, 500))
    return make_response(({'task_id': task_id}, 200))


@app.route('/add-export-page-design-task', methods=['GET'])
def add_export_page_design_task():
    is_valid, error = check_auth_token(request)
    if not is_valid:
        return make_response((error, 403))

    if task_manager.tasks_queue.full():
        from dtable_events.dtable_io import dtable_io_logger
        dtable_io_logger.warning('dtable io server busy, queue size: %d, current tasks: %s, threads is_alive: %s'
                                 % (task_manager.tasks_queue.qsize(), task_manager.current_task_info,
                                    task_manager.threads_is_alive()))
        return make_response(('dtable io server busy.', 400))

    username = request.args.get('username')
    repo_id = request.args.get('repo_id')
    dtable_uuid = request.args.get('dtable_uuid')
    page_id = request.args.get('page_id')

    try:
        task_id = task_manager.add_export_page_design_task(
            repo_id, dtable_uuid, page_id, username)
    except Exception as e:
        logger.error(e)
        return make_response((e, 500))

    return make_response(({'task_id': task_id}, 200))


@app.route('/add-import-page-design-task', methods=['GET'])
def add_import_page_design_task():
    is_valid, error = check_auth_token(request)
    if not is_valid:
        return make_response((error, 403))

    if task_manager.tasks_queue.full():
        from dtable_events.dtable_io import dtable_io_logger
        dtable_io_logger.warning('dtable io server busy, queue size: %d, current tasks: %s, threads is_alive: %s'
                                 % (task_manager.tasks_queue.qsize(), task_manager.current_task_info,
                                    task_manager.threads_is_alive()))
        return make_response(('dtable io server busy.', 400))

    username = request.args.get('username')
    repo_id = request.args.get('repo_id')
    dtable_uuid = request.args.get('dtable_uuid')
    page_id = request.args.get('page_id')
    workspace_id = request.args.get('workspace_id')
    is_dir = to_python_boolean(request.args.get('is_dir'))

    try:
        task_id = task_manager.add_import_page_design_task(
            repo_id, workspace_id, dtable_uuid, page_id, is_dir, username)
    except Exception as e:
        logger.error(e)
        return make_response((e, 500))

    return make_response(({'task_id': task_id}, 200))


@app.route('/query-big-data-status', methods=['GET'])
def query_big_data_status():
    is_valid, error = check_auth_token(request)
    if not is_valid:
        return make_response((error, 403))

    task_id = request.args.get('task_id')
    if not big_data_task_manager.is_valid_task_id(task_id):
        return make_response(('task_id invalid.', 400))

    try:
        is_finished, result = big_data_task_manager.query_status(task_id)
    except Exception as e:
        logger.debug(e)  # task_id not found
        return make_response((e, 500))

    resp = dict(is_finished=is_finished)
    resp['result'] = result if result else {}
    return make_response((resp, 200))


@app.route('/query-data-sync-status', methods=['GET'])
def query_data_sync_status():
    is_valid, error = check_auth_token(request)
    if not is_valid:
        return make_response((error, 403))

    task_id = request.args.get('task_id')
    if not data_sync_task_manager.is_valid_task_id(task_id):
        return make_response(('task_id invalid.', 400))

    try:
        is_finished = data_sync_task_manager.query_status(task_id)
    except Exception as e:
        logger.debug(e)
        return make_response((e, 500))

    resp = dict(is_finished=is_finished)
    return make_response((resp, 200))


@app.route('/plugin-email-send-email', methods=['POST'])
def add_email_to_table():
    is_valid, error = check_auth_token(request)
    if not is_valid:
        return make_response((error, 403))
    if plugin_email_task_manager.tasks_queue.full():
        return make_response(('dtable io server busy.', 400))
    try:
        context = json.loads(request.data)
    except:
        return make_response(('add email context invalid.', 400))

    try:
        task_id = plugin_email_task_manager.add_send_email_task(context)
    except Exception as e:
        logger.error(e)
        return make_response((e, 500))

    return make_response(({'task_id': task_id}, 200))


@app.route('/query-plugin-email-send-status', methods=['GET'])
def query_plugin_email_send_status():
    is_valid, error = check_auth_token(request)
    if not is_valid:
        return make_response((error, 403))

    task_id = request.args.get('task_id')
    if not plugin_email_task_manager.is_valid_task_id(task_id):
        return make_response(('task_id invalid.', 400))

    try:
        is_finished = plugin_email_task_manager.query_status(task_id)
    except Exception as e:
        logger.debug(e)
        return make_response((e, 500))

    resp = dict(is_finished=is_finished)
    return make_response((resp, 200))
