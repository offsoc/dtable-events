import logging
from collections import defaultdict
from datetime import datetime, timedelta
from threading import Thread

from apscheduler.schedulers.blocking import BlockingScheduler

from dtable_events.app.metadata_cache_managers import RuleIntervalMetadataCacheManager
from dtable_events.automations.auto_rules_utils import run_regular_execution_rule
from dtable_events.db import init_db_session_class
from dtable_events.utils import get_opt_from_conf_or_env, parse_bool


__all__ = [
    'DTableAutomationRulesScanner',
]


class DTableAutomationRulesScanner(object):

    def __init__(self, config):
        self._enabled = True
        self._parse_config(config)
        self._db_session_class = init_db_session_class(config)

    def _parse_config(self, config):
        """parse send email related options from config file
        """
        section_name = 'AUTOMATION-SCANNER'
        key_enabled = 'enabled'

        if not config.has_section(section_name):
            section_name = 'AUTOMATION SCANNER'
            if not config.has_section(section_name):
                return

        # enabled
        enabled = get_opt_from_conf_or_env(config, section_name, key_enabled, default=True)
        enabled = parse_bool(enabled)
        self._enabled = enabled

    def start(self):
        if not self.is_enabled():
            logging.warning('Can not start dtable automation rules scanner: it is not enabled!')
            return

        logging.info('Start dtable automation rules scanner')

        DTableAutomationRulesScannerTimer(self._db_session_class).start()

    def is_enabled(self):
        return self._enabled


def scan_dtable_automation_rules(db_session):
    sql = '''
            SELECT `dar`.`id`, `run_condition`, `trigger`, `actions`, `last_trigger_time`, `dtable_uuid`, `trigger_count`, `org_id`, dar.`creator` FROM dtable_automation_rules dar
            JOIN dtables d ON dar.dtable_uuid=d.uuid
            WHERE ((run_condition='per_day' AND (last_trigger_time<:per_day_check_time OR last_trigger_time IS NULL))
            OR (run_condition='per_week' AND (last_trigger_time<:per_week_check_time OR last_trigger_time IS NULL))
            OR (run_condition='per_month' AND (last_trigger_time<:per_month_check_time OR last_trigger_time IS NULL)))
            AND dar.is_valid=1 AND d.deleted=0 AND is_pause=0
        '''
    per_day_check_time = datetime.utcnow() - timedelta(hours=23)
    per_week_check_time = datetime.utcnow() - timedelta(days=6)
    per_month_check_time = datetime.utcnow() - timedelta(days=27)  # consider the least month-days 28 in February (the 2nd month) in common years
    rules = list(db_session.execute(sql, {
        'per_day_check_time': per_day_check_time,
        'per_week_check_time': per_week_check_time,
        'per_month_check_time': per_month_check_time
    }))

    dtable_uuids = list({rule.dtable_uuid for rule in rules})
    step = 1000
    usernames = []
    org_ids = []
    owner_dtable_dict = defaultdict(list)  # username/org_id: [dtable_uuid...]
    exceed_dtable_uuids = set()
    month = str(datetime.today())[:7]
    for i in range(0, len(rules), step):
        sql = "SELECT d.uuid, w.owner, w.org_id FROM dtables d JOIN workspaces w ON d.workspace_id=workspace.id WHERE d.uuid IN :dtable_uuids"
        for dtable in db_session.execute(sql, {'dtable_uuids': dtable_uuids[i, i+step]}):
            if dtable.org_id == -1 and '@seafile_group' not in dtable.owner:
                owner_dtable_dict[dtable.owner].append(dtable.uuid)
                usernames.append(dtable.owner)
            elif dtable.org_id != -1:
                owner_dtable_dict[dtable.org_id].append(dtable.uuid)
                org_ids.append(dtable.org_id)
    for i in range(0, len(usernames), step):
        sql = "SELECT username FROM user_auto_rules_statistics_per_month WHERE username in :usernames WHERE is_exceed=1 AND month=:month"
        for user_per_month in db_session.execute(sql, {'usernames': usernames[i: i+step], 'month': month}):
            exceed_dtable_uuids += set(owner_dtable_dict[user_per_month.username])
    for i in range(0, len(org_ids), step):
        sql = "SELECT org_id FROM org_auto_rules_statistics_per_month WHERE org_id in :org_ids WHERE is_exceed=1 AND month=:month"
        for org_per_month in db_session.execute(sql, {'org_ids': org_ids[i: i+step], 'month': month}):
            exceed_dtable_uuids += set(owner_dtable_dict[org_per_month.org_id])

    # each base's metadata only requested once and recorded in memory
    # The reason why it doesn't cache metadata in redis is metadatas in interval rules need to be up-to-date
    rule_interval_metadata_cache_manager = RuleIntervalMetadataCacheManager()
    for rule in rules:
        if rule.dtable_uuid in exceed_dtable_uuids:
            continue
        try:
            run_regular_execution_rule(rule, db_session, rule_interval_metadata_cache_manager)
        except Exception as e:
            logging.exception(e)
            logging.error(f'check rule failed. {rule}, error: {e}')
        db_session.commit()


class DTableAutomationRulesScannerTimer(Thread):

    def __init__(self, db_session_class):
        super(DTableAutomationRulesScannerTimer, self).__init__()
        self.db_session_class = db_session_class

    def run(self):
        sched = BlockingScheduler()
        # fire at every hour in every day of week
        @sched.scheduled_job('cron', day_of_week='*', hour='*')
        def timed_job():
            logging.info('Starts to scan automation rules...')

            db_session = self.db_session_class()
            try:
                scan_dtable_automation_rules(db_session)
            except Exception as e:
                logging.exception('error when scanning dtable automation rules: %s', e)
            finally:
                db_session.close()

        sched.start()
