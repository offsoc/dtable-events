import logging
from datetime import date
from queue import Queue
from threading import Thread

from seaserv import ccnet_api

from dtable_events.app.config import ENV_CCNET_CONF_PATH, get_config, DTABLE_WEB_SERVICE_URL
from dtable_events.db import init_db_session_class
from dtable_events.utils.dtable_web_api import DTableWebAPI

logger = logging.getLogger(__name__)


class AutoRulesStatsUpdater:

    def init(self, config):
        self.config = config
        self.queue = Queue()
        self.dtable_web_api = DTableWebAPI(DTABLE_WEB_SERVICE_URL)
        self.roles = None
        self.db_session_class = init_db_session_class(config)

        ccnet_config = get_config(ENV_CCNET_CONF_PATH)
        if ccnet_config.has_section('Database'):
            ccnet_db_name = ccnet_config.get('Database', 'DB', fallback='ccnet')
        else:
            ccnet_db_name = 'ccnet'
        self.ccnet_db_name = ccnet_db_name

    def get_roles(self):
        if self.roles:
            return self.roles
        self.roles = self.dtable_web_api.internal_roles()
        return self.roles

    def get_user_quota(self, db_session, username):
        sql = "SELECT username, auto_rules_limit_per_month FROM user_quota WHERE username=:username"
        row = db_session.execute(sql, {'username': username}).fetchone()
        if row and row.auto_rules_limit_per_month and row.auto_rules_limit_per_month != 0:
            return row.auto_rules_limit_per_month
        user = ccnet_api.get_emailuser(username)
        user_role = user.role
        return self.roles.get(user_role, {}).get('automation_rules_limit_per_month', -1)

    def get_org_quota(self, db_session, org_id):
        sql = "SELECT org_id, auto_rules_limit_per_month FROM organizations_org_quota WHERE org_id=:org_id"
        row = db_session.execute(sql, {'org_id': org_id}).fetchone()
        if row and row.auto_rules_limit_per_month and row.auto_rules_limit_per_month != 0:
            return row.auto_rules_limit_per_month
        sql = "SELECT role FROM organizations_orgsettings WHERE org_id=:org_id"
        row = db_session.execute(sql, {'org_id': org_id}).fetchone()
        if not row:
            org_role = 'org_default'  # check from dtable-web/seahub/role_permissions/settings DEFAULT_ENABLED_ROLE_PERMISSIONS[ORG_DEFAULT]
        else:
            org_role = row.role
        return self.get_roles().get(org_role, {}).get('automation_rules_limit_per_month', -1)

    def get_user_usage(self, db_session, username):
        """
        :return: trigger_count -> int, has_sent_warning -> bool
        """
        sql = "SELECT trigger_count, has_sent_warning FROM user_auto_rules_statistics_per_month WHERE username=:username AND month=:month"
        row = db_session.execute(sql, {'username': username, 'month': str(date.today())[:7]}).fetchone()
        if not row:
            return 0, False
        return row.trigger_count, row.has_sent_warning

    def get_org_usage(self, db_session, org_id):
        """
        :return: trigger_count -> int, has_sent_warning -> bool
        """
        sql = "SELECT trigger_count, has_sent_warning FROM org_auto_rules_statistics_per_month WHERE org_id=:org_id AND month=:month"
        row = db_session.execute(sql, {'org_id': org_id, 'month': str(date.today())[:7]}).fetchone()
        if not row:
            return 0, False
        return row.trigger_count, row.has_sent_warning

    def update_user(self, db_session, username):
        limit = self.get_user_quota(db_session, username)
        if limit < 0:
            return
        usage, has_sent_warning = self.get_user_usage(db_session, username)
        if usage >= limit:
            sql = "UPDATE user_auto_rules_statistics_per_month SET is_exceed=1 WHERE username=:username"
            db_session.execute(sql, {'username': username})
            db_session.commit()
            return
        if not has_sent_warning and usage >= limit * 0.9:
            self.dtable_web_api.internal_add_notification([username], 'autorule_trigger_reach_warning', {'limit': limit, 'usage': usage})
            sql = "UPDATE user_auto_rules_statistics_per_month SET has_sent_warning=1 WHERE username=:username"
            db_session.execute(sql, {'username': username})
            db_session.commit()

    def update_org(self, db_session, org_id):
        limit = self.get_org_quota(db_session, org_id)
        if limit < 0:
            return
        usage, has_sent_warning = self.get_org_usage(db_session, org_id)
        if usage >= limit:
            sql = "UPDATE org_auto_rules_statistics_per_month SET is_exceed=1 WHERE org_id=:org_id"
            db_session.execute(sql, {'org_id': org_id})
            db_session.commit()
            return
        if not has_sent_warning and usage >= limit * 0.9:
            admins = []
            sql = "SELECT email FROM %s.OrgUser WHERE org_id=:org_id AND is_staff=1" % self.ccnet_db_name
            for row in db_session.execute(sql, {'org_id': org_id}):
                admins.append(row.email)
            self.dtable_web_api.internal_add_notification(admins, 'autorule_trigger_reach_warning', {'limit': limit, 'usage': usage})
            sql = "UPDATE org_auto_rules_statistics_per_month SET has_sent_warning=1 WHERE org_id=:org_id"
            db_session.execute(sql, {'org_id': org_id})
            db_session.commit()

    def update_stats(self):
        while True:
            auto_rule_info = self.queue.get()
            username = auto_rule_info.get('username')
            org_id = auto_rule_info.get('org_id')
            db_session = self.db_session_class()
            try:
                if org_id == -1 and username:
                    self.update_user(db_session, username)
                elif org_id != -1:
                    self.update_org(db_session, org_id)
            except Exception as e:
                logger.exception('update stats info: %s error: %s', auto_rule_info, e)
            finally:
                db_session.close()

    def start(self):
        Thread(target=self.update_stats, daemon=True, name='Thread-auto-rules-stats-updater').start()

    def add_info(self, info):
        self.queue.put(info)


auto_rules_stats_updater = AutoRulesStatsUpdater()
