import logging
import time
from datetime import date
from threading import Thread

from apscheduler.schedulers.blocking import BlockingScheduler

from dtable_events.app.config import ENV_CCNET_CONF_PATH, get_config, DTABLE_WEB_SERVICE_URL
from dtable_events.db import init_db_session_class
from dtable_events.utils.dtable_web_api import DTableWebAPI

logger = logging.getLogger(__name__)


class DTableAutomationRulesStatsUpdater:

    def __init__(self, config):
        self.session_class = init_db_session_class(config)
        ccnet_config = get_config(ENV_CCNET_CONF_PATH)
        if ccnet_config.has_section('Database'):
            ccnet_db_name = ccnet_config.get('Database', 'DB', fallback='ccnet')
        else:
            ccnet_db_name = 'ccnet'
        self.ccnet_db_name = ccnet_db_name

    def start(self):
        DTableAutomationRulesStatsUpdaterTimer(self.session_class, self.ccnet_db_name).start()


class DTableAutomationRulesStatsUpdaterTimer(Thread):

    def __init__(self, session_class, ccnet_db_name):
        super(DTableAutomationRulesStatsUpdaterTimer, self).__init__()
        self.session_class = session_class
        self.ccnet_db_name = ccnet_db_name
        self.daemon = True

    def get_roles(self):
        dtable_web_api = DTableWebAPI(DTABLE_WEB_SERVICE_URL)
        try:
            roles = dtable_web_api.internal_roles()['roles']
        except Exception as e:
            logger.error('get roles error: %s', e)
            return {}
        return roles

    def update_users_stats(self, roles, db_session):
        month = str(date.today())[:7]
        limit, offset = 1000, 0
        username_trigger_count = {}
        exceed_usernames = []
        usernames = []
        db_set_usernames_set = set()
        while True:
            sql = "SELECT username, trigger_count FROM user_auto_rules_statistics_per_month WHERE month=:month LIMIT :offset, :limit"
            users_stats = list(db_session.execute(sql, {'month': month, 'limit': limit, 'offset': offset}))
            if len(users_stats) < limit:
                break
            for user_stats in users_stats:
                username_trigger_count[user_stats.username] = user_stats.trigger_count
                usernames.append(user_stats.username)
            offset += limit
        logger.debug('query out %s users', len(usernames))
        # query db user_quota
        step = 1000
        for i in range(0, len(usernames), step):
            sql = "SELECT username, auto_rules_limit_per_month FROM user_quota WHERE username in :usernames"
            for user_quota in db_session.execute(sql, {'usernames': usernames[i: i+step]}):
                logger.debug('username: %s auto_rules_limit_per_month: %s trigger_count: %s', 
                             user_quota.username, user_quota.auto_rules_limit_per_month, username_trigger_count[user_quota.username])
                if user_quota.auto_rules_limit_per_month is None:
                    continue
                elif user_quota.auto_rules_limit_per_month < 0:  # need to query role
                    db_set_usernames_set.add(user_quota.username)
                    continue
                elif user_quota.auto_rules_limit_per_month == 0:
                    continue
                db_set_usernames_set.add(user_quota.username)
                if user_quota.auto_rules_limit_per_month <= username_trigger_count[user_quota.username]:
                    exceed_usernames.append(user_quota.username)
        # query db users role
        usernames = [username for username in usernames if username not in db_set_usernames_set]
        logger.debug('totally %s users need to check roles', len(usernames))
        if usernames:
            for i in range(0, len(usernames), step):
                sql = "SELECT email, role FROM %s.UserRole WHERE email in :usernames" % self.ccnet_db_name
                for role in db_session.execute(sql, {'usernames': usernames[i: i+step]}):
                    role_trigger_limit = roles.get(role.role, {}).get('automation_rules_limit_per_month', -1)
                    logger.debug('check role user: %s role: %s role_trigger_limit: %s trigger_count: %s', role.email, role.role, role_trigger_limit, username_trigger_count[role.email])
                    if role_trigger_limit < 0:
                        continue
                    if role_trigger_limit <= username_trigger_count[role.email]:
                        exceed_usernames.append(role.email)
        # update exceed
        for i in range(0, len(exceed_usernames), step):
            sql = "UPDATE user_auto_rules_statistics_per_month SET is_exceed=1 WHERE month=:month AND username IN :usernames"
            db_session.execute(sql, {'month': month, 'usernames': exceed_usernames[i: i+step]})
            db_session.commit()

    def update_orgs_stats(self, roles, db_session):
        month = str(date.today())[:7]
        limit, offset = 1000, 0
        org_id_trigger_count = {}
        exceed_org_ids = []
        org_ids = []
        db_set_org_ids_set = set()
        while True:
            sql = "SELECT org_id, trigger_count FROM org_auto_rules_statistics_per_month WHERE month=:month LIMIT :offset, :limit"
            orgs_stats = list(db_session.execute(sql, {'month': month, 'limit': limit, 'offset': offset}))
            for org_stats in orgs_stats:
                org_id_trigger_count[org_stats.org_id] = org_stats.trigger_count
                org_ids.append(org_stats.org_id)
            if len(orgs_stats) < limit:
                break
            offset += limit
        logger.debug('query out %s orgs', len(org_ids))
        # query db user_quota
        step = 1000
        for i in range(0, len(org_ids), step):
            sql = "SELECT org_id, auto_rules_limit_per_month FROM organizations_org_quota WHERE org_id in :org_ids"
            for org_quota in db_session.execute(sql, {'org_ids': org_ids[i: i+step]}):
                logger.debug('org: %s auto_rules_limit_per_month: %s trigger_count: %s', 
                             org_quota.org_id, org_quota.auto_rules_limit_per_month, org_id_trigger_count[org_quota.org_id])
                if org_quota.auto_rules_limit_per_month is None:
                    continue
                elif org_quota.auto_rules_limit_per_month < 0:
                    db_set_org_ids_set.add(org_quota.org_id)
                    continue
                elif org_quota.auto_rules_limit_per_month == 0:  # need to check role
                    continue
                db_set_org_ids_set.add(org_quota.org_id)
                if org_quota.auto_rules_limit_per_month <= org_id_trigger_count[org_quota.org_id]:
                    exceed_org_ids.append(org_quota.org_id)
        # query db orgs role
        org_ids = [org_id for org_id in org_ids if org_id not in db_set_org_ids_set]
        logger.debug('totally %s orgs need to check roles', len(org_ids))
        if org_ids:
            for i in range(0, len(org_ids), step):
                sql = "SELECT org_id, role FROM organizations_orgsettings WHERE org_id in :org_ids"
                for role in db_session.execute(sql, {'org_ids': org_ids[i: i+step]}):
                    role_trigger_limit = roles.get(role.role, {}).get('automation_rules_limit_per_month', -1)
                    logger.debug('check role org: %s role: %s role_trigger_limit: %s trigger_count: %s', role.org_id, role.role, role_trigger_limit, org_id_trigger_count[role.org_id])
                    if role_trigger_limit < 0:
                        continue
                    if role_trigger_limit <= org_id_trigger_count[role.org_id]:
                        exceed_org_ids.append(role.org_id)
        # update exceed
        for i in range(0, len(exceed_org_ids), step):
            sql = "UPDATE org_auto_rules_statistics_per_month SET is_exceed=1 WHERE month=:month AND org_id IN :org_ids"
            db_session.execute(sql, {'month': month, 'org_ids': exceed_org_ids[i: i+step]})
            db_session.commit()

    def update_stats(self):
        db_session = self.session_class()
        roles = self.get_roles()
        logger.debug('roles: %s', roles)
        try:
            self.update_users_stats(roles, db_session)
            self.update_orgs_stats(roles, db_session)
        except Exception as e:
            logger.exception('update users/orgs auto rule stats error: %s', e)

    def run(self):
        sched = BlockingScheduler()

        @sched.scheduled_job('cron', day_of_week='*', hour='*', minute='52')
        def update():
            self.update_stats()

        sched.start()
