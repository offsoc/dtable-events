# -*- coding: utf-8 -*-
from dtable_events.activities.handlers import MessageHandler
from dtable_events.statistics.counter import UserActivityCounter
from dtable_events.dtable_io.dtable_io_server import DTableIOServer
from dtable_events.tasks.instant_notices_sender import InstantNoticeSender
from dtable_events.tasks.email_notices_sender import EmailNoticesSender
from dtable_events.tasks.dtables_cleaner import DTablesCleaner
from dtable_events.tasks.dtable_updates_sender import DTableUpdatesSender
from dtable_events.tasks.dtable_real_time_rows_counter import DTableRealTimeRowsCounter
from dtable_events.tasks.ldap_syncer import LDAPSyncer
from dtable_events.tasks.dtable_asset_trash_cleaner import DTableAssetTrashCleaner
from dtable_events.tasks.license_expiring_notices_sender import LicenseExpiringNoticesSender
from dtable_events.notification_rules.handler import NotificationRuleHandler
from dtable_events.notification_rules.dtable_notification_rules_scanner import DTableNofiticationRulesScanner
from dtable_events.automations.handler import AutomationRuleHandler
from dtable_events.automations.dtable_automation_rules_scanner import DTableAutomationRulesScanner
from dtable_events.automations.dtable_automation_rules_stats_updater import DTableAutomationRulesStatsUpdater
from dtable_events.webhook.webhook import Webhooker
from dtable_events.common_dataset.common_dataset_syncer import CommonDatasetSyncer
from dtable_events.tasks.big_data_storage_stats_worker import BigDataStorageStatsWorker
from dtable_events.data_sync.data_syncer import DataSyncer
from dtable_events.workflow.workflow_actions import WorkflowActionsHandler
from dtable_events.workflow.workflow_schedules_scanner import WorkflowSchedulesScanner
from dtable_events.page_design.manager import conver_page_to_pdf_manager


class App(object):
    def __init__(self, config, task_mode):
        self._enable_foreground_tasks = task_mode.enable_foreground_tasks
        self._enable_background_tasks = task_mode.enable_background_tasks

        if self._enable_foreground_tasks:
            self._dtable_io_server = DTableIOServer(config)

        if self._enable_background_tasks:
            # redis client subscriber
            self._message_handler = MessageHandler(config)
            self._notification_rule_handler = NotificationRuleHandler(config)
            self._automation_rule_handler = AutomationRuleHandler(config)
            self._user_activity_counter = UserActivityCounter(config)
            self._dtable_real_time_rows_counter = DTableRealTimeRowsCounter(config)
            self._workflow_actions_handler = WorkflowActionsHandler(config)
            self._webhooker = Webhooker(config)
            # cron jobs
            self._instant_notices_sender = InstantNoticeSender(config)
            self._email_notices_sender = EmailNoticesSender(config)
            self._dtables_cleaner = DTablesCleaner(config)
            self._dtable_updates_sender = DTableUpdatesSender(config)
            self._dtable_notification_rules_scanner = DTableNofiticationRulesScanner(config)
            self._dtable_automation_rules_scanner = DTableAutomationRulesScanner(config)
            self._dtable_automation_rules_stats_updater = DTableAutomationRulesStatsUpdater(config)
            self._ldap_syncer = LDAPSyncer(config)
            self._common_dataset_syncer = CommonDatasetSyncer(config)
            self._big_data_storage_stats_worker = BigDataStorageStatsWorker(config)
            self._data_syncr = DataSyncer(config)
            self._workflow_schedule_scanner = WorkflowSchedulesScanner(config)
            self._dtable_asset_trash_cleaner = DTableAssetTrashCleaner(config)
            self._license_expiring_notices_sender = LicenseExpiringNoticesSender()
            # convert pdf manager
            conver_page_to_pdf_manager.init(config)

    def serve_forever(self):

        if self._enable_foreground_tasks:
            self._dtable_io_server.start()

        if self._enable_background_tasks:
            # redis client subscriber
            self._message_handler.start()                    # always True
            self._notification_rule_handler.start()          # always True
            self._automation_rule_handler.start()            # always True
            self._user_activity_counter.start()              # always True
            self._dtable_real_time_rows_counter.start()      # default True
            self._workflow_actions_handler.start()           # always True
            self._webhooker.start()                          # always True
            # cron jobs
            self._instant_notices_sender.start()             # default True
            self._email_notices_sender.start()               # default True
            self._dtables_cleaner.start()                    # default True
            self._dtable_updates_sender.start()              # default True
            self._dtable_notification_rules_scanner.start()  # default True
            self._dtable_automation_rules_scanner.start()    # default True
            self._dtable_automation_rules_stats_updater.start()  # always True
            self._ldap_syncer.start()                        # default False
            self._common_dataset_syncer.start()              # default True
            self._big_data_storage_stats_worker.start()      # always True
            self._data_syncr.start()                         # default True
            self._workflow_schedule_scanner.start()          # default True
            self._dtable_asset_trash_cleaner.start()         # always True
            self._license_expiring_notices_sender.start()    # always True
            # convert pdf manager
            conver_page_to_pdf_manager.start()               # always True
