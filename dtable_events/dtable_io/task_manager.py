import os
import queue
import threading
import time
import uuid
from threading import Lock


from seaserv import seafile_api


class TaskManager(object):

    def __init__(self):
        self.tasks_map = {}
        self.task_results_map = {}
        self.tasks_queue = queue.Queue(10)
        self.config = None
        self.current_task_info = {}
        self.threads = []
        self.dataset_sync_ids = set()
        self.dataset_sync_ids_lock = Lock()
        self.conf = {}

    def init(self, workers, file_server_port, io_task_timeout, config):
        self.conf['file_server_port'] = file_server_port
        self.conf['io_task_timeout'] = io_task_timeout
        self.conf['workers'] = workers

        self.config = config

    def is_valid_task_id(self, task_id):
        return task_id in (self.tasks_map.keys() | self.task_results_map.keys())

    def add_export_task(self, username, repo_id, workspace_id, dtable_uuid, dtable_name, ignore_asset):
        from dtable_events.dtable_io import get_dtable_export_content

        asset_dir_id = None
        if not ignore_asset:
            asset_dir_path = os.path.join('/asset', dtable_uuid)
            asset_dir_id = seafile_api.get_dir_id_by_path(repo_id, asset_dir_path)

        task_id = str(uuid.uuid4())
        task = (get_dtable_export_content,
                (username, repo_id, workspace_id, dtable_uuid, asset_dir_id, self.config))
        self.tasks_queue.put(task_id)
        self.tasks_map[task_id] = task

        return task_id

    def add_import_task(self, username, repo_id, workspace_id, dtable_uuid, dtable_file_name, in_storage,
                        can_use_automation_rules, can_use_workflows, can_use_external_apps, owner, org_id):
        from dtable_events.dtable_io import post_dtable_import_files

        task_id = str(uuid.uuid4())
        task = (post_dtable_import_files,
                (username, repo_id, workspace_id, dtable_uuid, dtable_file_name, in_storage,
                 can_use_automation_rules, can_use_workflows, can_use_external_apps, owner, org_id, self.config))
        self.tasks_queue.put(task_id)
        self.tasks_map[task_id] = task
        return task_id

    def add_export_dtable_asset_files_task(self, username, repo_id, dtable_uuid, files, files_map=None):
        from dtable_events.dtable_io import get_dtable_export_asset_files

        task_id = str(uuid.uuid4())
        task = (get_dtable_export_asset_files,
                (username, repo_id, dtable_uuid, files, task_id, files_map))
        self.tasks_queue.put(task_id)
        self.tasks_map[task_id] = task
        return task_id
    
    def add_export_dtable_big_data_screen_task(self, username, repo_id, dtable_uuid, page_id):
        from  dtable_events.dtable_io import get_dtable_export_big_data_screen
        task_id = str(uuid.uuid4())
        task = (get_dtable_export_big_data_screen,
                (username, repo_id, dtable_uuid, page_id, task_id))
        self.tasks_queue.put(task_id)
        self.tasks_map[task_id] = task
        return task_id
    
    def add_import_dtable_big_data_screen_task(self, username, repo_id, dtable_uuid, page_id):
        from  dtable_events.dtable_io import import_big_data_screen
        task_id = str(uuid.uuid4())
        task = (import_big_data_screen,
                (username, repo_id, dtable_uuid, page_id))
        self.tasks_queue.put(task_id)
        self.tasks_map[task_id] = task
        return task_id


    def add_transfer_dtable_asset_files_task(self, username, repo_id, dtable_uuid, files, files_map, parent_dir, relative_path, replace, repo_api_token, seafile_server_url):
        from dtable_events.dtable_io import get_dtable_transfer_asset_files
        task_id = str(uuid.uuid4())
        task = (get_dtable_transfer_asset_files,
                (username,
                 repo_id,
                 dtable_uuid,
                 files,
                 task_id,
                 files_map,
                 parent_dir,
                 relative_path,
                 replace,
                 repo_api_token,
                 seafile_server_url))
        self.tasks_queue.put(task_id)
        self.tasks_map[task_id] = task
        return task_id

    def add_parse_excel_csv_task(self, username, repo_id, workspace_id, dtable_name, file_type, custom):
        from dtable_events.dtable_io import parse_excel_csv

        task_id = str(uuid.uuid4())
        task = (parse_excel_csv,
                (username, repo_id, workspace_id, dtable_name, file_type, custom, self.config))
        self.tasks_queue.put(task_id)
        self.tasks_map[task_id] = task
        return task_id

    def add_import_excel_csv_task(self, username, repo_id, workspace_id, dtable_uuid, dtable_name, lang):
        from dtable_events.dtable_io import import_excel_csv

        task_id = str(uuid.uuid4())
        task = (import_excel_csv,
                (username, repo_id, workspace_id, dtable_uuid, dtable_name, lang, self.config))
        self.tasks_queue.put(task_id)
        self.tasks_map[task_id] = task
        return task_id

    def add_import_excel_csv_add_table_task(self, username, repo_id, workspace_id, dtable_uuid, dtable_name, lang):
        from dtable_events.dtable_io import import_excel_csv_add_table

        task_id = str(uuid.uuid4())
        task = (import_excel_csv_add_table,
                (username, repo_id, workspace_id, dtable_uuid, dtable_name, lang, self.config))
        self.tasks_queue.put(task_id)
        self.tasks_map[task_id] = task
        return task_id

    def add_append_excel_csv_append_parsed_file_task(self, username, repo_id, dtable_uuid, file_name, table_name):
        from dtable_events.dtable_io import append_excel_csv_append_parsed_file

        task_id = str(uuid.uuid4())
        task = (append_excel_csv_append_parsed_file,
                (username, repo_id, dtable_uuid, file_name, table_name))
        self.tasks_queue.put(task_id)
        self.tasks_map[task_id] = task
        return task_id

    def add_append_excel_csv_upload_file_task(self, username, repo_id, file_name, dtable_uuid, table_name, file_type):
        from dtable_events.dtable_io import append_excel_csv_upload_file

        task_id = str(uuid.uuid4())
        task = (append_excel_csv_upload_file,
                (username, repo_id, file_name, dtable_uuid, table_name, file_type))
        self.tasks_queue.put(task_id)
        self.tasks_map[task_id] = task
        return task_id

    def add_run_auto_rule_task(self, automation_rule_id, creator, owner, org_id, dtable_uuid, run_condition, trigger, actions):
        from dtable_events.automations.auto_rules_utils import run_auto_rule_task
        task_id = str(uuid.uuid4())
        options = {
            'run_condition': run_condition,
            'dtable_uuid': dtable_uuid,
            'org_id': org_id,
            'owner': owner,
            'creator': creator,
            'rule_id': automation_rule_id
        }

        task = (run_auto_rule_task, (trigger, actions, options, self.config))
        self.tasks_queue.put(task_id)
        self.tasks_map[task_id] = task
        return task_id

    def add_update_excel_csv_update_parsed_file_task(self, username, repo_id, dtable_uuid, file_name, table_name,
                                                     selected_columns):
        from dtable_events.dtable_io import update_excel_csv_update_parsed_file

        task_id = str(uuid.uuid4())
        task = (update_excel_csv_update_parsed_file,
                (username, repo_id, dtable_uuid, file_name, table_name, selected_columns))
        self.tasks_queue.put(task_id)
        self.tasks_map[task_id] = task
        return task_id

    def add_update_excel_upload_excel_task(self, username, repo_id, file_name, dtable_uuid, table_name):
        from dtable_events.dtable_io import update_excel_upload_excel

        task_id = str(uuid.uuid4())
        task = (update_excel_upload_excel,
                (username, repo_id, file_name, dtable_uuid, table_name))
        self.tasks_queue.put(task_id)
        self.tasks_map[task_id] = task
        return task_id

    def add_update_csv_upload_csv_task(self, username, repo_id, file_name, dtable_uuid, table_name):
        from dtable_events.dtable_io import update_csv_upload_csv

        task_id = str(uuid.uuid4())
        task = (update_csv_upload_csv,
                (username, repo_id, file_name, dtable_uuid, table_name))
        self.tasks_queue.put(task_id)
        self.tasks_map[task_id] = task
        return task_id

    def add_import_excel_csv_to_dtable_task(self, username, repo_id, workspace_id, dtable_name, dtable_uuid, file_type, lang):
        from dtable_events.dtable_io import import_excel_csv_to_dtable

        task_id = str(uuid.uuid4())
        task = (import_excel_csv_to_dtable, (username, repo_id, workspace_id, dtable_name, dtable_uuid, file_type, lang))
        self.tasks_queue.put(task_id)
        self.tasks_map[task_id] = task
        return task_id

    def add_import_excel_csv_to_table_task(self, username, repo_id, workspace_id, file_name, dtable_uuid, file_type, lang):
        from dtable_events.dtable_io import import_excel_csv_to_table

        task_id = str(uuid.uuid4())
        task = (import_excel_csv_to_table, (username, repo_id, workspace_id, file_name, dtable_uuid, file_type, lang))
        self.tasks_queue.put(task_id)
        self.tasks_map[task_id] = task
        return task_id

    def add_update_table_via_excel_csv_task(self, username, repo_id, file_name, dtable_uuid, table_name, selected_columns, file_type):
        from dtable_events.dtable_io import update_table_via_excel_csv

        task_id = str(uuid.uuid4())
        task = (update_table_via_excel_csv, (username, repo_id, file_name, dtable_uuid, table_name, selected_columns, file_type))
        self.tasks_queue.put(task_id)
        self.tasks_map[task_id] = task
        return task_id

    def add_append_excel_csv_to_table_task(self, username, repo_id, file_name, dtable_uuid, table_name, file_type):
        from dtable_events.dtable_io import append_excel_csv_to_table

        task_id = str(uuid.uuid4())
        task = (append_excel_csv_to_table, (username, repo_id, file_name, dtable_uuid, table_name, file_type))
        self.tasks_queue.put(task_id)
        self.tasks_map[task_id] = task
        return task_id

    def query_status(self, task_id):
        task_result = self.task_results_map.pop(task_id, None)
        if task_result == 'success':
            return True, None
        if isinstance(task_result, str) and task_result.startswith('error_'):
            return True, task_result[6:]
        return False, None

    def convert_page_to_pdf(self, dtable_uuid, page_id, row_id):
        from dtable_events.dtable_io import convert_page_to_pdf

        task_id = str(uuid.uuid4())
        task = (convert_page_to_pdf,
                (dtable_uuid, page_id, row_id))
        self.tasks_queue.put(task_id)
        self.tasks_map[task_id] = task

        return task_id

    def add_import_table_from_base_task(self, context):
        from dtable_events.dtable_io.import_table_from_base import import_table_from_base

        task_id = str(uuid.uuid4())
        task = (import_table_from_base, (context,))
        self.tasks_queue.put(task_id)
        self.tasks_map[task_id] = task

        return task_id

    def add_import_common_dataset_task(self, context):
        from dtable_events.dtable_io.import_sync_common_dataset import import_common_dataset

        task_id = str(uuid.uuid4())
        task = (import_common_dataset, (context, self.config))
        self.tasks_queue.put(task_id)
        self.tasks_map[task_id] = task

        return task_id

    def add_sync_common_dataset_task(self, context):
        """
        return: task_id -> str or None, error_type -> str or None
        """
        from dtable_events.dtable_io.import_sync_common_dataset import sync_common_dataset

        dataset_sync_id = context.get('dataset_sync_id')
        with self.dataset_sync_ids_lock:
            if self.is_dataset_id_syncing(dataset_sync_id):
                return None, 'syncing'
            self.dataset_sync_ids.add(dataset_sync_id)

        task_id = str(uuid.uuid4())
        task = (sync_common_dataset, (context, self.config))
        self.tasks_queue.put(task_id)
        self.tasks_map[task_id] = task

        return task_id, None

    def add_convert_view_to_execl_task(self, dtable_uuid, table_id, view_id, username, id_in_org, user_department_ids_map, permission, name, repo_id, is_support_image):
        from dtable_events.dtable_io import convert_view_to_execl

        task_id = str(uuid.uuid4())
        task = (convert_view_to_execl, (dtable_uuid, table_id, view_id, username, id_in_org, user_department_ids_map, permission, name, repo_id, is_support_image))
        self.tasks_queue.put(task_id)
        self.tasks_map[task_id] = task

        return task_id

    def add_convert_table_to_execl_task(self, dtable_uuid, table_id, username, permission, name, repo_id, is_support_image):
        from dtable_events.dtable_io import convert_table_to_execl

        task_id = str(uuid.uuid4())
        task = (convert_table_to_execl, (dtable_uuid, table_id, username, permission, name, repo_id, is_support_image))
        self.tasks_queue.put(task_id)
        self.tasks_map[task_id] = task

        return task_id

    def add_app_users_sync_task(self, dtable_uuid, app_name, app_id, table_name, table_id, username):
        from dtable_events.dtable_io import app_user_sync
        task_id = str(uuid.uuid4())
        task = (app_user_sync, (dtable_uuid, app_name, app_id, table_name, table_id, username, self.config))
        self.tasks_queue.put(task_id)
        self.tasks_map[task_id] = task

        return task_id

    def add_export_page_design_task(self, repo_id, dtable_uuid, page_id, username):
        from dtable_events.dtable_io import export_page_design
        task_id = str(uuid.uuid4())
        task = (export_page_design, (repo_id, dtable_uuid, page_id, username))
        self.tasks_queue.put(task_id)
        self.tasks_map[task_id] = task

        return task_id
    
    def add_import_page_design_task(self, repo_id, workspace_id, dtable_uuid, page_id, is_dir, username):
        from dtable_events.dtable_io import import_page_design
        task_id = str(uuid.uuid4())
        task = (import_page_design, (repo_id, workspace_id, dtable_uuid, page_id, is_dir, username))
        self.tasks_queue.put(task_id)
        self.tasks_map[task_id] = task

        return task_id

    def threads_is_alive(self):
        info = {}
        for t in self.threads:
            info[t.name] = t.is_alive()
        return info

    def handle_task(self):
        from dtable_events.dtable_io import dtable_io_logger

        while True:
            try:
                task_id = self.tasks_queue.get(timeout=2)
            except queue.Empty:
                continue
            except Exception as e:
                dtable_io_logger.error(e)
                continue

            task = self.tasks_map.get(task_id)
            if type(task) != tuple or len(task) < 1:
                continue
            if type(task[0]).__name__ != 'function':
                continue
            task_info = task_id + ' ' + str(task[0])
            try:
                self.current_task_info[task_id] = task_info
                dtable_io_logger.info('Run task: %s' % task_info)
                start_time = time.time()

                # run
                task[0](*task[1])
                self.task_results_map[task_id] = 'success'

                finish_time = time.time()
                dtable_io_logger.info('Run task success: %s cost %ds \n' % (task_info, int(finish_time - start_time)))
                self.current_task_info.pop(task_id, None)
            except Exception as e:
                self.task_results_map[task_id] = 'error_' + str(e.args[0])
                if str(e.args[0]) in ('the number of cells accessing the table exceeds the limit',
                                      'Excel format error', 'Duplicated column names are not supported',
                                      'Number of cells returned exceeds the limit of 1 million'):
                    dtable_io_logger.warning('Failed to handle task %s, error: %s \n' % (task_info, e))
                elif str(e.args[0]).startswith('import_sync_common_dataset:'):
                    # Errors in import/sync common dataset, those have been record in real task code, so no duplicated error logs here
                    # Including source/destination table not found...
                    dtable_io_logger.warning('Failed to handle task %s error: %s \n' % (task_info, e))
                elif str(e.args[0]).startswith('import_table_from_base:'):
                    # Errors in import-table-from-base, those have been record in real task code, so no duplicated error logs here
                    dtable_io_logger.warning('Failed to handle task %s error: %s \n' % (task_info, e))
                else:
                    dtable_io_logger.exception(e)
                    dtable_io_logger.error('Failed to handle task %s, error: %s \n' % (task_info, e))
                self.current_task_info.pop(task_id, None)
            finally:
                self.tasks_map.pop(task_id, None)
                if hasattr(task[0], '__name__') and task[0].__name__ == 'sync_common_dataset':
                    context = task[1][0]
                    self.finish_dataset_id_sync(context.get('dataset_sync_id'))

    def run(self):
        thread_num = self.conf['workers']
        for i in range(thread_num):
            t_name = 'TaskManager Thread-' + str(i)
            t = threading.Thread(target=self.handle_task, name=t_name)
            self.threads.append(t)
            t.setDaemon(True)
            t.start()

    def cancel_task(self, task_id):
        self.tasks_map.pop(task_id, None)

    def is_dataset_id_syncing(self, dataset_id):
        return dataset_id in self.dataset_sync_ids

    def finish_dataset_id_sync(self, db_sync_id):
        with self.dataset_sync_ids_lock:
            self.dataset_sync_ids -= {db_sync_id}


task_manager = TaskManager()
