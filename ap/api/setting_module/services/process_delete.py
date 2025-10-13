import logging
import os
import shutil

from ap.common.constants import JobType
from ap.common.logger import log_execution_time
from ap.common.multiprocess_sharing import EventQueue, EventRemoveJobs
from ap.common.path_utils import delete_file, gen_sqlite3_file_name, get_data_path, resource_path
from ap.setting_module.models import CfgDataSource, CfgProcess, JobManagement, make_session
from ap.setting_module.services.process_config import update_is_import_column

logger = logging.getLogger(__name__)


@log_execution_time()
def delete_proc_cfg_and_relate_jobs(proc_id):
    with make_session() as meta_session:
        # get all processes to be deleted
        deleting_processes = CfgProcess.get_all_parents_and_children_processes(proc_id, session=meta_session)
        # get ids incase sqlalchemy session is dead
        deleting_process_ids = [proc.id for proc in deleting_processes]

        # stop all jobs before deleting
        target_jobs = JobType.jobs_include_process_id()
        for p in deleting_process_ids:
            EventQueue.put(EventRemoveJobs(job_types=target_jobs, process_id=p))

        for cfg_process in deleting_processes:
            meta_session.delete(cfg_process)

    delete_pulled_data_folders(deleting_process_ids)

    for p in deleting_process_ids:
        delete_transaction_db_file(p)

    return deleting_process_ids


@log_execution_time()
def initialize_proc_config(proc_id):
    # get all processes to be deleted
    deleting_processes = CfgProcess.get_all_parents_and_children_processes(proc_id)
    # get ids incase sqlalchemy session is dead
    deleting_process_ids = [proc.id for proc in deleting_processes]
    # stop all jobs before deleting
    target_jobs = JobType.jobs_include_process_id()

    for p in deleting_process_ids:
        EventQueue.put(EventRemoveJobs(job_types=target_jobs, process_id=p))

    delete_pulled_data_folders(deleting_process_ids)

    for p in deleting_process_ids:
        delete_transaction_db_file(p)
        update_is_import_column(p, is_import=False)


def del_data_source(ds_id):
    """
    delete data source
    :param ds_id:
    :return:
    """
    with make_session() as meta_session:
        ds = meta_session.query(CfgDataSource).get(ds_id)
        if not ds:
            return

        # delete data
        for proc in ds.processes or []:
            delete_proc_cfg_and_relate_jobs(proc.id)
        meta_session.delete(ds)


def delete_transaction_db_file(proc_id):
    try:
        file_name = gen_sqlite3_file_name(proc_id)
        delete_file(file_name)
    except Exception:
        pass

    return True


def delete_pulled_data_folders(process_ids: list[int]):
    data_folder = get_data_path()
    for process_id in process_ids:
        folder_path = resource_path(data_folder, str(process_id))
        if os.path.exists(folder_path):
            shutil.rmtree(folder_path)
            logger.debug('Deleted pulled data folder %s', folder_path)


def del_process_data_from_job_management(ds_id):
    """
    delete data source
    :param ds_id:
    :return:
    """
    with make_session() as meta_session:
        job_info = meta_session.query(JobManagement).get(ds_id)
        if not job_info:
            return

        # delete data
        meta_session.delete(job_info)
        meta_session.commit()
