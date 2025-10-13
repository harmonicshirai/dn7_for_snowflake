from __future__ import annotations

import logging
import os
from abc import ABC, abstractmethod
from collections import defaultdict
from typing import Optional, Union

import pandas as pd
import sqlalchemy as sa

from ap.api.setting_module.services.software_workshop_etl_services import SoftwareWorkshopDef
from ap.common.common_utils import (
    BoundType,
    TimeRange,
    get_data_path,
)
from ap.common.logger import log_execution_time
from ap.common.pydn.dblib.snowflake import Snowflake
from ap.etl.pull.common import PullBase, PullDataType
from ap.setting_module.models import CfgProcess

logger = logging.getLogger(__name__)


class PullSoftwareWorkshop(PullBase, ABC):
    @abstractmethod
    def get_min_max_factory_datetime_per_process_query(self) -> Optional[sa.Select]: ...

    def software_workshop_def(self) -> SoftwareWorkshopDef:
        return self.processes[0].data_source.software_workshop_def()

    def get_factory_time_range_per_process(self, factory_db_instance) -> dict[int, TimeRange]:
        if not self.processes:
            return {}

        sql = self.get_min_max_factory_datetime_per_process_query()
        _, rows = factory_db_instance.run_sql(sql)

        process_factid_to_processes: dict[str, list[CfgProcess]] = defaultdict(list)
        for process in self.processes:
            process_factid_to_processes[process.process_factid].append(process)

        process_id_to_factory_time_range: dict[int, TimeRange] = {}
        for row in rows:
            process_factid = row[self.software_workshop_def().child_equip_id]
            for process in process_factid_to_processes[process_factid]:
                min_datetime = row[f'min_{process.get_auto_increment_col_else_get_date()}']
                max_datetime = row[f'max_{process.get_auto_increment_col_else_get_date()}']
                process_id_to_factory_time_range[process.id] = TimeRange(
                    min_ts=min_datetime,
                    min_ts_bound_type=BoundType.INCLUDED,
                    max_ts=max_datetime,
                    max_ts_bound_type=BoundType.INCLUDED,
                )
        return process_id_to_factory_time_range

    @log_execution_time()
    def pull_master_data(
        self,
        factory_db_instance: Snowflake,
    ):
        child_equip_ids = [process.process_factid for process in self.processes]
        sql = self.software_workshop_def().get_master_query(child_equip_ids=child_equip_ids)
        cols, ret = factory_db_instance.run_sql(sql, row_is_dict=False)
        self.save_meta_data(ret, cols, PullDataType.MASTER)

    @log_execution_time()
    def pull_code_data(
        self,
        factory_db_instance: Snowflake,
    ):
        child_equip_ids = [process.process_factid for process in self.processes]
        sql = self.software_workshop_def().get_code_name_mapping_query(child_equip_ids=child_equip_ids)
        cols, ret = factory_db_instance.run_sql(sql, row_is_dict=False)
        self.save_meta_data(ret, cols, PullDataType.CODE)

    def pull_data(
        self,
        factory_db_instance: Snowflake,
    ):
        self.pull_master_data(
            factory_db_instance=factory_db_instance,
        )
        self.pull_code_data(
            factory_db_instance=factory_db_instance,
        )
        super().pull_data(
            factory_db_instance=factory_db_instance,
        )

    @log_execution_time()
    def save_meta_data(
        self,
        data: Union[list[tuple], list[dict]],
        columns: list[str],
        data_type: PullDataType,
    ):
        df = pd.DataFrame(data, columns=columns)
        data_path = get_data_path()
        file_name = f'{data_type.name}.feather'
        group_cols = [self.software_workshop_def().child_equip_id]
        for (child_equip_id, *_), process_df in df.groupby(by=group_cols, dropna=False):
            for process in self.processes:
                if process.process_factid != child_equip_id:
                    continue

                folder_path = os.path.join(data_path, str(process.id))
                if not os.path.exists(folder_path):
                    os.mkdir(folder_path)
                file_path = os.path.join(folder_path, file_name)

                process_df.to_feather(file_path)

    @log_execution_time()
    def save_transaction_data(
        self,
        data: Union[list[tuple], list[dict]],
        columns: list[str],
    ):
        df = pd.DataFrame(data, columns=columns)
        group_cols = [self.software_workshop_def().child_equip_id]
        for (child_equip_id, *_), process_df in df.groupby(by=group_cols, dropna=False, sort=False):
            process: CfgProcess = next(filter(lambda x: x.process_factid == child_equip_id, self.processes))
            self.save_transaction_data_for_one_process(process_df, process)
