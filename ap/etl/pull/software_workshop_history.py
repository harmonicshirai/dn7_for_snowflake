from __future__ import annotations

import logging
from typing import Optional

import sqlalchemy as sa

from ap.common.common_utils import TimeRange
from ap.etl.pull.software_workshop import PullSoftwareWorkshop
from ap.setting_module.models import CfgProcess

logger = logging.getLogger(__name__)


class PullSoftwareWorkshopHistory(PullSoftwareWorkshop):
    def get_min_max_factory_datetime_per_process_query(self) -> Optional[sa.Select]:
        if not self.processes:
            return None
        return self.software_workshop_def().get_history_min_max_time_query_group_by_child_equip_id(
            datetime_keys=[p.get_auto_increment_col_else_get_date() for p in self.processes],
            child_equip_ids=[p.process_factid for p in self.processes],
        )

    def get_transaction_data_query(self, process: CfgProcess, time_range: TimeRange) -> sa.Select:
        return self.software_workshop_def().get_history_data_query(
            process_factid=process.process_factid,
            datetime_key=process.get_auto_increment_col_else_get_date(),
            time_range=time_range,
        )
