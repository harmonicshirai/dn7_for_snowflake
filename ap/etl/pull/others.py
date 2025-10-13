import datetime as dt
import logging
from typing import Union

import pandas as pd
import sqlalchemy as sa

from ap import log_execution_time
from ap.common.common_utils import BoundType, TimeRange
from ap.etl.pull.common import PullBase
from ap.setting_module.models import CfgProcess

logger = logging.getLogger(__name__)


class PullOthers(PullBase):
    def get_factory_time_range_per_process(self, factory_db_instance) -> dict[int, TimeRange]:
        if not self.processes:
            return {}

        process = self.processes[0]

        datetime_key = process.get_auto_increment_col_else_get_date()
        sql_clause = sa.text(
            f"""
SELECT
    MIN("{datetime_key}") as min_time,
    MAX("{datetime_key}") as max_time
FROM {process.table_name}
"""
        )
        _, ret = factory_db_instance.run_sql(sql_clause, row_is_dict=False)
        min_time, max_time = (ret[0][0], ret[0][-1]) if len(ret) > 0 else (None, None)

        # not a datetime, but a date
        if min_time and not isinstance(min_time, dt.datetime) and isinstance(min_time, dt.date):
            min_time = dt.datetime.combine(min_time, dt.time(0, 0, 0, 0))

        # not a datetime, but a date
        if max_time and not isinstance(max_time, dt.datetime) and isinstance(max_time, dt.date):
            max_time = dt.datetime.combine(max_time, dt.time(0, 0, 0, 0))

        if not isinstance(min_time, dt.datetime):
            logger.error(f'min max time factory db are not date time type {process.table_name}')

        return {
            process.id: TimeRange(
                min_ts=min_time,
                min_ts_bound_type=BoundType.INCLUDED,
                max_ts=max_time,
                max_ts_bound_type=BoundType.INCLUDED,
            )
        }

    @classmethod
    def get_transaction_data_query(cls, process: CfgProcess, time_range: TimeRange) -> sa.Select:
        columns = [
            sa.column(col.column_raw_name)
            for col in process.columns
            if col.column_raw_name is not None and col.column_raw_name != ''
        ]
        increment_column = sa.column(process.get_auto_increment_col_else_get_date())
        table = sa.table(process.table_name)

        conditions = []
        if time_range.min.kind is BoundType.INCLUDED:
            conditions.append(increment_column >= time_range.min.value)
        elif time_range.min.kind is BoundType.EXCLUDED:
            conditions.append(increment_column > time_range.min.value)
        else:
            raise ValueError(f'Not supported unbounded query for process: {process.id}')

        if time_range.max.kind is BoundType.INCLUDED:
            conditions.append(increment_column <= time_range.max.value)
        elif time_range.max.kind is BoundType.EXCLUDED:
            conditions.append(increment_column < time_range.max.value)
        else:
            raise ValueError(f'Not supported unbounded query for process: {process.id}')

        # https://github.com/sqlalchemy/sqlalchemy/discussions/6434#discussioncomment-704389
        sql = sa.select(*columns).select_from(table)
        if conditions:
            sql = sql.where(sa.and_(*conditions))
        return sql

    @log_execution_time()
    def save_transaction_data(
        self,
        data: Union[list[tuple], list[dict]],
        columns: list[str],
    ):
        process_df = pd.DataFrame(data, columns=columns)
        process: CfgProcess = self.processes[0]
        self.save_transaction_data_for_one_process(process_df, process)
