from typing import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.engine.base import Connection


class OrphanedDataBase:
    __tablename__ = None
    find_orphaned_data_sql = None

    @classmethod
    def get(cls, conn: Connection):
        is_existing = bool(
            conn.execute(sa.text(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{cls.__tablename__}';"))
            .scalars()
            .all()
        )
        if not is_existing:
            return []
        return conn.execute(sa.text(cls.find_orphaned_data_sql)).scalars().fetchall()

    @classmethod
    def remove(cls, conn: Connection, broken_ids: Sequence[int]):
        for broken_id in broken_ids:
            conn.execute(sa.text(f'DELETE FROM {cls.__tablename__} WHERE id = {broken_id}'))


class OrphanedData:
    class CfgCsvColumn(OrphanedDataBase):
        __tablename__ = 'cfg_csv_column'
        find_orphaned_data_sql = """
                                 SELECT id
                                 FROM cfg_csv_column
                                 WHERE data_source_id NOT IN (SELECT id FROM cfg_data_source)
                                    OR data_source_id NOT IN (SELECT id FROM cfg_data_source_csv)
                                 """

    class CfgDataSourceCsv(OrphanedDataBase):
        __tablename__ = 'cfg_data_source_csv'
        find_orphaned_data_sql = """
                                 SELECT id
                                 FROM cfg_data_source_csv
                                 WHERE id NOT IN (SELECT id FROM cfg_data_source)
                                    OR id NOT IN (SELECT data_source_id FROM cfg_csv_column)
                                 """

    class CfgDataSourceDb(OrphanedDataBase):
        __tablename__ = 'cfg_data_source_db'
        find_orphaned_data_sql = """
                                 SELECT id
                                 FROM cfg_data_source_db
                                 WHERE id NOT IN (SELECT id FROM cfg_data_source)
                                 """

    class CfgDataSource(OrphanedDataBase):
        __tablename__ = 'cfg_data_source'
        find_orphaned_data_sql = """
                                 SELECT id
                                 FROM cfg_data_source
                                 WHERE id NOT IN (SELECT id
                                                  FROM cfg_data_source_db
                                                  UNION ALL
                                                  SELECT id
                                                  FROM cfg_data_source_csv)
                                 """

    class CfgProcessFunctionColumn(OrphanedDataBase):
        __tablename__ = 'cfg_process_function_column'
        find_orphaned_data_sql = """
                                 SELECT id
                                 FROM cfg_process_function_column
                                 WHERE process_column_id NOT IN (SELECT id FROM cfg_process_column)
                                 """

    class CfgProcessColumn(OrphanedDataBase):
        __tablename__ = 'cfg_process_column'
        find_orphaned_data_sql = """
                                 SELECT id
                                 FROM cfg_process_column
                                 WHERE process_id NOT IN (SELECT id FROM cfg_process)
                                    OR (parent_id IS NOT NULL AND parent_id NOT IN (SELECT id FROM cfg_process_column))
                                 """

    class CfgProcess(OrphanedDataBase):
        __tablename__ = 'cfg_process'
        find_orphaned_data_sql = """
                                 SELECT id
                                 FROM cfg_process
                                 WHERE id NOT IN (SELECT process_id FROM cfg_process_column)
                                    OR (parent_id IS NOT NULL AND parent_id NOT IN (SELECT id FROM cfg_process))
                                    OR data_source_id NOT IN (SELECT id FROM cfg_data_source)
                                 """

    class CfgTraceKey(OrphanedDataBase):
        __tablename__ = 'cfg_trace_key'
        find_orphaned_data_sql = """
                                 SELECT cfg_trace_key.id
                                 FROM cfg_trace_key
                                          LEFT JOIN cfg_trace ON cfg_trace_key.trace_id = cfg_trace.id
                                 WHERE cfg_trace_key.trace_id NOT IN (SELECT id FROM cfg_trace)
                                    OR cfg_trace_key.self_column_id NOT IN
                                       (SELECT id
                                        FROM cfg_process_column
                                        WHERE cfg_process_column.process_id = cfg_trace.self_process_id)
                                    OR cfg_trace_key.target_column_id NOT IN
                                       (SELECT id
                                        FROM cfg_process_column
                                        WHERE cfg_process_column.process_id = cfg_trace.target_process_id)
                                 """

    class CfgTrace(OrphanedDataBase):
        __tablename__ = 'cfg_trace'
        find_orphaned_data_sql = """
                                 SELECT id
                                 FROM cfg_trace
                                 WHERE self_process_id NOT IN (SELECT id FROM cfg_process)
                                    OR target_process_id NOT IN (SELECT id FROM cfg_process)
                                    OR id NOT IN (SELECT trace_id FROM cfg_trace_key)
                                 """

    class CfgFilterDetail(OrphanedDataBase):
        __tablename__ = 'cfg_filter_detail'
        find_orphaned_data_sql = """
                                 SELECT id
                                 FROM cfg_filter_detail
                                 WHERE (parent_detail_id IS NOT NULL AND
                                        parent_detail_id NOT IN (SELECT id FROM cfg_filter_detail))
                                    OR filter_id NOT IN (SELECT id FROM cfg_filter)
                                 """

    class CfgFilter(OrphanedDataBase):
        __tablename__ = 'cfg_filter'
        find_orphaned_data_sql = """
                                 SELECT id
                                 FROM cfg_filter
                                 WHERE process_id NOT IN (SELECT id FROM cfg_process)
                                    OR (column_id IS NOT NULL AND column_id NOT IN
                                                                  (SELECT id
                                                                   FROM cfg_process_column
                                                                   WHERE cfg_process_column.process_id = process_id))
                                    OR id NOT IN (SELECT filter_id FROM cfg_filter_detail)
                                    OR (parent_id IS NOT NULL AND parent_id NOT IN (SELECT id FROM cfg_filter))
                                 """

    class CfgVisualization(OrphanedDataBase):
        __tablename__ = 'cfg_visualization'
        find_orphaned_data_sql = """
                                 SELECT id
                                 FROM cfg_visualization
                                 WHERE process_id NOT IN (SELECT id FROM cfg_process)
                                    OR control_column_id NOT IN
                                       (SELECT id
                                        FROM cfg_process_column
                                        WHERE cfg_process_column.process_id = process_id)
                                    OR (
                                     filter_column_id IS NOT NULL AND filter_column_id NOT IN
                                                                      (SELECT id
                                                                       FROM cfg_process_column
                                                                       WHERE cfg_process_column.process_id = process_id)
                                     )
                                 """

    class TProcLink(OrphanedDataBase):
        __tablename__ = 't_proc_link'
        find_orphaned_data_sql = """
                                 SELECT id
                                 FROM t_proc_link
                                 WHERE process_id NOT IN (SELECT id FROM cfg_process)
                                    OR target_process_id NOT IN (SELECT id FROM cfg_process)
                                 """

    @classmethod
    def clean_up_all(cls, conn: Connection):
        while True:
            broken_csv_column_ids = cls.CfgCsvColumn.get(conn)
            cls.CfgCsvColumn.remove(conn, broken_csv_column_ids)
            broken_data_source_csv_ids = cls.CfgDataSourceCsv.get(conn)
            cls.CfgDataSourceCsv.remove(conn, broken_data_source_csv_ids)
            broken_data_source_db_ids = cls.CfgDataSourceDb.get(conn)
            cls.CfgDataSourceDb.remove(conn, broken_data_source_db_ids)
            broken_data_source_ids = cls.CfgDataSource.get(conn)
            cls.CfgDataSource.remove(conn, broken_data_source_ids)
            broken_function_column_ids = cls.CfgProcessFunctionColumn.get(conn)
            cls.CfgProcessFunctionColumn.remove(conn, broken_function_column_ids)
            broken_column_ids = cls.CfgProcessColumn.get(conn)
            cls.CfgProcessColumn.remove(conn, broken_column_ids)
            broken_process_ids = cls.CfgProcess.get(conn)
            cls.CfgProcess.remove(conn, broken_process_ids)
            broken_trace_key_ids = cls.CfgTraceKey.get(conn)
            cls.CfgTraceKey.remove(conn, broken_trace_key_ids)
            broken_trace_ids = cls.CfgTrace.get(conn)
            cls.CfgTrace.remove(conn, broken_trace_ids)
            broken_filter_detail_ids = cls.CfgFilterDetail.get(conn)
            cls.CfgFilterDetail.remove(conn, broken_filter_detail_ids)
            broken_filter_ids = cls.CfgFilter.get(conn)
            cls.CfgFilter.remove(conn, broken_filter_ids)
            broken_visualization_ids = cls.CfgVisualization.get(conn)
            cls.CfgVisualization.remove(conn, broken_visualization_ids)
            broken_proc_link_ids = cls.TProcLink.get(conn)
            cls.TProcLink.remove(conn, broken_proc_link_ids)

            dirty_ids = [
                *broken_csv_column_ids,
                *broken_data_source_csv_ids,
                *broken_data_source_db_ids,
                *broken_data_source_ids,
                *broken_column_ids,
                *broken_function_column_ids,
                *broken_process_ids,
                *broken_trace_key_ids,
                *broken_trace_ids,
                *broken_filter_detail_ids,
                *broken_filter_ids,
                *broken_visualization_ids,
                *broken_proc_link_ids,
            ]
            if not dirty_ids:
                break


already_cleaned_up = False


def remove_orphaned_data():
    global already_cleaned_up
    if already_cleaned_up:
        return

    conn = op.get_bind()
    OrphanedData.clean_up_all(conn)
    already_cleaned_up = True
