from typing import Union

from ap.common.pydn.dblib.db_proxy import DbProxy
from ap.setting_module.models import CfgDataSource, CfgDataSourceDB


class ReadOnlyDbProxy(DbProxy):
    """
    An interface for client to connect to many type of database
    """

    def __init__(self, data_source: Union[CfgDataSource, CfgDataSourceDB, int], force_connect=False):
        """
        cfg_data_source_db: CfgDataSourceDB object
        """
        super().__init__(data_source, read_only=True, force_connect=force_connect)

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.db_instance.disconnect()
        return False

    def _get_db_instance(self):
        return super()._get_db_instance()
