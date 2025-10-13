import json
import logging
from pathlib import Path
from typing import Optional

import pandas as pd

from ap.api.setting_module.services.software_workshop_etl_services import (
    SNOWFLAKE_SOFTWARE_WORKSHOP_DEF,
    SoftwareWorkshopDef,
)
from ap.common.common_utils import get_data_path, read_feather_file
from ap.common.pandas_helper import pandas_concat_with_filter_empty
from ap.common.pydn.dblib.db_proxy_read_only import ReadOnlyDbProxy
from ap.etl.pull.common import PullDataType
from ap.etl.transform import BaseTransformer, TransformData
from ap.setting_module.models import CfgDataSource

logger = logging.getLogger(__name__)


class SoftwareWorkshopJsonTransformer(BaseTransformer):
    """Transformer for converting software workshop snowflake json data"""

    def __init__(self, expected_json_columns: list[str], expected_columns_from_json: list[str]):
        self.expected_json_columns = expected_json_columns
        self.expected_columns_from_json = expected_columns_from_json

    def transform(self, data: TransformData) -> TransformData:
        """
        >>> df = pd.DataFrame(
        ...     {
        ...         'SERIAL_NO': ['2', '1'],
        ...         'MEASUREMENTS': [
        ...             '[{"code": "one", "value": 11, "unit": "kN"}, {"code": "two", "value": 21}]',
        ...             '[{"code": "one", "value": 12}, {"code": "two", "value": 22}]',
        ...         ],
        ...         'STRING_MEASUREMENTS': [
        ...             '[{"code": "one_string", "value": "a1"}, {"code": "two_string", "value": "b1"}]',
        ...             '[{"code": "one_string", "value": "a2"}, {"code": "two_string", "value": "b2"}]',
        ...         ],
        ...         'CHILD_EQUIP_ID': ['x', 'y'],
        ...     }
        ... )
        >>> transform_data = TransformData(df=df)
        >>> transformer = SoftwareWorkshopJsonTransformer(
        ...     expected_json_columns=['MEASUREMENTS', 'STRING_MEASUREMENTS'],
        ...     expected_columns_from_json=['code', 'value', 'unit'],
        ... )
        >>> transformer.transform(transform_data).df
                 code value unit SERIAL_NO CHILD_EQUIP_ID
        0         one    11   kN         2              x
        1         two    21  NaN         2              x
        2  one_string    a1  NaN         2              x
        3  two_string    b1  NaN         2              x
        4         one    12  NaN         1              y
        5         two    22  NaN         1              y
        6  one_string    a2  NaN         1              y
        7  two_string    b2  NaN         1              y
        """

        if len(data.df) == 0:
            logger.error(f'{self.__class__}: No data found')
            return data

        df_list = []

        non_json_keys: list[str] = [col for col in data.df.columns if col not in self.expected_json_columns]

        for _, row in data.df.iterrows():
            df_data = pandas_concat_with_filter_empty(
                [
                    self.df_from_json_array(
                        json_str=row.get(json_key),
                        expected_columns=self.expected_columns_from_json,
                    )
                    for json_key in self.expected_json_columns
                ],
            )

            if len(df_data) > 0:
                # push back other data from non-measurement-keys
                for col in non_json_keys:
                    df_data[col] = row.get(col)
                df_list.append(df_data)

        if len(df_list) > 0:
            df_result = pd.concat(df_list, ignore_index=True)
        else:
            df_result = pd.DataFrame(columns=non_json_keys + self.expected_columns_from_json)

        return data.with_df(df_result)

    @classmethod
    def df_from_json_array(cls, *, json_str: Optional[str], expected_columns: list[str]) -> pd.DataFrame:
        """Create dataframe from a json string containing list of raw data with expected columns.

        >>> s = '[{"code": "one", "value": 11, "unit": "kN"}, {"code": "two", "value": 21}]'
        >>> SoftwareWorkshopJsonTransformer.df_from_json_array(json_str=s, expected_columns=['code', 'value', 'unit'])
          code  value unit
        0  one     11   kN
        1  two     21  NaN
        >>> SoftwareWorkshopJsonTransformer.df_from_json_array(json_str=s, expected_columns=['one', 'two', 'three'])
           one  two  three
        0  NaN  NaN    NaN
        1  NaN  NaN    NaN
        """
        if json_str is None:
            return pd.DataFrame(columns=expected_columns)
        try:
            data = json.loads(json_str)
            return pd.DataFrame(data, columns=expected_columns)
        except Exception as e:
            logger.exception(e)
            return pd.DataFrame(columns=expected_columns)


class SoftwareWorkshopSnowflakeAddMasterDataTransformer(BaseTransformer):
    def __init__(self, data_source_id: int, process_factid: str):
        self.data_source_id = data_source_id
        self.process_factid = process_factid

    def transform(self, data: TransformData) -> TransformData:
        """Add master column to software workshop snowflake data"""

        data_source = CfgDataSource.query.get(self.data_source_id)
        with ReadOnlyDbProxy(data_source) as db_instance:
            sql = SNOWFLAKE_SOFTWARE_WORKSHOP_DEF.get_master_query(child_equip_ids=[self.process_factid])
            _, rows = db_instance.run_sql(sql)

        df_master = pd.DataFrame(rows).drop_duplicates()
        df_transaction = data.df
        master_columns = [col for col in df_master if col not in df_transaction]

        if len(df_master) == 0:
            logger.error(f'{self.__class__}: no master data for child_equip_id: `{self.process_factid}`')
            df_transaction[master_columns] = None
        else:
            if len(df_master) > 1:
                logger.error(f'{self.__class__}: multiple master data for child_equip_id: `{self.process_factid}`')
            for column in master_columns:
                df_transaction[column] = df_master.loc[0, column]

        return data.with_df(df_transaction)


class SoftwareWorkshopSnowflakeAddMasterDataTransformerLocal(BaseTransformer):
    def __init__(self, process_id: int):
        self.master_path = Path(get_data_path()) / str(process_id) / f'{PullDataType.MASTER.name}.feather'

    def transform(self, data: TransformData) -> TransformData:
        """Add master column to software workshop snowflake data"""
        df_master = read_feather_file(self.master_path).drop_duplicates().reset_index(drop=True)
        df_transaction = data.df
        master_columns = [col for col in df_master if col not in df_transaction]

        if len(df_master) == 0:
            df_transaction[master_columns] = None
        else:
            for column in master_columns:
                df_transaction[column] = df_master.loc[0, column]

        return data.with_df(df_transaction)


class SoftwareWorkshopReplaceCodeToNameTransformer(BaseTransformer):
    def __init__(
        self,
        software_workshop_def: SoftwareWorkshopDef,
        data_source_id: int,
        process_factid: str,
        code: str,
        add_missing_columns: bool,
    ):
        self.software_workshop_def = software_workshop_def
        self.data_source_id = data_source_id
        self.process_factid = process_factid
        self.code = code
        self.add_missing_columns = add_missing_columns

    def transform(self, data: TransformData) -> TransformData:
        """Rename all values in `self.code` to `name`"""

        code_name_mapping = self.software_workshop_def.get_code_name_mapping(self.data_source_id, self.process_factid)

        df = data.df
        if self.code not in df:
            df[self.code] = None
        df = df.sort_values(by=self.code, ascending=False)
        df[self.code] = df[self.code].map(code_name_mapping)

        # add code/name that isn't included in `df`
        if self.add_missing_columns:
            non_existed_names = set(code_name_mapping.values()) - set(df[self.code].unique())
            if len(non_existed_names) > 0:
                logger.warning(f'Add missed columns {non_existed_names} to software workshop dataframe')
                df_copy = df.copy(deep=False).iloc[0:0]
                df_copy[self.code] = sorted(non_existed_names)
                df = pd.concat([df, df_copy], ignore_index=True)

        return data.with_df(df)


class SoftwareWorkshopReplaceCodeToNameTransformerLocal(BaseTransformer):
    def __init__(self, process_id: int, code: str, meas_item_code: str, meas_item_name: str):
        self.code_path = Path(get_data_path()) / str(process_id) / f'{PullDataType.CODE.name}.feather'
        self.code = code
        self.meas_item_code = meas_item_code
        self.meas_item_name = meas_item_name

    def transform(self, data: TransformData) -> TransformData:
        """Rename all values in `self.code` to `name`"""

        df_code = read_feather_file(self.code_path)
        code_name_mapping = dict(zip(df_code[self.meas_item_code], df_code[self.meas_item_name]))

        df = data.df
        if self.code not in df:
            df[self.code] = None
        df = df.sort_values(by=self.code, ascending=False)
        df[self.code] = df[self.code].map(code_name_mapping)
        return data.with_df(df)
