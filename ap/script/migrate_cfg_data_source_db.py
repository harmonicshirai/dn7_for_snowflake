from ap.common.pydn.dblib import sqlite
from ap.setting_module.models import CfgDataSourceDB

snowflake_role_column = """alter table cfg_data_source_db add snowflake_role text;"""
snowflake_warehouse_column = """alter table cfg_data_source_db add snowflake_warehouse text;"""
snowflake_private_key_file_column = """alter table cfg_data_source_db add snowflake_private_key_file text;"""
snowflake_private_key_file_pwd_column = """alter table cfg_data_source_db add snowflake_private_key_file_pwd text;"""
snowflake_access_token_column = """alter table cfg_data_source_db add snowflake_access_token text;"""
snowflake_authentication_type_column = """alter table cfg_data_source_db add snowflake_authentication_type text;"""


def migrate_cfg_data_source_db(app_db_src):
    app_db = sqlite.SQLite3(app_db_src)
    app_db.connect()

    if not app_db.is_column_existing(CfgDataSourceDB.__table__.name, CfgDataSourceDB.snowflake_role.name):
        app_db.execute_sql(snowflake_role_column)

    if not app_db.is_column_existing(CfgDataSourceDB.__table__.name, CfgDataSourceDB.snowflake_warehouse.name):
        app_db.execute_sql(snowflake_warehouse_column)

    if not app_db.is_column_existing(CfgDataSourceDB.__table__.name, CfgDataSourceDB.snowflake_private_key_file.name):
        app_db.execute_sql(snowflake_private_key_file_column)

    if not app_db.is_column_existing(CfgDataSourceDB.__table__.name, CfgDataSourceDB.snowflake_private_key_file_pwd.name):
        app_db.execute_sql(snowflake_private_key_file_pwd_column)

    if not app_db.is_column_existing(CfgDataSourceDB.__table__.name, CfgDataSourceDB.snowflake_access_token.name):
        app_db.execute_sql(snowflake_access_token_column)

    if not app_db.is_column_existing(CfgDataSourceDB.__table__.name, CfgDataSourceDB.snowflake_authentication_type.name):
        app_db.execute_sql(snowflake_authentication_type_column)

    app_db.disconnect()
