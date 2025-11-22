import sys
import os

# Add project root to path
sys.path.append(os.getcwd())

try:
    import snowflake.connector
    print("SUCCESS: snowflake.connector imported")
except ImportError as e:
    print(f"FAILURE: Could not import snowflake.connector: {e}")

try:
    import snowflake.sqlalchemy
    print("SUCCESS: snowflake.sqlalchemy imported")
except ImportError as e:
    print(f"FAILURE: Could not import snowflake.sqlalchemy: {e}")

try:
    # Check if the 'snowflake' package is the ID generator or something else
    import snowflake
    print(f"INFO: 'snowflake' package version: {getattr(snowflake, '__version__', 'unknown')}")
    if hasattr(snowflake, 'make_snowflake'):
        print("INFO: 'snowflake' package seems to be the ID generator.")
    else:
        print("INFO: 'snowflake' package contents:", dir(snowflake))
except ImportError:
    print("INFO: 'snowflake' package not installed (this is good if we want to avoid confusion)")

# Try to instantiate the Snowflake class from our codebase
try:
    from ap.common.pydn.dblib.snowflake import Snowflake
    print("SUCCESS: Imported ap.common.pydn.dblib.snowflake.Snowflake")
except ImportError as e:
    print(f"FAILURE: Could not import ap.common.pydn.dblib.snowflake.Snowflake: {e}")
except Exception as e:
    print(f"FAILURE: Error during import of ap.common.pydn.dblib.snowflake: {e}")
