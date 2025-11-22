import sys
import os

# Add project root to path
sys.path.append(os.getcwd())

from ap import create_app

def verify_setup():
    print("Initializing app...")
    try:
        app = create_app()
    except Exception as e:
        print(f"Failed to create app: {e}")
        return

    print("Checking blueprints...")
    if 'analysis_mode' in app.blueprints:
        print("SUCCESS: 'analysis_mode' blueprint found.")
    else:
        print("FAILURE: 'analysis_mode' blueprint NOT found.")
        print(f"Available blueprints: {list(app.blueprints.keys())}")

    print("Checking routes...")
    found_route = False
    for rule in app.url_map.iter_rules():
        if rule.endpoint == 'analysis_mode.index':
            print(f"SUCCESS: Route found for 'analysis_mode.index': {rule}")
            found_route = True
            break
    
    if not found_route:
        print("FAILURE: Route for 'analysis_mode.index' NOT found.")

if __name__ == "__main__":
    verify_setup()
