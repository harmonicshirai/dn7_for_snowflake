import sys
import os

# Add project root to path
sys.path.append(os.getcwd())

from ap import create_app

def verify_setup():
    print("Initializing app...")
    try:
        app = create_app('config.ProdConfig')
        from ap.common.common_utils import bundle_assets
        with app.app_context():
            bundle_assets(app)
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
        return

    print("Checking page rendering...")
    with app.test_client() as client:
        try:
            response = client.get('/ap/analysis_mode/')
            if response.status_code == 200:
                content = response.data.decode('utf-8')
                if 'const procJsons = [' in content:
                    print("SUCCESS: Page rendered with procJsons.")
                else:
                    print("WARNING: Page rendered but procJsons might be empty or format changed.")
                    # print(content[:500]) # Debug
            else:
                print(f"FAILURE: Page returned status code {response.status_code}")
                try:
                    import json
                    data = json.loads(response.data.decode('utf-8'))
                    print(f"Error Message: {data.get('message')}")
                except:
                    print(f"Response body: {response.data.decode('utf-8')}")
        except Exception as e:
            print(f"FAILURE: Request failed: {e}")

if __name__ == "__main__":
    verify_setup()
