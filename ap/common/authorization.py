from functools import wraps

from flask import abort, current_app, request

from ap import DISABLE_CONFIG_FROM_EXTERNAL_KEY, is_admin_request


# naming login_required for implement login in the future
def login_required(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        is_authorized = current_app.config.get(DISABLE_CONFIG_FROM_EXTERNAL_KEY) is False or is_admin_request(request)
        if not is_authorized:
            return abort(403)
        return fn(*args, **kwargs)

    return wrapper
