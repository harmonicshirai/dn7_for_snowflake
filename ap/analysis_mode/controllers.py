from flask import Blueprint, render_template

analysis_mode_blueprint = Blueprint('analysis_mode', __name__, url_prefix='/ap/analysis_mode')


@analysis_mode_blueprint.route('/', methods=['GET'])
def index():
    """
    Render Data Analysis Mode page
    """
    return render_template('analysis_mode/analysis_mode.html')
