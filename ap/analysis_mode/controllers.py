import os
from flask import Blueprint, render_template
from ap.common.services.form_env import get_common_config_data

analysis_mode_blueprint = Blueprint(
    'analysis_mode',
    __name__,
    template_folder=os.path.join('..', 'templates', 'analysis_mode'),
    static_folder=os.path.join('..', 'static', 'analysis_mode'),
    url_prefix='/ap/analysis_mode'
)


@analysis_mode_blueprint.route('/', methods=['GET'])
def index():
    """
    Render Data Analysis Mode page
    """
    output_dict = get_common_config_data()
    return render_template('analysis_mode/analysis_mode.html', **output_dict)
