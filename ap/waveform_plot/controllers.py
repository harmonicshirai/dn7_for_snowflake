import os

from flask import Blueprint, render_template

from ap.common.services.form_env import get_common_config_data

waveform_plot_blueprint = Blueprint(
    'waveform_plot',
    __name__,
    template_folder=os.path.join('..', 'templates', 'waveform_plot'),
    static_folder=os.path.join('..', 'static', 'waveform_plot'),
    url_prefix='/ap',
)


@waveform_plot_blueprint.route('/wfp')
def index():
    output_dict = get_common_config_data()
    return render_template('waveform_plot.html', **output_dict)
