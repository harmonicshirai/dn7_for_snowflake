def create_module(app, **kwargs):
    from .controllers import waveform_plot_blueprint

    app.register_blueprint(waveform_plot_blueprint)
