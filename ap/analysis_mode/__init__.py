def create_module(app, **kwargs):
    from .controllers import analysis_mode_blueprint

    app.register_blueprint(analysis_mode_blueprint)
