from sanic import Sanic
from blueprints.bp_v0 import bp_v0
from fire import Fire
import yaml
from sanic_cors import CORS, cross_origin
import os

class Dashboard:

    def __init__(self, config_path="config.yaml", debug=False):
        self.debug = debug
        self.config = yaml.load(open(config_path))
        self.app = self._build_server(self.config)

    @staticmethod
    def _build_server(config):
        app = Sanic("BNP_API")
        app.config.update(config)
        app.blueprint(bp_v0)
        CORS(app)
        return app

    def run_server(self):
        self.app.run(host=self.config['APP']['host'],
                     port=int(self.config['APP']['port']),
                     access_log=True,
                     workers=int(os.environ.get('WEB_CONCURRENCY', 1)))


if __name__ == '__main__':
    Fire(Dashboard)
