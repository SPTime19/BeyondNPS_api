from sanic import Blueprint
from sanic.response import json

bp_v0 = Blueprint('v0', url_prefix='/')


@bp_v0.listener('before_server_start')
async def setup_connection(app, loop):
    global configuration
    configuration = app.config


@bp_v0.route('/process', methods=['POST', 'OPTIONS'])
async def process_text(request):
    """
    :param request:
    :return:
    """
    # text_input = general.get_parameter(request, "input")
    # category = general.get_parameter(request, "category")
    debug = True if request.json.get("debug") else False

    return json({})
