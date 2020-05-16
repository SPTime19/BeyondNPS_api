from sanic import Blueprint
from sanic.exceptions import ServerError, InvalidUsage
from sanic.response import json

bp_admin = Blueprint('admin')


@bp_admin.route('/health', methods=["GET"])
async def bp_healthcheck(request):
    try:
        resp = {"STATUS": "Rock and Roll"}
        return json(resp)
    except TypeError as err:
        raise ServerError(str(err), status_code=400)
    except InvalidUsage as err:
        raise ServerError(str(err), status_code=400)
    except Exception as err:
        raise ServerError("Internal error.", status_code=500)
