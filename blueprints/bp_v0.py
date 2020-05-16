from sanic import Blueprint
from sanic.response import json
import pickle
import pandas as pd
from sanic.exceptions import ServerError
from libs import features as feat

bp_v0 = Blueprint('v0', url_prefix='/')


@bp_v0.listener('before_server_start')
async def setup_connection(app, loop):
    global stores_ranked_df, stores_ranked_company_df
    stores_ranked_df = pickle.load(open("views/ranked_stores_ts_quarterly.pckl", "rb"))
    stores_ranked_company_df = pickle.load(open("views/ranked_company_stores_ts_quarterly.pckl", "rb"))

    global configuration
    configuration = app.config





@bp_v0.route('/geoMarkers/<metric>', methods=['GET', 'OPTIONS'])
async def get_markers(request, metric):
    """
    Get store marker data for a specific metric
    E.g:
    {'latitude': '-23.5975251',
      'longitude': '-46.6025457',
      'store_type': 't3',
      'store_id': 'magazine-luiza_0',
      '<metric>': 4.21291335978836,
      '<metric_rank>': 0.96,
      '<metric_eval>': 'Great'}
    :param request:
    :return: JSON
    """
    metric_rank = metric + "_rank"
    if any([m not in stores_ranked_df.columns for m in [metric, metric_rank]]):
        raise ServerError(status_code=400, message=f"Metric does not exist")
    latest_period = stores_ranked_df.date_comment.drop_duplicates().sort_values().iloc[-1]
    variables = ["latitude", "longitude", "store_type", "store_id", metric, metric_rank]
    tmp_df = stores_ranked_df.loc[stores_ranked_df.date_comment == latest_period][variables]
    tmp_df.columns = ["latitude", "longitude", "store_type", "store_id", "metric", "metric_rank"]
    tmp_df["metric_eval"] = tmp_df.metric_rank.apply(lambda x: feat.evaluation_results(x)["result"])
    return json(tmp_df.fillna("Not Available").to_dict("records"))
