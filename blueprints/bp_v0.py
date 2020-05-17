from sanic import Blueprint
from sanic.response import json
import pickle
import pandas as pd
from sanic.exceptions import ServerError
from libs import features as feat

bp_v0 = Blueprint('v0', url_prefix='/')


@bp_v0.listener('before_server_start')
async def setup_connection(app, loop):
    global stores_ranked_df, stores_ranked_company_df, benchmark_df
    stores_ranked_df = pickle.load(open("views/ranked_stores_ts_quarterly.pckl", "rb"))
    stores_ranked_company_df = pickle.load(open("views/ranked_company_stores_ts_quarterly.pckl", "rb"))
    benchmark_df = pickle.load(open("views/benchmarks_ts_quarterly.pckl", "rb"))

    global configuration
    configuration = app.config


@bp_v0.route('/ranked/companies/<metric>', methods=['GET', 'OPTIONS'])
async def get_ranked_companies(request, metric):
    """
    Get companies ranked data for a specific metric
    :param request:
    :return: JSON
    """
    metric_rank = metric + "_rank"
    if any([m not in stores_ranked_df.columns for m in [metric, metric_rank]]):
        raise ServerError(status_code=400, message=f"Metric does not exist")
    tmp_df = feat.get_company_rank(metric, stores_ranked_df)
    return json(tmp_df)


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

    return json(tmp_df.dropna().to_dict("records"))


@bp_v0.route('/detail/stores/<store_id>', methods=['GET', 'OPTIONS'])
async def get_store_detail(request, store_id):
    """
    Get detailed analytics of a store_id
    :param store_id:
    :param request:
    :return: JSON
    """
    if store_id not in stores_ranked_df.store_id.unique():
        raise ServerError(status_code=400, message=f"Invalid Store ID.")

    # Get store general rankings
    general_ranking = feat.get_store_general_rankings(store_id, stores_ranked_df, stores_ranked_company_df)

    # Get store performance
    performance = feat.get_store_performance(store_id, stores_ranked_df, exclude_macro_issues=True)

    # Get store positive and negative highlights
    best_highlight = feat.get_store_best_rankings(store_id, stores_ranked_df)
    worst_highlight = feat.get_store_worse_rankings(store_id, stores_ranked_df)
    general_highlight = feat.get_store_highlights(store_id, stores_ranked_df)

    return json({
        "store_id": store_id,
        "rankings": general_ranking,
        "performance": performance,
        "highlights": {
            "general": general_highlight,
            "best": best_highlight,
            "worst": worst_highlight
        }
    })


@bp_v0.route('/metric/<metric>/store/<store_id>', methods=['GET', 'OPTIONS'])
async def get_metric_ts(request, metric, store_id):
    """
    Get timeseries for store against their benchmark
    :param request:
    :return: JSON
    """
    metric_rank = metric + "_rank"
    if any([m not in stores_ranked_df.columns for m in [metric, metric_rank]]):
        raise ServerError(status_code=400, message=f"Metric does not exist")
    tmp_df = feat.get_store_bechmark_comparison(store_id, metric, stores_ranked_df, benchmark_df).dropna()
    tmp_df.columns = ["metric", "benchmark"]
    tmp_df.reset_index(inplace=True)
    tmp_df.date_comment = tmp_df.date_comment.astype(str)
    return json(tmp_df.to_dict("records"))
