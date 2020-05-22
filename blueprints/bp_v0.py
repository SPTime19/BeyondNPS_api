from sanic import Blueprint
from sanic.response import json
import pickle
import numpy as np
from sanic.exceptions import ServerError
from libs import features as feat

bp_v0 = Blueprint('v0', url_prefix='/')


@bp_v0.listener('before_server_start')
async def setup_connection(app, loop):
    global stores_ranked_df, stores_ranked_company_df, benchmark_df, stores_performance_agg_view
    stores_ranked_df = pickle.load(open("views/ranked_stores_ts_quarterly.pckl", "rb"))
    stores_ranked_company_df = pickle.load(open("views/ranked_company_stores_ts_quarterly.pckl", "rb"))
    benchmark_df = pickle.load(open("views/benchmarks_ts_quarterly.pckl", "rb"))
    stores_performance_agg_view = pickle.load(open("views/stores_performance_agg_view.pckl", "rb"))

    global configuration
    configuration = app.config


@bp_v0.route('/ranked/companies/<metric>', methods=['GET', 'OPTIONS'])
async def get_ranked_companies(request, metric):
    """
    Get companies ranked data for a specific metric
    :param request:
    :return: JSON
    """
    metric_rank = feat.format_issues_columns(metric) + "_rank"
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
    metric_rank = feat.format_issues_columns(metric) + "_rank"
    if any([m not in stores_ranked_df.columns for m in [metric, metric_rank]]):
        raise ServerError(status_code=400, message=f"Metric does not exist")

    latest_period = stores_ranked_df.date_comment.drop_duplicates().sort_values().iloc[-1]
    variables = ["latitude", "longitude", "store_type", "store_id", metric, metric_rank]

    tmp_df = stores_ranked_df.loc[stores_ranked_df.date_comment == latest_period][variables]
    tmp_df.columns = ["latitude", "longitude", "store_type", "store_id", "metric", "metric_rank"]
    tmp_df["metric_eval"] = tmp_df.metric_rank.apply(lambda x: feat.evaluation_results(x)["result"])

    return json(tmp_df.dropna().to_dict("records"))


@bp_v0.route('/geoMarkers/<metric>/company/<company_id>', methods=['GET', 'OPTIONS'])
async def get_markers_company(request, metric, company_id):
    """
    Get store marker data for a specific metric and company
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
    metric_rank = feat.format_issues_columns(metric) + "_rank"
    if any([m not in stores_ranked_df.columns for m in [metric, metric_rank]]):
        raise ServerError(status_code=400, message=f"Metric does not exist")

    latest_period = stores_ranked_df.date_comment.drop_duplicates().sort_values().iloc[-1]
    variables = ["latitude", "longitude", "store_type", "store_id", metric, metric_rank]

    tmp_df = \
        stores_ranked_df.loc[
            (stores_ranked_df.date_comment == latest_period) & (stores_ranked_df.company == company_id)][
            variables]
    tmp_df.columns = ["latitude", "longitude", "store_type", "store_id", "metric", "metric_rank"]
    tmp_df["metric_eval"] = tmp_df.metric_rank.apply(lambda x: feat.evaluation_results(x)["result"])

    return json(tmp_df.dropna().to_dict("records"))


@bp_v0.route('/metric/<metric>/store/<store_id>', methods=['GET', 'OPTIONS'])
async def get_metric_ts(request, metric, store_id):
    """
    Get timeseries for store against their benchmark
    :param request:
    :return: JSON
    """
    metric_rank = feat.format_issues_columns(metric) + "_rank"
    if any([m not in stores_ranked_df.columns for m in [metric, metric_rank]]):
        raise ServerError(status_code=400, message=f"Metric does not exist")
    tmp_df = feat.get_store_bechmark_comparison(store_id, metric, stores_ranked_df, benchmark_df).dropna()
    tmp_df.columns = ["metric", "benchmark"]
    tmp_df.reset_index(inplace=True)
    tmp_df.date_comment = tmp_df.date_comment.astype(str)
    return json(tmp_df.to_dict("records"))


@bp_v0.route('/metric/<metric>/company/<company_id>', methods=['GET', 'OPTIONS'])
async def get_company_metric_ts(request, metric, company_id):
    """
    Get timeseries for company against their benchmark
    :param request:
    :return: JSON
    """
    metric_rank = feat.format_issues_columns(metric) + "_rank"
    if any([m not in stores_ranked_df.columns for m in [metric, metric_rank]]):
        raise ServerError(status_code=400, message=f"Metric does not exist")
    tmp_df = feat.get_company_bechmark_comparison(company_id, metric, stores_ranked_df)
    return json(tmp_df)


@bp_v0.route('/metric/distribution/<metric>/company/<company_id>/<dt_com>', methods=['GET', 'OPTIONS'])
async def get_company_metric_distribution(request, metric, company_id, dt_com):
    """
    Get distribution for company metric against their benchmark on a specific date
    :param request:
    :return: JSON
    """
    if any([m not in stores_ranked_df.columns for m in [metric]]):
        raise ServerError(status_code=400, message=f"Metric does not exist")

    if dt_com == "latest":  # Get the latest period
        dt_com = stores_ranked_df.date_comment.drop_duplicates().sort_values().iloc[-1]

    tmp_df = feat.get_metric_distribution(metric=metric,
                                          company_id=company_id,
                                          dt_com=dt_com,
                                          store_ts=stores_ranked_df, bins=10)
    return json(tmp_df)


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


@bp_v0.route('/detail/company/<company_id>', methods=['GET', 'OPTIONS'])
async def get_company_details(request, company_id):
    """
    Get detailed analytics of a company_id
    :param request:
    :return: JSON
    """
    if company_id not in stores_ranked_df.company.unique():
        raise ServerError(status_code=400, message=f"Invalid Company ID.")

    number_stores = feat.get_number_of_stores(company_id, stores_ranked_df)
    ranked_companies = feat.get_ranked_companies(stores_ranked_df)

    if company_id in ranked_companies:
        company_rank = ranked_companies[company_id]
    else:
        company_rank = "Not Available"
    best_worst_stores = feat.get_best_worst_store(company_id, stores_ranked_df)
    store_performants = feat.get_company_general_performance(company_id, stores_performance_agg_view)
    return json({
        "company_id": company_id,
        "num_stores": number_stores,
        "company_rank": company_rank,
        "highlight_stores": best_worst_stores,
        "perfomants": store_performants
    })
