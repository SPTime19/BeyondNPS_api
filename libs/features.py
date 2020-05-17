import logging
from datetime import datetime
from typing import *

import pandas as pd


def get_store_bechmark_comparison(store_id: str, metric: str, stores_ts: "pd.DataFrame",
                                  benchmark_ts: "pd.DataFrame") -> pd.DataFrame:
    """
    Provides a dataframe with store_id x benchmark on a particular metric
    """
    # get benchmark class
    store_class_map = stores_ts[["store_id", "store_type"]].drop_duplicates().set_index("store_id").to_dict()[
        "store_type"]
    store_class = store_class_map[store_id]

    # Filter and agg with benchmark data
    tmp_df = stores_ts.loc[(stores_ts.store_id == store_id)][["date_comment", metric]]
    tmp_df = tmp_df.merge(benchmark_ts.loc[(benchmark_ts.store_type == store_class)][["date_comment", metric]],
                          left_on="date_comment", right_on="date_comment", suffixes=('_store', '_benchmark'))
    tmp_df.index = tmp_df.date_comment
    tmp_df = tmp_df.drop("date_comment", axis=1)
    return tmp_df


def format_metric_display(metric):
    """
    Format metric name display
    """
    return " ".join(metric.split("_"))


def get_store_ranking(store_id: str, metric: str, ranked_ts: "pd.DataFrame", dt_period: "datetime") -> float:
    """
    Get the ranking value for dt_period, metric, store_id on ranked_ts
    :return: ranking float
    """
    return ranked_ts.loc[(ranked_ts.store_id == store_id) & (ranked_ts.date_comment == dt_period)][metric].iloc[0]


def get_store_highlights(store_id: str, type_ts: "pd.DataFrame") -> List[Dict[str, Any]]:
    """
    Get Store Highlights Ranking
    :param store_id: store id
    :param type_ts: ranked store df
    :return: [{'index': 'product_issues_quality',
              'rank_val': 0.21739130434782608,
              'performance': 'Average'}...]
    """
    return get_store_rankings(store_id, type_ts, 7)


def evaluation_results(rank):
    if rank is not None:
        if rank >= 0.95:
            result = "Great"
        elif rank >= 0.7:
            result = "Good"
        elif rank <= 0.3:
            result = "Poor"
        else:
            result = "Average"
        return {"result": result, "rank": round(rank * 100, 2)}
    return "Not Available"


def get_store_main_rankings(store_id: str, type_ts, company_ts) -> dict:
    latest_period = type_ts.date_comment.drop_duplicates().sort_values().iloc[-1]
    rank_metric = "rating_rank"
    within_type = evaluation_results(get_store_ranking(store_id, rank_metric, type_ts, latest_period))
    within_company = evaluation_results(get_store_ranking(store_id, rank_metric, company_ts, latest_period))
    return {"type_ranking": within_type, "company_ranking": within_company}


def get_general_ranking(store_id: str, ranked_ts: "pd.DataFrame", dt_period: "datetime") -> Optional["pd.DataFrame"]:
    """
    Average ranking over all issue rankings
    """
    try:
        ranking_vars = [col for col in ranked_ts.columns if "_rank" in col]
        return \
            ranked_ts.loc[(ranked_ts.store_id == store_id) & (ranked_ts.date_comment == dt_period)][ranking_vars].mean(
                axis=1).iloc[0]
    except IndexError:
        logging.error(f"Store '{store_id}' does not have enough data...")
    except Exception as err:
        logging.error(f"Error computing general ranking @{store_id}")
        logging.error(err)
        return None


def get_store_general_rankings(store_id: str, type_ts, company_ts) -> dict:
    """
    Get latest store average ranking (within its type and own company)
    """
    latest_period = type_ts.date_comment.drop_duplicates().sort_values()
    if latest_period.size > 0:
        latest_period = latest_period.iloc[-1]
        within_type = evaluation_results(get_general_ranking(store_id, type_ts, latest_period))
        within_company = evaluation_results(get_general_ranking(store_id, company_ts, latest_period))

        if within_type is not None and within_company is not None:
            return {"type_ranking": within_type, "company_ranking": within_company}

    return {"type_ranking": {"result": "Not Enough Data"}, "company_ranking": {"result": "Not Enough Data"}}


def format_issues_columns(col):
    """
    Easy formatting for issue col names
    :return:
    """
    return "_".join(col.split(" ")).lower()


def get_store_performance(store_id: str, type_ts: "pd.DataFrame", exclude_macro_issues: bool = False):
    """
    Get positive and negative aspects from a company
    :param store_id: store_id
    :param type_ts: ranked pandas dataFrame by size type
    :return:
    """
    issues_metrics = [col for col in type_ts.columns if "issues" in col and "rank" not in col]

    # Remove macro issue tags from the filter
    if exclude_macro_issues:
        issues_metrics = [col for col in issues_metrics if col not in ["product_issues", "business_issues"]]

    performance_df = type_ts.loc[type_ts.store_id == store_id].set_index("date_comment")[issues_metrics].diff()
    report_apects = {"positive": [], "negative": []}

    # Labeling
    for issue in issues_metrics:
        if all(performance_df[issue].iloc[-2:] < 0):
            report_apects["positive"].append(
                {"metric": format_issues_columns(issue), "performance": "Consistently Improving"})
        elif all(performance_df[issue].iloc[-1:] < 0):
            report_apects["positive"].append({"metric": format_issues_columns(issue), "performance": "Improving"})
        elif all(performance_df[issue].iloc[-1:] > 0):
            report_apects["negative"].append({"metric": format_issues_columns(issue), "performance": "Worsening"})
        elif all(performance_df[issue].iloc[-2:] > 0):
            report_apects["negative"].append(
                {"metric": format_issues_columns(issue), "performance": "Consistently Worsening"})
    return report_apects


def get_store_rankings(store_id: str, type_ts: "pd.DataFrame", n=3, by=None) -> List[Dict[str, Any]]:
    """
    Get stores top/lowest N ranking metrics
    :param store_id: store id
    :param type_ts: ranked time series dataFrame
    :param n: Number of top/lowest N rankings
    :param by: [best/worst/None] to get the top/lowest N rankings
    :return: dataFrame with rank value and performance indicator
    """

    def format_display_issues_rank(col):
        return col.replace("_rank", "")

    def get_performance_label(rank):
        if rank >= 0.95:
            return "Great"
        elif rank >= 0.8:
            return "Good"
        elif rank <= 0.2:
            return "Poor"
        else:
            return "Average"

    if by == "best":
        ascend_rank = False
    else:
        ascend_rank = True

    generic_issues = ["product_issues_rank", "business_issues_rank"]  # Remove general products/business issues
    ranking_vars = [col for col in type_ts.columns if "_rank" in col and col not in generic_issues]
    latest_period = type_ts.date_comment.drop_duplicates().sort_values().iloc[-1]

    # Filter ts dataFrame
    tmp_ts = type_ts.loc[(type_ts.date_comment == latest_period) & (type_ts.store_id == store_id)][ranking_vars]
    if tmp_ts.size > 0:
        tmp_ts = tmp_ts.reset_index(drop=True).transpose().sort_values(by=0, ascending=ascend_rank).dropna(axis=0).iloc[
                 0:n]
        tmp_ts.columns = ["rank_val"]
        tmp_ts.index = [format_display_issues_rank(i) for i in tmp_ts.index.values]

        # Limit to get only average and bad metrics
        if by == "best":
            tmp_ts = tmp_ts.loc[tmp_ts.rank_val > 0.4]
        elif by == "worst":
            tmp_ts = tmp_ts.loc[tmp_ts.rank_val < 0.7]

        # Create perf label
        tmp_ts["performance"] = tmp_ts.rank_val.apply(lambda x: get_performance_label(x))

        return tmp_ts.reset_index().to_dict("record")
    else:
        return [{'performance': None, 'rank_val': None, "index": None}]


def get_store_worse_rankings(store_id: str, type_ts: "pd.DataFrame", n=3):
    return get_store_rankings(store_id, type_ts, n, by="worst")


def get_store_best_rankings(store_id: str, type_ts: "pd.DataFrame", n=3):
    return get_store_rankings(store_id, type_ts, n, by="best")


def get_company_rank(metric: str, type_ts: "pd.DataFrame") -> dict:
    """
    Get the average rank companies rank on a metric
    Eg.
    {'sylvia-design': 0.3849835318068351,
     'mobly': 0.39418091356099483}
    """
    ranked_metric = metric + "_rank"
    return type_ts.groupby("company").mean().sort_values(by=ranked_metric, ascending=False)[
        ranked_metric].dropna().to_dict()
