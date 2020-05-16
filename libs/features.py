import pandas as pd
from datetime import datetime
import logging
from typing import *


def get_store_ranking(store_id: str, metric: str, ranked_ts: "pd.DataFrame", dt_period: "datetime") -> float:
    """
    Get the ranking value for dt_period, metric, store_id on ranked_ts
    :return: ranking float
    """
    return ranked_ts.loc[(ranked_ts.store_id == store_id) & (ranked_ts.date_comment == dt_period)][metric].iloc[0]


def evaluation_results(rank):
    if rank is not None:
        if rank >= 0.8:
            result = "Great"
        elif rank <= 0.3:
            result = "Poor"
        else:
            result = "Good"
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


def get_store_performance(store_id: str, type_ts: "pd.DataFrame"):
    """
    Get positive and negative aspects from a company
    :param store_id: store_id
    :param type_ts: ranked pandas dataFrame by size type
    :return:
    """
    issues_metrics = [col for col in type_ts.columns if "issues" in col and "rank" not in col]
    performance_df = type_ts.loc[type_ts.store_id == store_id].set_index("date_comment")[issues_metrics].diff()
    report_apects = {"positive": [], "negative": []}

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


def get_store_rankings(store_id: str, type_ts: "pd.DataFrame", n=3, by="best") -> "pd.DataFrame":
    """
    Get stores top/lowest N ranking metrics
    :param store_id: store id
    :param type_ts: ranked time series dataFrame
    :param n: Number of top/lowest N rankings
    :param by: [best/worst] to get the top/lowest N rankings
    :return: dataFrame with rank value and performance indicator
    """

    def format_display_issues_rank(col):
        return col.replace("_rank", "")

    def get_performance_label(rank):
        if rank >= 0.8:
            return "Good"
        elif rank <= 0.2:
            return "Bad"
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
    tmp_ts = tmp_ts.reset_index(drop=True).transpose().sort_values(by=0, ascending=ascend_rank).dropna(axis=0).iloc[0:n]
    tmp_ts.columns = ["rank_val"]
    tmp_ts.index = [format_display_issues_rank(i) for i in tmp_ts.index.values]

    # Limit to get only average and bad metrics
    if by == "best":
        tmp_ts = tmp_ts.loc[tmp_ts.rank_val > 0.4]
    else:
        tmp_ts = tmp_ts.loc[tmp_ts.rank_val < 0.7]

    # Create perf label
    tmp_ts["performace"] = tmp_ts.rank_val.apply(lambda x: get_performance_label(x))

    return tmp_ts


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
    return type_ts.groupby("company").mean().sort_values(by=ranked_metric, ascending=False)[ranked_metric].dropna().to_dict()
