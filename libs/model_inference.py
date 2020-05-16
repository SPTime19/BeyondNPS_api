import numpy as np
from typing import *
from libs.text_formatting import get_tokens_from_RA_df
import pandas as pd
from tqdm import tqdm


def get_review_embbedings(review_seq: List[str], model: "gensim.model"):
    try:
        return np.array([model.wv[w] for w in review_seq]).mean(axis=0)
    except:
        return None


def get_text_feature_df(reviews_df, embedding_model) -> "pd.DataFrame":
    # Get features
    texts_vecs = [get_review_embbedings(seq, embedding_model) for seq in tqdm(get_tokens_from_RA_df(reviews_df))]
    feat_num = texts_vecs[0].shape[0]
    indexes = pd.Series(texts_vecs)
    indexes = indexes[~indexes.isna()].index

    # Remove nans and maintain index order to df
    feat_df = pd.DataFrame([i for i in texts_vecs if not isinstance(i, type(np.nan))],
                           columns=[f"feat_{i}" for i in range(feat_num)], index=indexes)

    return feat_df
