import string
import re
import unicodedata
from typing import *


def normalize_text(text):
    """
    Strip accents and lower text string
    :param text: (str) text to be cleaned
    :return: (str) cleaned text
    """
    text = strip_accents(text)
    text = text.lower().strip()
    text = text.translate(str.maketrans("", "", string.punctuation))
    return text


def strip_accents(s):
    return ''.join(c for c in unicodedata.normalize('NFD', s) if unicodedata.category(c) != 'Mn')


def remove_numbers(text):
    return re.sub(r'\b[0-9]+\b', '', text)


def tokenize(data, sep=None):
    if sep is not None:
        return data.split(sep)
    return data.split()


def get_tokens_from_RA_df(df) -> List[List[str]]:
    def format_text_input(row):
        return f"{remove_numbers(normalize_text(row['title']))} {remove_numbers(normalize_text(row['description']))}"
    # Get input tokens
    texts = df.apply(format_text_input, axis=1)
    tokens_sq = [tokenize(i) for i in texts.values]
    return tokens_sq
