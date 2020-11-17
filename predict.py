import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import hashlib


def load_data(path):
    try:
        return pd.read_csv(path)
    except UnicodeDecodeError:
        return pd.read_csv(path, encoding='ANSI')


def test_set_check(identifier, test_ratio):
    return hashlib.md5(np.int64(identifier)).digest()[-1] < 256 * test_ratio


def split_train_test(data, test_ratio, id_column):
    ids = data[id_column]
    in_test_set = ids.apply(lambda id_: test_set_check(id_, test_ratio))
    return data.loc[~in_test_set], data.loc[in_test_set]


def get_predict(path):
    data = load_data(path).reset_index()
    train_set, test_set = split_train_test(data, 0.2, 'index')

