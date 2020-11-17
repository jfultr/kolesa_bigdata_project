import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

from sklearn.model_selection import StratifiedShuffleSplit


def load_data(path):
    try:
        return pd.read_csv(path)
    except UnicodeDecodeError:
        return pd.read_csv(path, encoding='ANSI')


def split_train_test(data, test_ratio, id_column):
    strat_train_set = None
    strat_test_set = None
    ceil_k = 2500000  # Коэффицент маштабирования, чтобы ограничить количество страт
    sever = 15000000  # Граница ценны автомобиля после которой все автомобили совмещаються в одну страту
    data['_cat'] = np.ceil(data[id_column]/ceil_k)
    data['_cat'].where(data['_cat'] < sever/ceil_k, sever/ceil_k, inplace=True)
    split = StratifiedShuffleSplit(n_splits=1, test_size=test_ratio, random_state=42)

    for train_index, test_index in split.split(data, data['_cat']):
        strat_train_set = data.loc[train_index]
        strat_test_set = data.loc[test_index]

    for set_ in (strat_train_set, strat_test_set):
        set_.drop('_cat', axis=1, inplace=True)

    return strat_train_set, strat_test_set


def get_predict(path):
    data = load_data(path).reset_index()
    train_set, test_set = split_train_test(data, 0.2, 'price')




