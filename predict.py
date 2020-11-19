import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

from sklearn.model_selection import StratifiedShuffleSplit
from sklearn.impute import SimpleImputer
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder
from sklearn.preprocessing import StandardScaler
from sklearn.compose import ColumnTransformer
from sklearn.metrics import mean_squared_error


class CombinedAttributesAdder(BaseEstimator, TransformerMixin):
    def __init__(self):
        pass

    def fit(self, x, y=None):
        return self

    def transform(self, x):
        # here will added new features
        pass


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
    data['_cat'] = np.ceil(data[id_column] / ceil_k)
    data['_cat'].where(data['_cat'] < sever / ceil_k, sever / ceil_k, inplace=True)

    split = StratifiedShuffleSplit(n_splits=1, test_size=test_ratio, random_state=42)

    for train_index, test_index in split.split(data, data['_cat']):
        strat_train_set = data.loc[train_index]
        strat_test_set = data.loc[test_index]

    for set_ in (strat_train_set, strat_test_set):
        set_.drop('_cat', axis=1, inplace=True)

    return strat_train_set, strat_test_set


def draw_plots_to_research_data(data):  # just to draw some plots
    # 1
    data.plot(kind='scatter', x='year', y='price', alpha=0.1)
    plt.show()
    # 2
    most_pop_cars = data['name'].value_counts()
    most_pop_cars = pd.DataFrame(data={'count': most_pop_cars.values},
                                 index=most_pop_cars.index)
    most_pop_cars.iloc[:40].plot(kind='pie', y='count', figsize=(15, 6))
    plt.show()


def preprocessing(train_set):
    data = train_set.drop('price', axis=1)
    data_labels = train_set['price'].copy()
    data_num = data.drop(['name', 'city'], axis=1)
    num_pipeline = Pipeline([
        ('imputer', SimpleImputer(strategy='median')),
        # ('attribs_adder', CombinedAttributesAdder()),
        ('std_scaler', StandardScaler())
    ])
    num_attribs = list(data_num)
    cat_attribs = ['name', 'city']

    full_pipeline = ColumnTransformer([
        ('num', num_pipeline, num_attribs),
        ('cat', OneHotEncoder(), cat_attribs)
    ])
    prepared_data = full_pipeline.fit_transform(data)
    return prepared_data, data_labels


def get_model(model, prepared_data, data_labels):
    if model == 'LR':
        from sklearn.linear_model import LinearRegression
        # fit with LR and predict
        lin_reg = LinearRegression()
        lin_reg.fit(prepared_data, data_labels)
        return lin_reg

    elif model == 'DTR':
        from sklearn.tree import DecisionTreeRegressor
        # fit with DTR
        tree_reg = DecisionTreeRegressor()
        tree_reg.fit(prepared_data, data_labels)
        return tree_reg

    elif model == 'RFR':
        from sklearn.ensemble import RandomForestRegressor
        # fit with RFR
        forest_reg = RandomForestRegressor()
        forest_reg.fit(prepared_data, data_labels)
        return forest_reg

    else:
        raise AttributeError(f'you cant chose "{model}"')


def get_predict(path):
    data = load_data(path).reset_index()
    data = data.drop('url', axis=1)
    train_set, test_set = split_train_test(data, 0.2, 'price')
    prepared_data, data_labels = preprocessing(train_set)
    model = get_model('RFR', prepared_data, data_labels)

