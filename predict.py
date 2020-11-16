import pandas as pd


def load_data(path):
    try:
        return pd.read_csv(path)
    except UnicodeDecodeError:
        return pd.read_csv(path, encoding='ANSI')


def get_predict(path):
    data = load_data(path)
    print(data.iloc[1])

