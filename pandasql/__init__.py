from .sqldf import *
import os
import pandas as pd

_ROOT = os.path.abspath(os.path.dirname(__file__))


def get_data(path):
    return os.path.join(_ROOT, 'data', path)


def load_meat():
    filename = get_data("meat.csv")
    df = pd.read_csv(filename, parse_dates=[0])
    return df


def load_births():
    filename = get_data("births_by_month.csv")
    df = pd.read_csv(filename, parse_dates=[0])
    return df
