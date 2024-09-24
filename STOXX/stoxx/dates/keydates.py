import pandas as pd
from stoxx import dates
import os

def reviewdates():
    return pd.read_csv(os.path.join(os.path.dirname(dates.__file__),'review_dates.txt'), sep=';')

def easterdates():
    return pd.read_csv(os.path.join(os.path.dirname(dates.__file__),'easter_dates.txt'), sep=';')