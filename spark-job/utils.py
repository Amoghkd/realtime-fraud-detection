from math import radians, sin, cos, asin, sqrt

import pandas as pd


def haversine(lat1, lon1, lat2, lon2):
    # calculate the great-circle distance between two points on the Earth (km)
    try:
        lat1, lon1, lat2, lon2 = map(float, [lat1, lon1, lat2, lon2])
    except Exception:
        return 0.0
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * asin(sqrt(a))
    r = 6371
    return c * r


# Provide a simple wrapper that Spark can convert to a UDF in the streaming job
def haversine_py(lat1, lon1, lat2, lon2):
    return haversine(lat1, lon1, lat2, lon2)


def load_blacklist(path):
    """Read merchant_blacklist.csv and return a list of merchant ids."""
    try:
        df = pd.read_csv(path)
        return df['merchant_id'].astype(str).tolist()
    except Exception:
        return []
