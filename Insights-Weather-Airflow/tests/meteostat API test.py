from meteostat import Hourly
from datetime import datetime

data = Hourly("72345", datetime(1951, 1, 1), datetime(2025, 6, 5)).fetch()
print(data.shape)

