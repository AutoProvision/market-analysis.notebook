import warnings
import os

os.system("pip install requests pyspark pandas matplotlib numpy")

import zipfile, requests, random
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import IntegerType, DecimalType, DoubleType
from pyspark.sql.functions import regexp_replace, to_date, col, trim, substring, when, mean, format_number

warnings.simplefilter(action='ignore', category=FutureWarning)
pd.options.display.float_format = '{:.2f}'.format
