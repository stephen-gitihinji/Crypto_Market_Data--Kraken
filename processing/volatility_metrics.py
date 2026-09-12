from pyspark.sql.window import Window
from pyspark.sql import functions as f

#calculating the high-low range for each row
def ohlcv_volatility_transformation(ohlcv_df):
	ohlcv_df = (ohlcv_df.withColumn('high_low_range', f.col('high')-f.col('low'))
             .withColumn('relative_volatitlity', f.try_divide(
                   f.col('high_low_range'), f.col('open'))* 100
			 ))
	return ohlcv_df

def volatility_metrics(ohlcv_df):
    ohlcv_df = ohlcv_volatility_transformation(ohlcv_df)
    return ohlcv_df