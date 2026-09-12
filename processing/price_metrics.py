from pyspark.sql.window import Window
from pyspark.sql import functions as f

def ohlcv_price_transformation(ohlcv_df):
    #using the lag functions
    ohlcv_df = (ohlcv_df.withColumn('price_change', f.col('close') - f.col('open'))
                .withColumn('price_change_pct', f.try_divide(f.col('price_change'), f.col('close'))*100)
                .withColumn('typical_price',f.try_divide((f.col('high') + f.col('low') + f.col('close')), f.lit(3)))
                .withColumn('normalized_price_position', f.try_divide((f.col('close') - f.col('low')),(f.col('high') - f.col('low'))))
                .withColumn('intraday_return', f.try_divide((f.col('close') - f.col('open')), f.col('open')))
                )
    return ohlcv_df

# def best_bid_ask(l2_book_df):
#     best_bid_ask_df = l2_book_df.withWatermark("timestamp", "10 minutes").groupBy(
#          f.window(f.col("timestamp"), "1 day"),
#          "pair"
#     ).agg(
#         f.max(f.when(f.col('side') == 'bid', f.col('price'))).alias('best_bid'),
#         f.min(f.when(f.col('side') == 'ask', f.col('price'))).alias('best_ask')
#     )
#     return best_bid_ask_df

#spread
def spread_transformation(recent_spreads_df):
      recent_spreads_df = recent_spreads_df.withColumn("spread", f.col('ask')-f.col('bid'))
      avg_spread_prices_df = (recent_spreads_df.withWatermark('timestamp', '10 minutes').groupBy(
           f.window('timestamp', "1 day"),
           'pair').agg(
           f.avg(f.col('bid')).alias('avg_bid_price'),
           f.avg(f.col('ask')).alias('avg_ask_price'),
           f.avg(f.col('spread')).alias('avg_spread')
      ))
      return avg_spread_prices_df

def price_metrics(ohlcv_df,recent_spreads_df):
    ohlcv_df = ohlcv_price_transformation(ohlcv_df)
    spread_df = spread_transformation(recent_spreads_df)
    
    return ohlcv_df, spread_df