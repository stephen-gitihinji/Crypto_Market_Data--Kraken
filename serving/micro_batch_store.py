def write_to_cassandra(batch_df, batch_id, table_name):
    print(f"==========Batch {batch_id}==========")
    print(f"Table: {table_name}")
    print(f"Rows: {batch_df.count()}")

    (
        batch_df.write
        .format("org.apache.spark.sql.cassandra")
        .mode("append")
        .options(
            keyspace="crypto",
            table=table_name
        )
        .save())
    
    print(f"Finished Batch {batch_id}")