import query


def run(spark, args):
    query_dt = args['dt']

    data = spark.read.option('header', 'true').csv(args['input_path'])
    data.createOrReplaceTempView('conviva_strings')
    data_recast = spark.sql(query.user_cast_sql)
    data_recast.createOrReplaceTempView('conviva_streams')

    user_perf_features = spark.sql(query.get_perf_sql(query_dt, days_back=28))
    user_perf_features.write.parquet(args['output_path'], mode='overwrite')

