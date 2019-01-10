import pandas as pd


def get_data_dict(spark_df):
    df = pd.DataFrame(spark_df.collect(), columns=spark_df.columns)
    df = df.set_index('user_id')
    return df.to_dict(orient='index')
