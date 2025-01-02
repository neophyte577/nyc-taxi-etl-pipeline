import pyarrow.parquet as pq

filename = 'taxi_data'

parquet = pq.read_table(f'{filename}.parquet')

parquet.to_pandas().to_csv(f'{filename}.csv')
