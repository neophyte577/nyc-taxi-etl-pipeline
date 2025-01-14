import pyarrow.parquet as pq

parquet = pq.read_table('taxi_data.parquet')
df = parquet.to_pandas()

def transform(df):

    import numpy as np
    import pandas as pd

    df.drop(columns=['VendorID','store_and_fwd_flag'], inplace=True)

    # Drop anachronistic entries by year and month

    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])

    # Incorrect years

    to_drop = df['tpep_pickup_datetime'].dt.year.isin([2002,2009]) | df['tpep_dropoff_datetime'].dt.year.isin([2002,2009, 2023])
    df = df.loc[~to_drop]

    # Incorrect months

    to_drop = df['tpep_pickup_datetime'].dt.month.isin([2]) | df['tpep_dropoff_datetime'].dt.month.isin([12])
    df = df.loc[~to_drop]

    # Drop missing data (cf. Jupyter notebook section entitled 'Examine and Drop NanN')

    df.dropna(inplace=True)

    # dim_date

    dates = pd.concat([df['tpep_pickup_datetime'].dt.date, df['tpep_dropoff_datetime'].dt.date]).sort_values().drop_duplicates()

    dim_date = pd.DataFrame({
        'date_id':range(1,len(dates)+1), 
        'date':pd.to_datetime(dates) # Convert to datetime for more time-efficient month, day, etc. extraction
        })
    dim_date.set_index('date_id', inplace=True)

    dim_date['year'] = dim_date['date'].dt.year.astype(int)
    dim_date['month'] = dim_date['date'].dt.month.astype(int)
    dim_date['day'] = dim_date['date'].dt.day.astype(int)
    dim_date['day_of_week'] = dim_date['date'].dt.day_of_week.astype(int)
    dim_date['is_weekend'] = dim_date['date'].apply(lambda d: 1 if d in [5,6] else 0).astype(int)
    dim_date['date'] = dim_date['date'].dt.date # Convert back to date

    # dim_time 

    times = pd.concat([df['tpep_pickup_datetime'].dt.time, df['tpep_dropoff_datetime'].dt.time]).sort_values().drop_duplicates()

    dim_time = pd.DataFrame({
        'time_id':range(1,len(times)+1), 
        'time':pd.to_datetime(times, format='%H:%M:%S') # Convert to datetime for more time-efficient hour, minute, etc. extraction
        })
    dim_time.set_index('time_id', inplace=True)

    dim_time['hour'] = dim_time['time'].dt.hour
    dim_time['minute'] = dim_time['time'].dt.minute
    dim_time['second'] = dim_time['time'].dt.second
    dim_time['time'] = dim_time['time'].dt.time # Convert back to time

    # dim_location

    dim_location = pd.read_csv('taxi_zone_lookup.csv')

    dim_location.columns = ['location_id', 'borough', 'zone', 'service_zone']

    dim_location.set_index('location_id', inplace=True)

    # dim_rate_code

    rate_code_lib = {1:'Standard Rate', 2:'JFK', 3:'Newark', 4:'Nassau or Westchester', 5:'Negotiated fare', 6:'Group ride', 99:'Other'}

    dim_rate_code = pd.DataFrame({
        'rate_code_id':rate_code_lib.keys(), 
        'rate_code':rate_code_lib.values()
        })
    dim_rate_code.set_index('rate_code_id', inplace=True)

    # dim_payment_type

    payment_type_lib = {1:'Credit card', 2:'Cash', 3:'No charge', 4:'Dispute', 5:'Unkown', 6:'Voided trip'}

    dim_payment_type = pd.DataFrame({
        'payment_type_id':payment_type_lib.keys(), 
        'payment_type':payment_type_lib.values()
        })
    dim_payment_type.set_index('payment_type_id', inplace=True)

    # fact_table

    fact_table = pd.DataFrame()

    fact_table['pickup_date_id'] = df['tpep_pickup_datetime'].dt.date.map(dim_date.reset_index().set_index('date')['date_id'])
    fact_table['dropoff_date_id'] = df['tpep_dropoff_datetime'].dt.date.map(dim_date.reset_index().set_index('date')['date_id'])
    fact_table['pickup_time_id'] = df['tpep_pickup_datetime'].dt.time.map(dim_time.reset_index().set_index('time')['time_id'])
    fact_table['dropoff_time_id'] = df['tpep_dropoff_datetime'].dt.time.map(dim_time.reset_index().set_index('time')['time_id'])
    fact_table['pickup_location_id'] = df['PULocationID'].astype(int)
    fact_table['dropoff_location_id'] = df['DOLocationID'].astype(int)
    fact_table['payment_type_id'] = df['payment_type']
    fact_table['rate_code_id'] = df['RatecodeID']
    fact_table['airport_fee'] = df['Airport_fee']
    fact_table['passenger_count'] = df['passenger_count'].astype(int)
    other_cols = ['trip_distance', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 'improvement_surcharge', 'total_amount', 'congestion_surcharge']
    fact_table[other_cols] = df[other_cols]

    fact_table.index.name = 'fact_id'

    return {'fact_table':fact_table, 'dim_date':dim_date, 'dim_time':dim_time,'dim_location':dim_location, \
            'dim_rate_code':dim_rate_code, 'dim_payment_type':dim_payment_type}

def main():
    transform(df)

if __name__ == '__main__':
    main()