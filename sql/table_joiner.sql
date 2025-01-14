CREATE OR REPLACE TABLE `taxi_data_engineering.fact_table_enriched` AS (
  SELECT 
      pd.date AS pickup_date,  
      dd.date AS dropoff_date,  
      pt.time AS pickup_time,
      dt.time AS dropoff_time,
      pl.borough AS pickup_borough,
      pl.zone AS pickup_zone,
      pl.service_zone AS pickup_service_zone,
      dl.borough AS dropoff_borough,
      dl.zone AS dropoff_zone,
      dl.service_zone AS dropoff_service_zone,
      rate.rate_code AS rate_code,
      pay.payment_type AS payment_type,
      f.passenger_count, f.trip_distance, f.fare_amount, f.extra, f.mta_tax, f.tip_amount, f.tolls_amount, 
      f.improvement_surcharge, f.total_amount, f.congestion_surcharge, f.airport_fee
  FROM `taxi_data_engineering.fact_table` AS f
  LEFT JOIN `taxi_data_engineering.dim_date` AS pd ON f.pickup_date_id = pd.date_id   
  LEFT JOIN `taxi_data_engineering.dim_date` AS dd ON f.dropoff_date_id = dd.date_id
  LEFT JOIN `taxi_data_engineering.dim_time` AS pt ON f.pickup_time_id = pt.time_id   
  LEFT JOIN `taxi_data_engineering.dim_time` AS dt ON f.dropoff_time_id = dt.time_id 
  LEFT JOIN `taxi_data_engineering.dim_location` AS pl ON f.pickup_location_id = pl.location_id   
  LEFT JOIN `taxi_data_engineering.dim_location` AS dl ON f.dropoff_location_id = dl.location_id 
  LEFT JOIN `taxi_data_engineering.dim_rate_code` AS rate ON f.rate_code_id = rate.rate_code_id
  LEFT JOIN `taxi_data_engineering.dim_payment_type` AS pay ON f.payment_type_id = pay.payment_type_id
);


