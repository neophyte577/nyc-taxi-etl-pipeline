SELECT 
  rate_code, 
  AVG(fare_amount) AS avg_fare, 
  AVG(trip_distance) AS avg_distance, 
  AVG(tip_amount) AS avg_tip, 
  AVG(congestion_surcharge) AS avg_con_surcharge, 
  AVG(total_amount) AS avg_total
FROM `taxi_data_engineering.fact_table`
INNER JOIN `taxi_data_engineering.dim_rate_code` ON `taxi_data_engineering.fact_table`.rate_code_id = `taxi_data_engineering.dim_rate_code`.rate_code_id
GROUP BY rate_code;

SELECT 
  payment_type, 
  AVG(fare_amount) AS avg_fare, 
  AVG(trip_distance) AS avg_distance, 
  AVG(tip_amount) AS avg_tip,
  AVG(congestion_surcharge) AS avg_con_surcharge, 
  AVG(total_amount) AS avg_total
FROM `taxi_data_engineering.fact_table`
INNER JOIN `taxi_data_engineering.dim_payment_type` ON `taxi_data_engineering.fact_table`.payment_type_id = `taxi_data_engineering.dim_payment_type`.payment_type_id
GROUP BY payment_type;

SELECT 
  is_weekend, 
  AVG(fare_amount) AS avg_fare, 
  AVG(trip_distance) AS avg_distance, 
  AVG(tip_amount) AS avg_tip,
  AVG(congestion_surcharge) AS avg_con_surcharge, 
  AVG(total_amount) AS avg_total
FROM `taxi_data_engineering.fact_table`
INNER JOIN `taxi_data_engineering.dim_date` ON `taxi_data_engineering.fact_table`.date_id = `taxi_data_engineering.dim_date`.date_id
GROUP BY is_weekend; 

