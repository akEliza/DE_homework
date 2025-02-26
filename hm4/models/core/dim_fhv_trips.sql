{{
    config(
        materialized='table'
    )
}}

WITH fhv_tripdata AS (
    SELECT *, 
        'FHV' AS service_type
    FROM {{ ref('stg_fhv_tripdata') }}
),

dim_zones AS (
    SELECT * FROM {{ ref('dim_zones') }}
    WHERE borough != 'Unknown'
),

trip_data AS (
    SELECT 
        NULL AS tripid, -- FHV 数据没有 tripid，可在后续处理中生成
        fhv_tripdata.dispatching_base_num,
        fhv_tripdata.service_type,
        fhv_tripdata.PUlocationID AS pickup_locationid,
        pickup_zone.borough AS pickup_borough,
        pickup_zone.zone AS pickup_zone,
        fhv_tripdata.DOlocationID AS dropoff_locationid,
        dropoff_zone.borough AS dropoff_borough,
        dropoff_zone.zone AS dropoff_zone,
        fhv_tripdata.pickup_datetime,
        fhv_tripdata.dropOff_datetime,
        fhv_tripdata.SR_Flag,
        fhv_tripdata.Affiliated_base_number,
        -- 拆分 year_quarter 字段为 year 和 month
        EXTRACT(YEAR FROM fhv_tripdata.pickup_datetime) AS year,
        EXTRACT(MONTH FROM fhv_tripdata.pickup_datetime) AS month,
        -- 计算 trip_duration
        TIMESTAMP_DIFF(fhv_tripdata.dropOff_datetime, fhv_tripdata.pickup_datetime, SECOND) AS trip_duration
    FROM fhv_tripdata
    LEFT JOIN dim_zones AS pickup_zone
    ON fhv_tripdata.PUlocationID = pickup_zone.locationid
    LEFT JOIN dim_zones AS dropoff_zone
    ON fhv_tripdata.DOlocationID = dropoff_zone.locationid
    WHERE dropoff_zone.zone IS NOT NULL
),

trip_p90 AS (
    SELECT 
        year, 
        month, 
        pickup_locationid, 
        dropoff_locationid,
        APPROX_QUANTILES(trip_duration, 100)[OFFSET(90)] AS p90_trip_duration
    FROM trip_data
    GROUP BY year, month, pickup_locationid, dropoff_locationid
)

SELECT 
    t.*,
    p.p90_trip_duration
FROM trip_data t
LEFT JOIN trip_p90 p
ON t.year = p.year 
AND t.month = p.month
AND t.pickup_locationid = p.pickup_locationid
AND t.dropoff_locationid = p.dropoff_locationid
