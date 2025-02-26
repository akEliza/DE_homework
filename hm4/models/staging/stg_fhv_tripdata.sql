{{
    config(
        materialized='view'
    )
}}

WITH source_data AS (
    SELECT 
        dispatching_base_num,
        pickup_datetime,
        dropOff_datetime,
        PUlocationID,
        DOlocationID,
        SR_Flag,
        Affiliated_base_number
    FROM {{ source('staging', 'fhv_tripdata') }}
    WHERE dispatching_base_num IS NOT NULL and DOlocationID IS NOT NULL
)

SELECT * FROM source_data
