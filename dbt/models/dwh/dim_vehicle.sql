{{ config(
    materialized='table'
) }}

with vehicles as (
    SELECT
        DISTINCT data_id
    FROM {{ ref('vehicle_iot_data_parsed') }}
    WHERE lower(event_on) = 'vehicle'
)
SELECT
    {{ dbt_utils.generate_surrogate_key([ 'data_id' ]) }} as vehicle_key,
    data_id as vehicle_source_id
FROM vehicles