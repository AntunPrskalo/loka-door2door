{{ config(
    materialized='table'
) }}

with operating_periods_creates as (
    SELECT
        data_id as c_data_id,
        data_start,
        data_finish
    FROM {{ ref('vehicle_iot_data_parsed') }}
    WHERE lower(event_on) = 'operating_period' and lower(event) = 'create' 
)
,operating_periods_deletes as (
    SELECT
        data_id,
        event_at
    FROM {{ ref('vehicle_iot_data_parsed') }}
    WHERE lower(event_on) = 'operating_period' and lower(event) = 'delete'
)
SELECT
    {{ dbt_utils.generate_surrogate_key([ 'c_data_id' ]) }} as operating_period_key,
    c.c_data_id as operating_period_source_id,
    c.data_start as operating_period_start_time,
    c.data_finish as operating_period_finish_time,
    d.data_id IS NOT NULL as operating_period_is_deleted,
    d.event_at as operating_period_deletion_time
FROM operating_periods_creates c
LEFT JOIN operating_periods_deletes d
    ON c.c_data_id = d.data_id