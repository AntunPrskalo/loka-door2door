{{ config(
    materialized='table'
) }}

with operating_periods as (
    SELECT
        data_id,
        data_start,
        data_finish,
        ROW_NUMBER() OVER (PARTITION BY data_id ORDER BY data_finish desc, data_start desc) as rn
    FROM {{ ref('vehicle_iot_data_parsed') }}
    WHERE lower(event_on) = 'operating_period'
)
SELECT
    {{ dbt_utils.generate_surrogate_key([ 'data_id' ]) }} as operating_period_key,
    data_id as operating_period_source_id,
    data_start as operating_period_start_time,
    data_finish as operating_period_finish_time
FROM operating_periods