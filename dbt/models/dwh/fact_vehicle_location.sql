{{ config(
    materialized='incremental',
    sort='vehicle_location_time',
    dist='vehicle_location_time',
) }}

with locations as (
    SELECT
        e.event_at,
        e.data_id,
        e.data_location_lat,
        e.data_location_lng,
        e.data_location_at,
        op.operating_period_key
    FROM {{ ref('vehicle_iot_data_parsed') }} e
    INNER JOIN {{ ref('registration_events_grouped') }} r
        ON e.data_id = r.data_id AND e.data_location_at BETWEEN r.register_event_at and COALESCE(r.deregister_event_at, CURRENT_TIMESTAMP())
    INNER JOIN {{ ref('dim_operating_period') }} op
        ON r.register_event_at BETWEEN op.operating_period_start_time and op.operating_period_finish_time
    WHERE lower(event) = 'update'

    {% if is_incremental() %}
        and data_location_at > (select max(vehicle_location_time) from {{ this }})
    {% endif %}
)
SELECT
    {{ dbt_utils.generate_surrogate_key([ 'data_id',  'data_location_lat', 'data_location_lng', 'data_location_at' ]) }} as vehicle_location_key,
    {{ dbt_utils.generate_surrogate_key([ 'data_id' ]) }} as vehicle_key,
    operating_period_key,
    data_location_lat as vehicle_location_lat,
    data_location_lng as vehicle_location_lng,
    data_location_at as vehicle_location_time
FROM locations


--- ASSUMPTION Events are recorded only if vehicle is registered to a operation period