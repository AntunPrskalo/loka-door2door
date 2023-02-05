{{ config(
    materialized='incremental',
    sort='event_at',
    dist='event_at'
) }}

with events as (
    SELECT
        VALUE:"event"::TEXT as event,
        VALUE:"on"::TEXT as event_on,
        VALUE:"at"::TIMESTAMP as event_at,
        PARSE_JSON(VALUE:"data"::TEXT) as data,
        VALUE:"organization_id"::TEXT as organization_id
    FROM {{ source('RAW', 'vehicle_iot_data') }}

    {% if is_incremental() %}
        where event_at > (select max(event_at) from {{ this }})
    {% endif %}
)
SELECT
    event,
    event_on,
    event_at,
    data:"id"::TEXT as data_id,
    data:"location":"lat"::FLOAT as data_location_lat,
    data:"location":"lng"::FLOAT as data_location_lng,
    data:"location":"at"::TIMESTAMP as data_location_at,
    data:"start"::TIMESTAMP as data_start,
    data:"finish"::TIMESTAMP as data_finish,
    organization_id
FROM events