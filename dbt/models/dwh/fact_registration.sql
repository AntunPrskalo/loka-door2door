{{ config(
    materialized='incremental',
    sort='registration_time',
    dist='registration_time',
) }}


with events as (
    SELECT
        r.data_id,
        r.register_event_at,
        op.operating_period_key
    FROM {{ ref('registration_events_grouped') }} r
    LEFT JOIN {{ ref('dim_operating_period') }} op
        ON r.register_event_at BETWEEN op.operating_period_start_time and op.operating_period_finish_time
    WHERE 1=1

    {% if is_incremental() %}
        and r.register_event_at > (select max(registration_time) from {{ this }})
    {% endif %}
)
SELECT
    {{ dbt_utils.generate_surrogate_key([ 'data_id', 'register_event_at' ]) }} as registration_key,
    operating_period_key,
    register_event_at as registration_time
FROM events

-- ASSUMPTION: Operation periods are not overlaping
-- QUESTION:   Difference between event_on and data_location_at