{{ config(
    materialized='incremental',
    sort='deregistration_time',
    dist='deregistration_time',
) }}

with events as (
    SELECT
        r.data_id,
        r.deregister_event_at,
        r.organization_id,
        op.operating_period_key
    FROM {{ ref('registration_events_grouped') }} r
    LEFT JOIN {{ ref('dim_operating_period') }} op
        ON r.register_event_at BETWEEN op.operating_period_start_time AND op.operating_period_finish_time
    WHERE r.deregister_event_at IS NOT NULL

    {% if is_incremental() %}
        and r.deregister_event_at > (select max(deregistration_time) from {{ this }})
    {% endif %}
)
SELECT
    {{ dbt_utils.generate_surrogate_key([ 'data_id', 'deregister_event_at' ]) }} as deregistration_key,
    operating_period_key,
    {{ dbt_utils.generate_surrogate_key([ 'organization_id' ]) }} as organization_key,
    deregister_event_at as deregistration_time
FROM events

-- ASSUMPTION: Operation periods are not overlaping