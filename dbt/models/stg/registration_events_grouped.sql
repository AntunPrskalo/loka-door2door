{{ config(
    materialized='incremental',
    unique_key="_unique_key",
    sort='register_event_at',
    dist='register_event_at',
) }}

--- ASSUMPTION - REG and DREG events are ordered

with new_events as (
    SELECT
        data_id,
        event,
        event_at,
        LEAD(event_at) OVER (PARTITION BY data_id ORDER BY event_at) as next_event_at
    FROM {{ ref('vehicle_iot_data_parsed') }} e
    WHERE lower(event) in ('register', 'deregister')
        -- AND event_at <= '2019-06-01 18:17:04.089'

    {% if is_incremental() %}
        and event_at > (select max(register_event_at) from {{ this }})
    {% endif %}
)
,new_registers as (
    SELECT
        data_id,
        event_at as register_event_at,
        next_event_at as deregister_event_at,
        {{ dbt_utils.generate_surrogate_key([ 'data_id', 'event_at' ]) }} as _unique_key
    FROM new_events
    WHERE lower(event) = 'register'
)

{% if is_incremental() %}

    ,new_deregisters as (
        SELECT
            d.data_id,
            d.event_at
        FROM new_events d
        LEFT JOIN new_registers r
            ON d.data_id = r.data_id AND d.event_at = r.deregister_event_at
        WHERE 
            lower(d.event) = 'deregister'
            AND r.deregister_event_at IS NULL
    )
    ,updated_existing_registers as (
        SELECT
            r.data_id,
            r.register_event_at,
            d.event_at as deregister_event_at,
            r._unique_key
        FROM {{ this }} r
        LEFT JOIN new_deregisters d ON
            r.data_id = d.data_id   
        WHERE r.deregister_event_at IS NULL
    )

    SELECT * FROM updated_existing_registers
    UNION ALL

{% endif %}

SELECT * FROM new_registers
