{{ config(
    materialized='table'
) }}

with organizations as (
    SELECT
        DISTINCT organization_id
    FROM {{ ref('vehicle_iot_data_parsed') }}
)
SELECT
    {{ dbt_utils.generate_surrogate_key([ 'organization_id' ]) }} as organization_key,
    organization_id as organization_source_id
FROM organizations