{% macro refresh_source_external_tables() %}
    ALTER EXTERNAL TABLE {{ source('RAW', 'vehicle_iot_data') }} REFRESH;
{% endmacro %}