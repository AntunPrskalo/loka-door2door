{% macro recreate_external_source_tables() %}
    CREATE OR replace EXTERNAL TABLE {{ source('RAW', 'vehicle_iot_data') }}
    WITH LOCATION = @DOOR2DOOR_DWH.RAW.vehicle_iot_data_stg
    FILE_FORMAT = (Format_Name = PARQUET_FORMAT);
{% endmacro %}