{{ 
    config(materialized = 'table') 
}}

select * from {{ source('RAW_DATA', 'THAMES_VALLEY_STREET') }}
union all
select * from {{ source('RAW_DATA', 'SURREY_STREET') }}