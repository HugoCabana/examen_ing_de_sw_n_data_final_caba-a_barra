{% set clean_dir = var('clean_dir') %}
{% set ds_nodash = var('ds_nodash') %}

with source as (
    select *
    from read_parquet(
        '{{ clean_dir }}/transactions_{{ ds_nodash }}_clean.parquet'
    )
),

-- TODO: Completar el modelo para que cree la tabla staging con los tipos adecuados segun el schema.yml.

typed as (
    select
        -- Identificadores
        cast(transaction_id as varchar)      as transaction_id,
        cast(customer_id as varchar)         as customer_id,

        -- Métricas
        cast(amount as double)               as amount,

        -- Estado normalizado
        lower(cast(status as varchar))       as status,

        -- Timestamp: lo dejamos como tipo timestamp
        cast(transaction_ts as timestamp)    as transaction_ts,

        -- Campo derivado (date)
        cast(transaction_ts as date)         as transaction_date

    from source
)

select *
from typed