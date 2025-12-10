
with base as (
    select * from {{ ref('stg_transactions') }}
),

-- TODO: Completar el modelo para que cree la tabla fct_customer_transactions con las metricas en schema.yml.

aggregated as (
    select
        customer_id,

        -- cantidad de transacciones
        count(*) as transaction_count,

        -- suma de transacciones completadas
        sum(case when status = 'completed' then amount else 0 end) as total_amount_completed,

        -- suma de todas las transacciones
        sum(amount) as total_amount_all

    from base
    group by customer_id
)

select *
from aggregated