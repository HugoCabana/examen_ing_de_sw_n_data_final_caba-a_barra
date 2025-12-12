-- Verifica la coherencia entre la tabla gold y la silver.
-- El campo transaction_count debe reflejar exactamente la cantidad
-- de transacciones del customer en la tabla stg_transactions.
-- Este test permite detectar duplicaciones, filtrados incorrectos
-- o cambios no intencionales que alteren la consistencia del modelo gold.
select
  f.customer_id,
  f.transaction_count,
  count(s.transaction_id) as count_from_stg
from {{ ref('fct_customer_transactions') }} f
join {{ ref('stg_transactions') }} s
  on s.customer_id = f.customer_id
group by 1,2
having f.transaction_count <> count(s.transaction_id)