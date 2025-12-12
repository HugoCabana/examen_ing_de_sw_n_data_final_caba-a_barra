-- Verificación de total_amount_completed <= total_amount_all
-- Verifica la coherencia de montos en la tabla gold.
-- Para cada customer, el monto de transacciones completadas debe ser
-- menor o igual al monto total de transacciones, ya que las completadas
-- son un subconjunto del total.
select
  customer_id,
  total_amount_completed,
  total_amount_all
from {{ ref('fct_customer_transactions') }}
where total_amount_completed > total_amount_all