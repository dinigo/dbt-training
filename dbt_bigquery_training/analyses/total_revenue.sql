
select
  sum(amount) as revenue
from {{ ref('stg_payments') }}