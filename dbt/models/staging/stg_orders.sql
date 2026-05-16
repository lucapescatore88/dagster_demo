select
    id                          as order_id,
    name                        as order_ref,
    value                       as amount,
    status                      as order_status
from {{ source('bronze', 'orders') }}
