select
    id                          as customer_id,
    name                        as customer_name,
    status                      as customer_status
from {{ source('bronze', 'customers') }}
