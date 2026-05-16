select
    id                          as product_id,
    name                        as product_name,
    value                       as price,
    status                      as product_status
from {{ source('bronze', 'products') }}
