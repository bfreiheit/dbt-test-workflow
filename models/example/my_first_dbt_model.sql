
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='view') }}

select date_trunc('day', ordered_at)
from {{ source('raw', 'raw_orders') }}
where id = '2b1aefde-b644-4d6e-ae71-dd7f8c3d73c9'

