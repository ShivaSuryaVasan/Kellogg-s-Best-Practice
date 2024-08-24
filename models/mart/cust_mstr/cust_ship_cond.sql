/*
===================================================================================================================================================================================================================
Authors           : Ilakkiya
Create Date       : 12/08/2024
Description       : Customer Master Shipment Condition
Name              : cust_ship_cond
Revisions   :
Input Variable :
Version     Date        Author           Description
---------  ----------  ---------------  ------------------------------------------------------------------
1.0        12/08/2024   Ilakkiya         Final DBT Model created from Intermediate


===================================================================================================================================================================================================================
*/

{{
    config(
        tags=['cust_mstr']
    )
}}

with int_cust_ship_cond as(
    select * from {{ ref('int_cust_ship_cond') }}
),

--fetching columns from int_cust_ship_cond
final as(
    select
        cust_nbr,
        plant_nbr,
        bu_cd,
        ship_cond_cd,
        load_methd_cd,
        ship_depart_point_cd,
        hash_key,
        kortex_dprct_ts,
        kortex_upld_ts,
        src_nm
    from int_cust_ship_cond
)

select * from final