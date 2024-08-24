/*
===================================================================================================================================================================================================================
Authors           : Ilakkiya
Create Date       : 12/08/2024
Description       :
Name              : price_list_type
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

with int_price_list_type as(
    select * from {{ ref('int_price_list_type') }}
),

--fetching columns from int_price_list_type
final as(
    select
        chnl_cd,
        trnsptn_group_cd,
        price_list_type_cd,
        cpu_bump_up_cd,
        hash_key,
        kortex_dprct_ts,
        kortex_upld_ts,
        src_nm
    from int_price_list_type
)

select * from final
 