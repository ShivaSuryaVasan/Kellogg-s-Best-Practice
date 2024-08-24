/*
===================================================================================================================================================================================================================
Authors           : Roshni Balaji
Create Date       : 18/07/2024
Description       : This store procedure stores swell cust allowance data for canada
Name              : cust_mstr.cust_co_cd
Revisions   :
Input Variable : 
Version     Date        Author           Description
---------  ----------  ---------------  ------------------------------------------------------------------
1.0        18/07/2024   Roshni Balaji         Final DBT Model created from Intermediate 


===================================================================================================================================================================================================================
*/

{{
    config(
        tags=['cust_mstr']
    )
}}

with int_cust_co_cd as(
    select * from {{ ref('int_cust_co_cd') }}
),

final as(
    select
        cust_nbr,
        co_cd,
        tdlinx_nbr,
        co_cd_del_ind,
        hash_key,
        kortex_dprct_ts,
        kortex_upld_ts,
        src_nm
    from
        int_cust_co_cd
)

select * from final