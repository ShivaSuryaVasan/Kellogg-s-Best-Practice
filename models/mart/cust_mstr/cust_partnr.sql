/*
===================================================================================================================================================================================================================
Authors           : Roshni Balaji
Create Date       : 18/07/2024
Description       : This store procedure stores swell cust allowance data for canada
Name              : cust_mstr.sp_cust_partnr
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

with int_cust_partnr as(
    select * from {{ ref('int_cust_partnr') }}
),

final as(
    select
        partnr_fctn_cd,
        partnr_fctn_desc,
        cust_nbr,
        sales_org_cd,
        distbn_chnl_cd,
        div_cd,
        partnr_ctr_nbr,
        partnr_cust_nbr,
        lng_cd,
        hash_key,
        kortex_dprct_ts,
        kortex_upld_ts,
        src_nm
    from
        int_cust_partnr)

select * from final