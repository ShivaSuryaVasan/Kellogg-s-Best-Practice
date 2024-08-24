
/*
-- klg_nga_kna.cust_mstr.sp_cust_hier_b

-- ===================================================================================================================================================================================================================
Authors    : Surya
Create Date: 07/24/2024
Description: Customer hierarchy (SAP)
Name       : cust_mstr.sp_cust_hier_b
Revisions  :
Version     Date        Author           Description
---------  ----------  ---------------  ------------------------------------------------------------------
1.0        07/24/2024   Surya           Initial Version.
-- ===================================================================================================================================================================================================================
*/

{{
    config(
        tags = ["cust_mstr"]
    )
}}

with int_cust_hier_b as (
    select * from {{ ref('int_cust_hier_b') }}
),

final as (
    select 
        distinct src_nm,
        hier_tmfrm_id,
        hash_key,
        hier_dt,
        hier_id,
        co_nbr,
        ctry_nbr,
        chnl_nbr,
        level4_misc_nbr,
        rgn_nbr,
        area_nbr,
        terr_nbr,
        sold_to_nbr,
        co_nm,
        ctry_nm,
        chnl_nm,
        level4_misc_nm,
        rgn_nm,
        area_nm,
        terr_nm,
        sold_to_nm,
        last_level_nbr,
        last_level_cust_nbr,
        kortex_upld_ts,
        kortex_dprct_ts
    from 
        int_cust_hier_b
)

select * from final