
/*
-- klg_nga_kna.cust_mstr.sp_cust_hier_a

-- ===================================================================================================================================================================================================================
 Authors    : Surya
 Create Date: 07/24/2024
 Description: Customer Master (SAP)
 Name       : cust_mstr.sp_cust_hier_a
 Revisions  :
 Version     Date        Author           Description
 ---------  ----------  ---------------  ------------------------------------------------------------------
 1.0        07/24/2024   Surya           Initial Version.
-- ===================================================================================================================================================================================================================
*/

{{
    config(
        materialized='table'
    )
}}

with int_cust_hier_a as (
    select * from {{ ref('int_cust_hier_a') }}
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
        level3_misc_nbr,
        level4_misc_nbr,
        level5_misc_nbr,
        level6_misc_nbr,
        level7_misc_nbr,
        level8_misc_nbr,
        level9_misc_nbr,
        level10_misc_nbr,
        co_nm,
        ctry_nm,
        level3_misc_nm,
        level4_misc_nm,
        level5_misc_nm,
        level6_misc_nm,
        level7_misc_nm,
        level8_misc_nm,
        level9_misc_nm,
        level10_misc_nm,
        last_level_nbr,
        last_level_cust_nbr,
        kortex_upld_ts,
        kortex_dprct_ts
    from 
        int_cust_hier_a
)

select * from final