/*
-- klg_nga_kna.cust_mstr.sp_cust_hier_c

-- ===================================================================================================================================================================================================================
Authors    : Surya
Create Date: 07/24/2024
Description: Customer hierarchy C
Name       : cust_mstr.sp_cust_hier_c
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

with kna_ecc_cust_mstr_kna1 as (
    select * from {{ source('stg_kna_ecc_cust_mstr','kna_ecc_cust_mstr_kna1') }}
),

int_cust_hier_c as (
    select * from {{ ref('int_cust_hier_c') }}
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
        rgn_nbr,
        zone_nbr,
        distrct_nbr,
        terr_nbr,
        sold_to_nbr,
        co_nm,
        ctry_nm,
        chnl_nm,
        rgn_nm,
        zone_nm,
        distrct_nm,
        terr_nm,
        sold_to_nm,
        last_level_nbr,
        last_level_cust_nbr,
        kortex_upld_ts,
        kortex_dprct_ts
    from  
        int_cust_hier_c
    where
        trim(sold_to_nbr) = '' or sold_to_nbr in (select distinct kunnr from kna_ecc_cust_mstr_kna1 where KTOKD='0001')
)

select * from final