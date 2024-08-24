
/*
-- klg_nga_kna.cust_mstr.sp_cust_hier_d

-- ===================================================================================================================================================================================================================
 Authors    : Surya
 Create Date: 07/24/2024
 Description: Customer hierarchy (SAP)
 Name       : cust_mstr.sp_cust_hier_d
 Revisions  :
 Version     Date        Author           Description
 ---------  ----------  ---------------  ------------------------------------------------------------------
 1.0        07/24/2024   Surya           Initial Version.
-- ===================================================================================================================================================================================================================
*/

{{
    config(
        tags = ['cust_mstr']
    )
}}

with kna_ecc_cust_mstr_kna1 as (
    select * from {{ source('stg_kna_ecc_cust_mstr','kna_ecc_cust_mstr_kna1') }}
),

int_cust_hier_d as (
    select * from {{ ref('int_cust_hier_d') }}
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
        sales_mgmt_a_nbr,
        sales_mgmt_b_nbr,
        sales_mgmt_c_nbr,
        sales_mgmt_d_nbr,
        sales_mgmt_e_nbr,
        plan_to_nbr,
        chain_nbr,
        sold_to_nbr,
        co_nm,
        ctry_nm,
        sales_mgmt_a_nm,
        sales_mgmt_b_nm,
        sales_mgmt_c_nm,
        sales_mgmt_d_nm,
        sales_mgmt_e_nm,
        plan_to_nm,
        chain_nm,
        sold_to_nm,
        last_level_nbr,
        last_level_cust_nbr,
        kortex_upld_ts,
        kortex_dprct_ts
    from 
        int_cust_hier_d 
    where
        sold_to_nbr = '' or sold_to_nbr in (select distinct kunnr from kna_ecc_cust_mstr_kna1 where KTOKD='0001')
)

select * from final