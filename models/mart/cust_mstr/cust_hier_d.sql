
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
        materialized='ephemeral'
    )
}}

with kna_ecc_cust_mstr_kna1 as (
    select * from {{ source('stg_','kna_ecc_cust_mstr_kna1') }}
),

int_cust_hier_unrvl_i_cur_d as (
    select * from {{ ref('int_cust_hier_unrvl_i_cur_d') }}
),

int_cust_hier_unrvl_i_fut_d as (
    select * from {{ ref('int_cust_hier_unrvl_i_fut_d') }}
),

kunnr as (
    select 
        distinct kunnr 
    from 
        kna_ecc_cust_mstr_kna1 
    where 
        KTOKD='0001'
),

base as (
    select
        src_nm,
        case when t1.hier_dt = '9999-12-31' then 'FUTURE'
        else 'CURRENT'
        end as hier_tmfrm_id,
        MD5( src_nm ||
            hier_id ||
            ( case when t1.hier_dt = '9999-12-31' then 'FUTURE'
        else 'CURRENT'
        end ) ||
        lst_level_cust_nbr ) as hash_key,
        -- generating column hash key
        hier_dt,
        hier_id,
        level1_nbr as co_nbr,
        level2_nbr as ctry_nbr,
        level3_nbr as sales_mgmt_a_nbr,
        level4_nbr as sales_mgmt_b_nbr,
        level5_nbr as sales_mgmt_c_nbr,
        level6_nbr as sales_mgmt_d_nbr,
        level7_nbr as sales_mgmt_e_nbr,
        level8_nbr as plan_to_nbr,
        level9_nbr as chain_nbr,
        level10_nbr as sold_to_nbr,
        level11_nbr,
        level12_nbr,
        level13_nbr,
        level14_nbr,
        level15_nbr,
        level16_nbr,
        level17_nbr,
        level18_nbr,
        level19_nbr,
        level20_nbr,
        level1_nm as co_nm,
        level2_nm as ctry_nm,
        level3_nm as sales_mgmt_a_nm,
        level4_nm as sales_mgmt_b_nm,
        level5_nm as sales_mgmt_c_nm,
        level6_nm as sales_mgmt_d_nm,
        level7_nm as sales_mgmt_e_nm,
        level8_nm as plan_to_nm,
        level9_nm as chain_nm,
        level10_nm as sold_to_nm,
        level11_nm,
        level12_nm,
        level13_nm,
        level14_nm,
        level15_nm,
        level16_nm,
        level17_nm,
        level18_nm,
        level19_nm,
        level20_nm,
        lst_level_nbr as last_level_nbr,
        lst_level_cust_nbr as last_level_cust_nbr,
        kortex_upld_ts,
        kortex_dprct_ts
    from 
        int_cust_hier_unrvl_i_cur_d as t1

    union all

    select
        src_nm,
        case when t2.hier_dt = '9999-12-31' then 'FUTURE'
        else 'CURRENT'
        end as hier_tmfrm_id,
        MD5( src_nm ||
            hier_id ||
            ( case when t2.hier_dt = '9999-12-31' then 'FUTURE'
        else 'CURRENT'
        end ) ||
        lst_level_cust_nbr ) as hash_key,
        -- generating column hash key
        hier_dt,
        hier_id,
        level1_nbr as co_nbr,
        level2_nbr as ctry_nbr,
        level3_nbr as sales_mgmt_a_nbr,
        level4_nbr as sales_mgmt_b_nbr,
        level5_nbr as sales_mgmt_c_nbr,
        level6_nbr as sales_mgmt_d_nbr,
        level7_nbr as sales_mgmt_e_nbr,
        level8_nbr as plan_to_nbr,
        level9_nbr as chain_nbr,
        level10_nbr as sold_to_nbr,
        level11_nbr,
        level12_nbr,
        level13_nbr,
        level14_nbr,
        level15_nbr,
        level16_nbr,
        level17_nbr,
        level18_nbr,
        level19_nbr,
        level20_nbr,
        level1_nm as co_nm,
        level2_nm as ctry_nm,
        level3_nm as sales_mgmt_a_nm,
        level4_nm as sales_mgmt_b_nm,
        level5_nm as sales_mgmt_c_nm,
        level6_nm as sales_mgmt_d_nm,
        level7_nm as sales_mgmt_e_nm,
        level8_nm as plan_to_nm,
        level9_nm as chain_nm,
        level10_nm as sold_to_nm,
        level11_nm,
        level12_nm,
        level13_nm,
        level14_nm,
        level15_nm,
        level16_nm,
        level17_nm,
        level18_nm,
        level19_nm,
        level20_nm,
        lst_level_nbr as last_level_nbr,
        lst_level_cust_nbr as last_level_cust_nbr,
        kortex_upld_ts,
        kortex_dprct_ts
    from 
        int_cust_hier_unrvl_i_fut_d as t2
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
        base 
    where
        sold_to_nbr = '' or sold_to_nbr in kunnr;
)

select * from final