/*
-- klg_nga_kna.cust_mstr_spin.sp_cust

-- ===================================================================================================================================================================================================================
Authors: Surya
Create Date: 08/12/2024
Description: Customer Master (SAP)
Name: cust_mstr_spin.sp_cust
Revisions:
Version     Date        Author           Description
---------  ----------  ---------------  ------------------------------------------------------------------
1.0        08/12/2024   Surya     Initial Version.
-- ===================================================================================================================================================================================================================
*/

{{ 
    config(
        tags = ['cust_mstr']
    )
}}

with int_cust as (
    select * from {{ ref('int_cust') }}
),

kna_ecc_cust_mstr_t151t as (
    select * from {{ source('stg_kna_ecc_cust_mstr', 'kna_ecc_cust_mstr_t151t') }}
),

final as(
    select 
    c.cust_nbr,
    c.cust_nm,
    c.cust2_nm,
    c.sort2 as srch_term2_txt,
    c.street_nm,
    c.city_nm,
    c.rgn_cd,
    c.distrct_nm,
    c.ctry_cd,
    c.pstl_cd,
    c.store_nbr,
    c.plan_to_cd,
    c.chain_ind,
    c.cre_dt,
    c.duns_nbr,
    c.duns4_nbr,
    c.store_cd,
    c.altn_payer_nbr,
    c.cust_type_cd,
    c.acct_group_cd,
    c.acct_group_desc,
    c.pymt_blok_cd,
    c.cust_pref_brkt_dry_cd,
    c.cust_pref_brkt_frz_cd,
    c.cust_fix_brkt_dry_cd,
    c.cust_fix_brkt_frz_cd,
    c.cust_ovrd_brkt_cd,
    c.altn_payer_acct_cd,
    c.order_blok_cd,
    c.ctry_nm,
    c.del_ind,
    c.CHNL_CD,
    c.area_lat_val,
    c.terr_lat_val,
    c.terr_lon_val,
    c.hier_a_co_nbr,
    c.hier_a_ctry_nbr,
    c.hier_a_level3_misc_nbr,
    c.hier_a_level4_misc_nbr,
    c.hier_a_level5_misc_nbr,
    c.hier_a_level6_misc_nbr,
    c.hier_a_level7_misc_nbr,
    c.hier_a_level8_misc_nbr,
    c.hier_a_level9_misc_nbr,
    c.hier_a_level10_misc_nbr,
    c.hier_a_co_nm,
    c.hier_a_ctry_nm,
    c.hier_a_level3_misc_nm,
    c.hier_a_level4_misc_nm,
    c.hier_a_level5_misc_nm,
    c.hier_a_level6_misc_nm,
    c.hier_a_level7_misc_nm,
    c.hier_a_level8_misc_nm,
    c.hier_a_level9_misc_nm,
    c.hier_a_level10_misc_nm,
    c.hier_b_co_nbr,
    c.hier_b_ctry_nbr,
    c.hier_b_chnl_nbr,
    c.hier_b_level4_misc_nbr,
    c.hier_b_rgn_nbr,
    c.hier_b_area_nbr,
    c.hier_b_terr_nbr,
    c.hier_b_co_nm,
    c.hier_b_ctry_nm,
    c.hier_b_chnl_nm,
    c.hier_b_level4_misc_nm,
    c.hier_b_rgn_nm,
    c.hier_b_area_nm,
    c.hier_b_terr_nm,
    c.hier_c_co_nbr,
    c.hier_c_ctry_nbr,
    c.hier_c_chnl_nbr,
    c.hier_c_rgn_nbr,
    c.hier_c_zone_nbr,
    c.hier_c_distrct_nbr,
    c.hier_c_terr_nbr,
    c.hier_c_co_nm,
    c.hier_c_ctry_nm,
    c.hier_c_chnl_nm,
    c.hier_c_rgn_nm,
    c.hier_c_zone_nm,
    c.hier_c_distrct_nm,
    c.hier_c_terr_nm,
    c.hier_d_co_nbr,
    c.hier_d_ctry_nbr,
    c.hier_d_sales_mgmt_a_nbr,
    c.hier_d_sales_mgmt_b_nbr,
    c.hier_d_sales_mgmt_c_nbr,
    c.hier_d_sales_mgmt_d_nbr,
    c.hier_d_sales_mgmt_e_nbr,
    c.plan_to_nbr,
    c.chain_nbr,
    c.hier_d_co_nm,
    c.hier_d_ctry_nm,
    c.hier_d_sales_mgmt_a_nm,
    c.hier_d_sales_mgmt_b_nm,
    c.hier_d_sales_mgmt_c_nm,
    c.hier_d_sales_mgmt_d_nm,
    c.hier_d_sales_mgmt_e_nm,
    c.plan_to_nm,
    c.chain_nm,
    c.tdlinx_store_nbr,
    c.tdlinx_street_nm,
    c.tdlinx_city_nm,
    c.tdlinx_rgn_nm,
    c.tdlinx_place_nm,
    c.tdlinx_pstl_cd,
    c.lat_val,
    c.lon_val,
    c.tdlinx_lat_lon_cd,
    c.tdlinx_trade_class_cd,
    c.tdlinx_owner_cd,
    c.tdlinx_owner_fmly_cd,
    c.tdlinx_owner_nm,
    c.tdlinx_owner_city_nm,
    c.tdlinx_owner_rgn_cd,
    c.tdlinx_splr_fmly_cd,
    c.tdlinx_splr_nm,
    c.tdlinx_splr_city_nm,
    c.tdlinx_splr_rgn_cd,
    c.tdlinx_ctry_cd,
    c.lng_cd,
    c.hash_key,
    -- generating column hash_key
    c.kortex_dprct_ts,
    c.kortex_upld_ts,
    c.src_nm,
    a.chnl_nm
    from
        int_cust c
        left join 
            (select distinct a.chnl_nm, a.KDGRP from kna_ecc_cust_mstr_t151t a where a.SPRAS = 'E') as a 
        on coalesce(c.chnl_cd, '') = coalesce(a.kdgrp,'')
)

select * from final