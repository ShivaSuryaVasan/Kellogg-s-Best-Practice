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
        materialized='ephemeral'
    )
}}

with kna_ecc_cust_mstr_kna1 as (
    select * from {{ source('stg_kna_ecc_cust_mstr', 'kna_ecc_cust_mstr_kna1') }}
),

kna_ecc_cust_mstr_t077x as (
    select * from {{ source('stg_kna_ecc_cust_mstr', 'kna_ecc_cust_mstr_t077x') }}
),

kna_ecc_ctry_mstr_t005t as (
    select * from {{ source('stg_kna_ecc_ctry_mstr', 'kna_ecc_ctry_mstr_t005t') }}
),

kna_ecc_lng_mstr_t002 as (
    select * from {{ source('stg_kna_ecc_lng_mstr', 'kna_ecc_lng_mstr_t002') }}
),

kna_ecc_cust_mstr_knvv as (
    select distinct kunnr, kdgrp
    from 
        {{ source('stg_kna_ecc_cust_mstr', 'kna_ecc_cust_mstr_knvv') }}
    where 
        trim(kdgrp) <> ''
),

hier_knvv as (
    select
        hv.kdgrp,
        h.plan_to_nbr,
        h.sold_to_nbr,
        h.last_level_cust_nbr,
        h.hier_tmfrm_id,
        h.hier_id
    from 
        {{ ref('cust_hier_d') }} h
    left outer join
        kna_ecc_cust_mstr_knvv hv
        on h.plan_to_nbr = hv.kunnr
        and h.last_level_cust_nbr is not null
),

sales_user_store_loc_store as (
    select * from
        {{ source('stg_sales_user_store_loc','"sales_user_store_loc_store-location"') }}
),

cust_hier_a as (
    select * from {{ ref('cust_hier_a') }} 
),

cust_hier_b as (
    select * from {{ ref('cust_hier_b') }}
),

cust_hier_c as (
    select * from {{ ref('cust_hier_c') }}
),

cust_hier_d as (
    select * from {{ ref('cust_hier_d') }}
),

tdlinx_cust_mstr as (
    select
        tkm.rclientno,
        tks.stdlinxscd,
        tks.sstreetadd,
        tks.scity,
        tks.sst,
        tks.splacenm,
        tks.szip,
        tks.slat,
        tks.slong,
        tks.slatlongcd,
        tks.sno,
        tks.stradeclcd,
        tks.sname,
        tks.stdlinxocd,
        tks.sownfamcd,
        tks.sownnm,
        tks.sowncity,
        tks.sownst,
        tks.ssupfamcd,
        tks.ssupnm,
        tks.ssupcity,
        tks.ssupst,
        tks.mcountynm
    from
        {{ source('stg_tdlinx_cust_mstr', 'tdlinx_cust_mstr_kellgstr') }} tks
    join
        {{ source('stg_tdlinx_cust_mstr', 'tdlinx_cust_mstr_kellgrs1') }} tkm
        on tkm.rtdlinxscd = tks.stdlinxscd
),

kna_ecc_addr_mstr_adrc as (
    select distinct addrnumber, sort2
    from 
        {{ source('stg_kna_ecc_addr_mstr', 'kna_ecc_addr_mstr_adrc') }}
    where 
        trim(sort2) <> ''
),

cust_spin as (
    select * from {{ source('stg_cust_spin', 'cust_spin') }}
),

final as (
    select distinct
        k.cust_nbr,
        k.cust_nm,
        k.cust2_nm,
        ad.sort2,
        k.street_nm,
        k.city_nm,
        k.rgn_cd,
        k.distrct_nm,
        k.ctry_cd,
        k.pstl_cd,
        k.store_nbr,
        k.plan_to_cd,
        k.chain_ind,
        k.cre_dt,
        k.duns_nbr,
        k.duns4_nbr,
        k.store_cd,
        k.altn_payer_nbr,
        k.cust_type_cd,
        k.acct_group_cd,
        t.acct_group_desc,
        k.pymt_blok_cd,
        k.cust_pref_brkt_dry_cd,
        k.cust_pref_brkt_frz_cd,
        k.cust_fix_brkt_dry_cd,
        k.cust_fix_brkt_frz_cd,
        k.cust_ovrd_brkt_cd,
        k.altn_payer_acct_cd,
        k.order_blok_cd,
        tc.ctry_nm,
        k.del_ind,
        case
            when coalesce(trim(v.kdgrp), '') = '' 
            then hd.kdgrp 
            else v.kdgrp
        end as chnl_cd,
        -- generating chnl_cd
        cf.area_lat_val,
        cf.area_lon_val,
        cf.terr_lat_val,
        cf.terr_lon_val,
        a.co_nbr as hier_a_co_nbr,
        a.ctry_nbr as hier_a_ctry_nbr,
        a.level3_misc_nbr as hier_a_level3_misc_nbr,
        a.level4_misc_nbr as hier_a_level4_misc_nbr,
        a.level5_misc_nbr as hier_a_level5_misc_nbr,
        a.level6_misc_nbr as hier_a_level6_misc_nbr,
        a.level7_misc_nbr as hier_a_level7_misc_nbr,
        a.level8_misc_nbr as hier_a_level8_misc_nbr,
        a.level9_misc_nbr as hier_a_level9_misc_nbr,
        a.level10_misc_nbr as hier_a_level10_misc_nbr,
        a.co_nm as hier_a_co_nm,
        a.ctry_nm as hier_a_ctry_nm,
        a.level3_misc_nm as hier_a_level3_misc_nm,
        a.level4_misc_nm as hier_a_level4_misc_nm,
        a.level5_misc_nm as hier_a_level5_misc_nm,
        a.level6_misc_nm as hier_a_level6_misc_nm,
        a.level7_misc_nm as hier_a_level7_misc_nm,
        a.level8_misc_nm as hier_a_level8_misc_nm,
        a.level9_misc_nm as hier_a_level9_misc_nm,
        a.level10_misc_nm as hier_a_level10_misc_nm,
        b.co_nbr as hier_b_co_nbr,
        b.ctry_nbr as hier_b_ctry_nbr,
        b.chnl_nbr as hier_b_chnl_nbr,
        b.level4_misc_nbr as hier_b_level4_misc_nbr,
        b.rgn_nbr as hier_b_rgn_nbr,
        b.area_nbr as hier_b_area_nbr,
        b.terr_nbr as hier_b_terr_nbr,
        b.co_nm as hier_b_co_nm,
        b.ctry_nm as hier_b_ctry_nm,
        b.chnl_nm as hier_b_chnl_nm,
        b.level4_misc_nm as hier_b_level4_misc_nm,
        b.rgn_nm as hier_b_rgn_nm,
        b.area_nm as hier_b_area_nm,
        b.terr_nm as hier_b_terr_nm,
        c.co_nbr as hier_c_co_nbr,
        c.ctry_nbr as hier_c_ctry_nbr,
        c.chnl_nbr as hier_c_chnl_nbr,
        c.rgn_nbr as hier_c_rgn_nbr,
        c.zone_nbr as hier_c_zone_nbr,
        c.distrct_nbr as hier_c_distrct_nbr,
        c.terr_nbr as hier_c_terr_nbr,
        c.co_nm as hier_c_co_nm,
        c.ctry_nm as hier_c_ctry_nm,
        c.chnl_nm as hier_c_chnl_nm,
        c.rgn_nm as hier_c_rgn_nm,
        c.zone_nm as hier_c_zone_nm,
        c.distrct_nm as hier_c_distrct_nm,
        c.terr_nm as hier_c_terr_nm,
        d.co_nbr as hier_d_co_nbr,
        d.ctry_nbr as hier_d_ctry_nbr,
        d.sales_mgmt_a_nbr as hier_d_sales_mgmt_a_nbr,
        d.sales_mgmt_b_nbr as hier_d_sales_mgmt_b_nbr,
        d.sales_mgmt_c_nbr as hier_d_sales_mgmt_c_nbr,
        d.sales_mgmt_d_nbr as hier_d_sales_mgmt_d_nbr,
        d.sales_mgmt_e_nbr as hier_d_sales_mgmt_e_nbr,
        d.plan_to_nbr as plan_to_nbr,
        d.chain_nbr as chain_nbr,
        d.co_nm as hier_d_co_nm,
        d.ctry_nm as hier_d_ctry_nm,
        d.sales_mgmt_a_nm as hier_d_sales_mgmt_a_nm,
        d.sales_mgmt_b_nm as hier_d_sales_mgmt_b_nm,
        d.sales_mgmt_c_nm as hier_d_sales_mgmt_c_nm,
        d.sales_mgmt_d_nm as hier_d_sales_mgmt_d_nm,
        d.sales_mgmt_e_nm as hier_d_sales_mgmt_e_nm,
        d.plan_to_nm as plan_to_nm,
        d.chain_nm as chain_nm,
        tdlinx.sno as tdlinx_store_nbr,
        tdlinx.sstreetadd as tdlinx_street_nm,
        tdlinx.scity as tdlinx_city_nm,
        tdlinx.sst as tdlinx_rgn_nm,
        tdlinx.splacenm as tdlinx_place_nm,
        tdlinx.szip as tdlinx_pstl_cd,
        tdlinx.slat as lat_val,
        tdlinx.slong as lon_val,
        tdlinx.slatlongcd as tdlinx_lat_lon_cd,
        tdlinx.stradeclcd as tdlinx_trade_class_cd,
        tdlinx.stdlinxocd as tdlinx_owner_cd,
        tdlinx.sownfamcd as tdlinx_owner_fmly_cd,
        tdlinx.sownnm as tdlinx_owner_nm,
        tdlinx.sowncity as tdlinx_owner_city_nm,
        tdlinx.sownst as tdlinx_owner_rgn_cd,
        tdlinx.ssupfamcd as tdlinx_splr_fmly_cd,
        tdlinx.ssupnm as tdlinx_splr_nm,
        tdlinx.ssupcity as tdlinx_splr_city_nm,
        tdlinx.ssupst as tdlinx_splr_rgn_cd,
        tdlinx.mcountynm as tdlinx_ctry_cd,
        l.lng_cd,
        md5(k.src_nm || k.kunnr) as hash_key,
        -- generating column hash_key
        k.kortex_dprct_ts,
        k.kortex_upld_ts,
        k.src_nm
    from kna_ecc_cust_mstr_kna1 k
        left outer join kna_ecc_cust_mstr_t077x t on t.ktokd = k.ktokd and t.spras = 'E'
        left outer join kna_ecc_ctry_mstr_t005t tc on k.land1 = tc.land1 and tc.spras = 'E'
        left outer join kna_ecc_lng_mstr_t002 l on l.spras = 'E'
        left outer join kna_ecc_cust_mstr_knvv v on v.kunnr = k.kunnr
        left outer join hier_knvv hd on k.kunnr = hd.last_level_cust_nbr and hd.hier_id = 'D' and hd.hier_tmfrm_id = 'CURRENT' and k.ktokd = '0001'
        left outer join sales_user_store_loc_store cf on k.kunnr = cf.external_id
        left outer join cust_hier_d d on k.kunnr = d.sold_to_nbr and d.hier_tmfrm_id = 'CURRENT'
        left outer join cust_hier_a a on k.kunnr = a.last_level_cust_nbr and a.hier_tmfrm_id = 'CURRENT'
        left outer join cust_hier_b b on k.kunnr = b.sold_to_nbr and b.hier_tmfrm_id = 'CURRENT'
        left outer join cust_hier_c as c on k.kunnr = c.sold_to_nbr and c.hier_tmfrm_id = 'CURRENT'
        left outer join tdlinx_cust_mstr as tdlinx on tdlinx.rclientno = k.kunnr
        left outer join kna_ecc_addr_mstr_adrc ad on ad.addrnumber = k.adrnr
        left outer join cust_spin cs on k.kunnr = cs.cust_nbr
    where cs.spin_owner_nm <> 'CEREAL'
)

select * from final