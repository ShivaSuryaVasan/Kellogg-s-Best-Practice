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
1.0        07/24/2024    Surya           Initial Version.
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

kna_ecc_cust_mstr_knvh as (
    select * from {{ source('stg_','kna_ecc_cust_mstr_knvh') }}
),

cust_spin as (
    select * from {{ source('stg_','cust_spin') }}
),

kna1_base as (
    select 
        distinct kunnr, name1
    from 
        kna_ecc_cust_mstr_kna1 as kna
    left outer join 
        cust_spin as custspin on kna.kunnr = custspin.cust_nbr
    where 
        custspin.spin_owner_nm <> 'CEREAL';
),

lv1 as ( 
    select 
        kunnr, 
        hkunnr, 
        hityp, 
        max(kortex_upld_ts) as kortex_upld_ts, 
        max(kortex_dprct_ts) as kortex_dprct_ts, 
        src_nm  
    from
        kna_ecc_cust_mstr_knvh
    where 
        datab <= current_date  and datbi >=  current_date and hityp = 'D' 
    group by 
        kunnr, hkunnr, hityp, src_nm
),
    
name as (
    select kunnr, name1 
    from kna1_base
),

cust_hier_unrvl_i_cur_d as (

    --level 1
    select hier_dt, lv1.hier_id, lv1.level1_nbr, n1.level1_nm,
    ' ' as level2_nbr, ' ' as level2_nm,
    ' ' as level3_nbr, ' ' as level3_nm,
    ' ' as level4_nbr, ' 'as level4_nm,
    ' ' as level5_nbr, ' ' as level5_nm,
    ' ' as level6_nbr, ' ' as level6_nm,
    ' ' as level7_nbr, ' ' as level7_nm,
    ' ' as level8_nbr, ' ' as level8_nm,
    ' ' as level9_nbr, ' ' as level9_nm,
    ' ' as level10_nbr, ' ' as level10_nm,
    ' ' as level11_nbr, ' ' as level11_nm,
    ' ' as level12_nbr, ' ' as level12_nm,
    ' ' as level13_nbr, ' ' as level13_nm,
    ' ' as level14_nbr, ' ' as level14_nm,
    ' ' as level15_nbr, ' ' as level15_nm,
    ' ' as level16_nbr, ' ' as level16_nm,
    ' ' as level17_nbr, ' ' as level17_nm,
    ' ' as level18_nbr, ' ' as level18_nm,
    ' ' as level19_nbr, ' ' as level19_nm,
    ' ' as level20_nbr, ' ' as level20_nm,
    '1' as lst_level_nbr, lv1.lst_level_cust_nbr,
    lv1.kortex_upld_ts, lv1.kortex_dprct_ts, lv1.src_nm
    from lv1
    inner join name as n1 on lv1.kunnr = n1.kunnr
    where lv1.hkunnr = ' '

    union all
    --level 2

    select current_date as hier_dt, lv1.hier_id, lv1.level1_nbr, n1.level1_nm,
    lv2.level2_nbr, n2.level2_nm,
    ' ' as level3_nbr, ' ' as level3_nm,
    ' ' as level4_nbr, ' 'as level4_nm,
    ' ' as level5_nbr, ' ' as level5_nm,
    ' ' as level6_nbr, ' ' as level6_nm,
    ' ' as level7_nbr, ' ' as level7_nm,
    ' ' as level8_nbr, ' ' as level8_nm,
    ' ' as level9_nbr, ' ' as level9_nm,
    ' ' as level10_nbr, ' ' as level10_nm,
    ' ' as level11_nbr, ' ' as level11_nm,
    ' ' as level12_nbr, ' ' as level12_nm,
    ' ' as level13_nbr, ' ' as level13_nm,
    ' ' as level14_nbr, ' ' as level14_nm,
    ' ' as level15_nbr, ' ' as level15_nm,
    ' ' as level16_nbr, ' ' as level16_nm,
    ' ' as level17_nbr, ' ' as level17_nm,
    ' ' as level18_nbr, ' ' as level18_nm,
    ' ' as level19_nbr, ' ' as level19_nm,
    ' ' as level20_nbr, ' ' as level20_nm,
    '2' as lst_level_nbr, lv2.lst_level_cust_nbr,
    lv2.kortex_upld_ts, lv2.kortex_dprct_ts, lv2.src_nm
    from lv1
    inner join name as n1 on lv1.kunnr = n1.kunnr
    inner join lv1 as lv2 on lv2.hkunnr = lv1.kunnr
    inner join name as n2 on lv2.kunnr = n2.kunnr
    where lv1.hkunnr = ' '

    union all
    --level 3

    select current_date as hier_dt, lv1.hier_id, lv1.level1_nbr, n1.level1_nm,
    lv2.level2_nbr, n2.level2_nm,
    lv3.level3_nbr, n3.level3_nm,
    ' ' as level4_nbr, ' 'as level4_nm,
    ' ' as level5_nbr, ' ' as level5_nm,
    ' ' as level6_nbr, ' ' as level6_nm,
    ' ' as level7_nbr, ' ' as level7_nm,
    ' ' as level8_nbr, ' ' as level8_nm,
    ' ' as level9_nbr, ' ' as level9_nm,
    ' ' as level10_nbr, ' ' as level10_nm,
    ' ' as level11_nbr, ' ' as level11_nm,
    ' ' as level12_nbr, ' ' as level12_nm,
    ' ' as level13_nbr, ' ' as level13_nm,
    ' ' as level14_nbr, ' ' as level14_nm,
    ' ' as level15_nbr, ' ' as level15_nm,
    ' ' as level16_nbr, ' ' as level16_nm,
    ' ' as level17_nbr, ' ' as level17_nm,
    ' ' as level18_nbr, ' ' as level18_nm,
    ' ' as level19_nbr, ' ' as level19_nm,
    ' ' as level20_nbr, ' ' as level20_nm,
    '3' as lst_level_nbr, lv3.lst_level_cust_nbr,
    lv3.kortex_upld_ts, lv3.kortex_dprct_ts, lv3.src_nm
    from lv1
    inner join name as n1 on lv1.kunnr = n1.kunnr
    inner join lv1 as lv2 on lv2.hkunnr = lv1.kunnr
    inner join name as n2 on lv2.kunnr = n2.kunnr
    inner join lv1 as lv3 on lv3.hkunnr = lv2.kunnr
    inner join name as n3 on lv3.kunnr = n3.kunnr
    where lv1.hkunnr = ' '

    union all
    --level 4

    select current_date as hier_dt, lv1.hier_id, lv1.level1_nbr, n1.level1_nm,
    lv2.level2_nbr, n2.level2_nm,
    lv3.level3_nbr, n3.level3_nm,
    lv4.level4_nbr, n4.level4_nm,
    ' ' as level5_nbr, ' ' as level5_nm,
    ' ' as level6_nbr, ' ' as level6_nm,
    ' ' as level7_nbr, ' ' as level7_nm,
    ' ' as level8_nbr, ' ' as level8_nm,
    ' ' as level9_nbr, ' ' as level9_nm,
    ' ' as level10_nbr, ' ' as level10_nm,
    ' ' as level11_nbr, ' ' as level11_nm,
    ' ' as level12_nbr, ' ' as level12_nm,
    ' ' as level13_nbr, ' ' as level13_nm,
    ' ' as level14_nbr, ' ' as level14_nm,
    ' ' as level15_nbr, ' ' as level15_nm,
    ' ' as level16_nbr, ' ' as level16_nm,
    ' ' as level17_nbr, ' ' as level17_nm,
    ' ' as level18_nbr, ' ' as level18_nm,
    ' ' as level19_nbr, ' ' as level19_nm,
    ' ' as level20_nbr, ' ' as level20_nm,
    '4' as lst_level_nbr, lv4.lst_level_cust_nbr,
    lv4.kortex_upld_ts, lv4.kortex_dprct_ts, lv4.src_nm
    from lv1
    inner join name as n1 on lv1.kunnr = n1.kunnr
    inner join lv1 as lv2 on lv2.hkunnr = lv1.kunnr
    inner join name as n2 on lv2.kunnr = n2.kunnr
    inner join lv1 as lv3 on lv3.hkunnr = lv2.kunnr
    inner join name as n3 on lv3.kunnr = n3.kunnr
    inner join lv1 as lv4 on lv4.hkunnr = lv3.kunnr
    inner join name as n4 on lv4.kunnr = n4.kunnr
    where lv1.hkunnr = ' '

    union all

    --level 5

    select current_date as hier_dt, lv1.hier_id, lv1.level1_nbr, n1.level1_nm,
    lv2.level2_nbr, n2.level2_nm,
    lv3.level3_nbr, n3.level3_nm,
    lv4.level4_nbr, n4.level4_nm,
    lv5.level4_nbr, n5.level5_nm,
    ' ' as level6_nbr, ' ' as level6_nm,
    ' ' as level7_nbr, ' ' as level7_nm,
    ' ' as level8_nbr, ' ' as level8_nm,
    ' ' as level9_nbr, ' ' as level9_nm,
    ' ' as level10_nbr, ' ' as level10_nm,
    ' ' as level11_nbr, ' ' as level11_nm,
    ' ' as level12_nbr, ' ' as level12_nm,
    ' ' as level13_nbr, ' ' as level13_nm,
    ' ' as level14_nbr, ' ' as level14_nm,
    ' ' as level15_nbr, ' ' as level15_nm,
    ' ' as level16_nbr, ' ' as level16_nm,
    ' ' as level17_nbr, ' ' as level17_nm,
    ' ' as level18_nbr, ' ' as level18_nm,
    ' ' as level19_nbr, ' ' as level19_nm,
    ' ' as level20_nbr, ' ' as level20_nm,
    '5' as lst_level_nbr, lv5.lst_level_cust_nbr,
    lv5.kortex_upld_ts, lv5.kortex_dprct_ts, lv5.src_nm
    from lv1
    inner join name as n1 on lv1.kunnr = n1.kunnr
    inner join lv1 as lv2 on lv2.hkunnr = lv1.kunnr
    inner join name as n2 on lv2.kunnr = n2.kunnr
    inner join lv1 as lv3 on lv3.hkunnr = lv2.kunnr
    inner join name as n3 on lv3.kunnr = n3.kunnr
    inner join lv1 as lv4 on lv4.hkunnr = lv3.kunnr
    inner join name as n4 on lv4.kunnr = n4.kunnr
    inner join lv1 as lv5 on lv5.hkunnr = lv4.kunnr
    inner join name as n5 on lv5.kunnr = n5.kunnr
    where lv1.hkunnr = ' '

    union all
    --level 6

    select current_date as hier_dt, lv1.hier_id, lv1.level1_nbr, n1.level1_nm,
    lv2.level2_nbr, n2.level2_nm,
    lv3.level3_nbr, n3.level3_nm,
    lv4.level4_nbr, n4.level4_nm,
    lv5.level5_nbr, n5.level5_nm,
    lv6.level6_nbr, n6.level6_nm,
    ' ' as level7_nbr, ' ' as level7_nm,
    ' ' as level8_nbr, ' ' as level8_nm,
    ' ' as level9_nbr, ' ' as level9_nm,
    ' ' as level10_nbr, ' ' as level10_nm,
    ' ' as level11_nbr, ' ' as level11_nm,
    ' ' as level12_nbr, ' ' as level12_nm,
    ' ' as level13_nbr, ' ' as level13_nm,
    ' ' as level14_nbr, ' ' as level14_nm,
    ' ' as level15_nbr, ' ' as level15_nm,
    ' ' as level16_nbr, ' ' as level16_nm,
    ' ' as level17_nbr, ' ' as level17_nm,
    ' ' as level18_nbr, ' ' as level18_nm,
    ' ' as level19_nbr, ' ' as level19_nm,
    ' ' as level20_nbr, ' ' as level20_nm,
    '6' as lst_level_nbr, lv6.lst_level_cust_nbr,
    lv6.kortex_upld_ts, lv6.kortex_dprct_ts, lv6.src_nm
    from lv1
    inner join name as n1 on lv1.kunnr = n1.kunnr
    inner join lv1 as lv2 on lv2.hkunnr = lv1.kunnr
    inner join name as n2 on lv2.kunnr = n2.kunnr
    inner join lv1 as lv3 on lv3.hkunnr = lv2.kunnr
    inner join name as n3 on lv3.kunnr = n3.kunnr
    inner join lv1 as lv4 on lv4.hkunnr = lv3.kunnr
    inner join name as n4 on lv4.kunnr = n4.kunnr
    inner join lv1 as lv5 on lv5.hkunnr = lv4.kunnr
    inner join name as n5 on lv5.kunnr = n5.kunnr
    inner join lv1 as lv6 on lv6.hkunnr = lv5.kunnr
    inner join name as n6 on lv6.kunnr = n6.kunnr
    where lv1.hkunnr = ' '

    union all
    --level 7

    select current_date as hier_dt, lv1.hier_id, lv1.level1_nbr, n1.level1_nm,
    lv2.level2_nbr, n2.level2_nm,
    lv3.level3_nbr, n3.level3_nm,
    lv4.level4_nbr, n4.level4_nm,
    lv5.level5_nbr, n5.level5_nm,
    lv6.level6_nbr, n6.level6_nm,
    lv7.level7_nbr, n7.level7_nm,
    ' ' as level8_nbr, ' ' as level8_nm,
    ' ' as level9_nbr, ' ' as level9_nm,
    ' ' as level10_nbr, ' ' as level10_nm,
    ' ' as level11_nbr, ' ' as level11_nm,
    ' ' as level12_nbr, ' ' as level12_nm,
    ' ' as level13_nbr, ' ' as level13_nm,
    ' ' as level14_nbr, ' ' as level14_nm,
    ' ' as level15_nbr, ' ' as level15_nm,
    ' ' as level16_nbr, ' ' as level16_nm,
    ' ' as level17_nbr, ' ' as level17_nm,
    ' ' as level18_nbr, ' ' as level18_nm,
    ' ' as level19_nbr, ' ' as level19_nm,
    ' ' as level20_nbr, ' ' as level20_nm,
    '7' as lst_level_nbr, lv7.lst_level_cust_nbr,
    lv7.kortex_upld_ts, lv7.kortex_dprct_ts, lv7.src_nm
    from lv1
    inner join name as n1 on lv1.kunnr = n1.kunnr
    inner join lv1 as lv2 on lv2.hkunnr = lv1.kunnr
    inner join name as n2 on lv2.kunnr = n2.kunnr
    inner join lv1 as lv3 on lv3.hkunnr = lv2.kunnr
    inner join name as n3 on lv3.kunnr = n3.kunnr
    inner join lv1 as lv4 on lv4.hkunnr = lv3.kunnr
    inner join name as n4 on lv4.kunnr = n4.kunnr
    inner join lv1 as lv5 on lv5.hkunnr = lv4.kunnr
    inner join name as n5 on lv5.kunnr = n5.kunnr
    inner join lv1 as lv6 on lv6.hkunnr = lv5.kunnr
    inner join name as n6 on lv6.kunnr = n6.kunnr
    inner join lv1 as lv7 on lv7.hkunnr = lv6.kunnr
    inner join name as n7 on lv7.kunnr = n7.kunnr
    where lv1.hkunnr = ' '

    union all
    --level 8

    select current_date as hier_dt, lv1.hier_id, lv1.level1_nbr, n1.level1_nm,
    lv2.level2_nbr, n2.level2_nm,
    lv3.level3_nbr, n3.level3_nm,
    lv4.level4_nbr, n4.level4_nm,
    lv5.level5_nbr, n5.level5_nm,
    lv6.level6_nbr, n6.level6_nm,
    lv7.level7_nbr, n7.level7_nm,
    lv8.level8_nbr, n8.level8_nm,
    ' ' as level9_nbr, ' ' as level9_nm,
    ' ' as level10_nbr, ' ' as level10_nm,
    ' ' as level11_nbr, ' ' as level11_nm,
    ' ' as level12_nbr, ' ' as level12_nm,
    ' ' as level13_nbr, ' ' as level13_nm,
    ' ' as level14_nbr, ' ' as level14_nm,
    ' ' as level15_nbr, ' ' as level15_nm,
    ' ' as level16_nbr, ' ' as level16_nm,
    ' ' as level17_nbr, ' ' as level17_nm,
    ' ' as level18_nbr, ' ' as level18_nm,
    ' ' as level19_nbr, ' ' as level19_nm,
    ' ' as level20_nbr, ' ' as level20_nm,
    '8' as lst_level_nbr, lv8.lst_level_cust_nbr,
    lv8.kortex_upld_ts, lv8.kortex_dprct_ts, lv8.src_nm
    from lv1
    inner join name as n1 on lv1.kunnr = n1.kunnr
    inner join lv1 as lv2 on lv2.hkunnr = lv1.kunnr
    inner join name as n2 on lv2.kunnr = n2.kunnr
    inner join lv1 as lv3 on lv3.hkunnr = lv2.kunnr
    inner join name as n3 on lv3.kunnr = n3.kunnr
    inner join lv1 as lv4 on lv4.hkunnr = lv3.kunnr
    inner join name as n4 on lv4.kunnr = n4.kunnr
    inner join lv1 as lv5 on lv5.hkunnr = lv4.kunnr
    inner join name as n5 on lv5.kunnr = n5.kunnr
    inner join lv1 as lv6 on lv6.hkunnr = lv5.kunnr
    inner join name as n6 on lv6.kunnr = n6.kunnr
    inner join lv1 as lv7 on lv7.hkunnr = lv6.kunnr
    inner join name as n7 on lv7.kunnr = n7.kunnr
    inner join lv1 as lv8 on lv8.hkunnr = lv7.kunnr
    inner join name as n8 on lv8.kunnr = n8.kunnr
    where lv1.hkunnr = ' '

    union all
    --level 9

    select current_date as hier_dt, lv1.hier_id, lv1.level1_nbr, n1.level1_nm,
    lv2.level2_nbr, n2.level2_nm,
    lv3.level3_nbr, n3.level3_nm,
    lv4.level4_nbr, n4.level4_nm,
    lv5.level5_nbr, n5.level5_nm,
    lv6.level6_nbr, n6.level6_nm,
    lv7.level7_nbr, n7.level7_nm,
    lv8.level8_nbr, n8.level8_nm,
    lv9.level9_nbr, n9.level9_nm,
    ' ' as level10_nbr, ' ' as level10_nm,
    ' ' as level11_nbr, ' ' as level11_nm,
    ' ' as level12_nbr, ' ' as level12_nm,
    ' ' as level13_nbr, ' ' as level13_nm,
    ' ' as level14_nbr, ' ' as level14_nm,
    ' ' as level15_nbr, ' ' as level15_nm,
    ' ' as level16_nbr, ' ' as level16_nm,
    ' ' as level17_nbr, ' ' as level17_nm,
    ' ' as level18_nbr, ' ' as level18_nm,
    ' ' as level19_nbr, ' ' as level19_nm,
    ' ' as level20_nbr, ' ' as level20_nm,
    '9' as lst_level_nbr, lv9.lst_level_cust_nbr,
    lv9.kortex_upld_ts, lv9.kortex_dprct_ts, lv9.src_nm
    from lv1
    inner join name as n1 on lv1.kunnr = n1.kunnr
    inner join lv1 as lv2 on lv2.hkunnr = lv1.kunnr
    inner join name as n2 on lv2.kunnr = n2.kunnr
    inner join lv1 as lv3 on lv3.hkunnr = lv2.kunnr
    inner join name as n3 on lv3.kunnr = n3.kunnr
    inner join lv1 as lv4 on lv4.hkunnr = lv3.kunnr
    inner join name as n4 on lv4.kunnr = n4.kunnr
    inner join lv1 as lv5 on lv5.hkunnr = lv4.kunnr
    inner join name as n5 on lv5.kunnr = n5.kunnr
    inner join lv1 as lv6 on lv6.hkunnr = lv5.kunnr
    inner join name as n6 on lv6.kunnr = n6.kunnr
    inner join lv1 as lv7 on lv7.hkunnr = lv6.kunnr
    inner join name as n7 on lv7.kunnr = n7.kunnr
    inner join lv1 as lv8 on lv8.hkunnr = lv7.kunnr
    inner join name as n8 on lv8.kunnr = n8.kunnr
    inner join lv1 as lv9 on lv9.hkunnr = lv8.kunnr
    inner join name as n9 on lv9.kunnr = n9.kunnr
    where lv1.hkunnr = ' '

    union all
    --level 10

    select current_date as hier_dt, lv1.hier_id, lv1.level1_nbr, n1.level1_nm,
    lv2.level2_nbr, n2.level2_nm,
    lv3.level3_nbr, n3.level3_nm,
    lv4.level4_nbr, n4.level4_nm,
    lv5.level5_nbr, n5.level5_nm,
    lv6.level6_nbr, n6.level6_nm,
    lv7.level7_nbr, n7.level7_nm,
    lv8.level8_nbr, n8.level8_nm,
    lv9.level9_nbr, n9.level9_nm,
    lv10.level10_nbr, n10.level10_nm,
    ' ' as level11_nbr, ' ' as level11_nm,
    ' ' as level12_nbr, ' ' as level12_nm,
    ' ' as level13_nbr, ' ' as level13_nm,
    ' ' as level14_nbr, ' ' as level14_nm,
    ' ' as level15_nbr, ' ' as level15_nm,
    ' ' as level16_nbr, ' ' as level16_nm,
    ' ' as level17_nbr, ' ' as level17_nm,
    ' ' as level18_nbr, ' ' as level18_nm,
    ' ' as level19_nbr, ' ' as level19_nm,
    ' ' as level20_nbr, ' ' as level20_nm,
    '10' as lst_level_nbr, lv10.lst_level_cust_nbr,
    lv10.kortex_upld_ts, lv10.kortex_dprct_ts, lv10.src_nm
    from lv1
    inner join name as n1 on lv1.kunnr = n1.kunnr
    inner join lv1 as lv2 on lv2.hkunnr = lv1.kunnr
    inner join name as n2 on lv2.kunnr = n2.kunnr
    inner join lv1 as lv3 on lv3.hkunnr = lv2.kunnr
    inner join name as n3 on lv3.kunnr = n3.kunnr
    inner join lv1 as lv4 on lv4.hkunnr = lv3.kunnr
    inner join name as n4 on lv4.kunnr = n4.kunnr
    inner join lv1 as lv5 on lv5.hkunnr = lv4.kunnr
    inner join name as n5 on lv5.kunnr = n5.kunnr
    inner join lv1 as lv6 on lv6.hkunnr = lv5.kunnr
    inner join name as n6 on lv6.kunnr = n6.kunnr
    inner join lv1 as lv7 on lv7.hkunnr = lv6.kunnr
    inner join name as n7 on lv7.kunnr = n7.kunnr
    inner join lv1 as lv8 on lv8.hkunnr = lv7.kunnr
    inner join name as n8 on lv8.kunnr = n8.kunnr
    inner join lv1 as lv9 on lv9.hkunnr = lv8.kunnr
    inner join name as n9 on lv9.kunnr = n9.kunnr
    inner join lv1 as lv10 on lv10.hkunnr = lv9.kunnr
    inner join name as n10 on lv10.kunnr = n10.kunnr
    where lv1.hkunnr = ' '

    union all
    --level 11

    select current_date as hier_dt, lv1.hier_id, lv1.level1_nbr, n1.level1_nm,
    lv2.level2_nbr, n2.level2_nm,
    lv3.level3_nbr, n3.level3_nm,
    lv4.level4_nbr, n4.level4_nm,
    lv5.level5_nbr, n5.level5_nm,
    lv6.level6_nbr, n6.level6_nm,
    lv7.level7_nbr, n7.level7_nm,
    lv8.level8_nbr, n8.level8_nm,
    lv9.level9_nbr, n9.level9_nm,
    lv10.level10_nbr, n10.level10_nm,
    lv11.level11_nbr, n11.level11_nm,
    ' ' as level12_nbr, ' ' as level12_nm,
    ' ' as level13_nbr, ' ' as level13_nm,
    ' ' as level14_nbr, ' ' as level14_nm,
    ' ' as level15_nbr, ' ' as level15_nm,
    ' ' as level16_nbr, ' ' as level16_nm,
    ' ' as level17_nbr, ' ' as level17_nm,
    ' ' as level18_nbr, ' ' as level18_nm,
    ' ' as level19_nbr, ' ' as level19_nm,
    ' ' as level20_nbr, ' ' as level20_nm,
    '11' as lst_level_nbr, lv11.lst_level_cust_nbr,
    lv11.kortex_upld_ts, lv11.kortex_dprct_ts, lv11.src_nm
    from lv1
    inner join name as n1 on lv1.kunnr = n1.kunnr
    inner join lv1 as lv2 on lv2.hkunnr = lv1.kunnr
    inner join name as n2 on lv2.kunnr = n2.kunnr
    inner join lv1 as lv3 on lv3.hkunnr = lv2.kunnr
    inner join name as n3 on lv3.kunnr = n3.kunnr
    inner join lv1 as lv4 on lv4.hkunnr = lv3.kunnr
    inner join name as n4 on lv4.kunnr = n4.kunnr
    inner join lv1 as lv5 on lv5.hkunnr = lv4.kunnr
    inner join name as n5 on lv5.kunnr = n5.kunnr
    inner join lv1 as lv6 on lv6.hkunnr = lv5.kunnr
    inner join name as n6 on lv6.kunnr = n6.kunnr
    inner join lv1 as lv7 on lv7.hkunnr = lv6.kunnr
    inner join name as n7 on lv7.kunnr = n7.kunnr
    inner join lv1 as lv8 on lv8.hkunnr = lv7.kunnr
    inner join name as n8 on lv8.kunnr = n8.kunnr
    inner join lv1 as lv9 on lv9.hkunnr = lv8.kunnr
    inner join name as n9 on lv9.kunnr = n9.kunnr
    inner join lv1 as lv10 on lv10.hkunnr = lv9.kunnr
    inner join name as n10 on lv10.kunnr = n10.kunnr
    inner join lv1 as lv11 on lv11.hkunnr = lv10.kunnr
    inner join name as n11 on lv11.kunnr = n11.kunnr
    where lv1.hkunnr = ' '

    union all
    --level 12

    select current_date as hier_dt, lv1.hier_id, lv1.level1_nbr, n1.level1_nm,
    lv2.level2_nbr, n2.level2_nm,
    lv3.level3_nbr, n3.level3_nm,
    lv4.level4_nbr, n4.level4_nm,
    lv5.level5_nbr, n5.level5_nm,
    lv6.level6_nbr, n6.level6_nm,
    lv7.level7_nbr, n7.level7_nm,
    lv8.level8_nbr, n8.level8_nm,
    lv9.level9_nbr, n9.level9_nm,
    lv10.level10_nbr, n10.level10_nm,
    lv11.level11_nbr, n11.level11_nm,
    lv12.level12_nbr, n12.level12_nm,
    ' ' as level13_nbr, ' ' as level13_nm,
    ' ' as level14_nbr, ' ' as level14_nm,
    ' ' as level15_nbr, ' ' as level15_nm,
    ' ' as level16_nbr, ' ' as level16_nm,
    ' ' as level17_nbr, ' ' as level17_nm,
    ' ' as level18_nbr, ' ' as level18_nm,
    ' ' as level19_nbr, ' ' as level19_nm,
    ' ' as level20_nbr, ' ' as level20_nm,
    '12' as lst_level_nbr, lv12.lst_level_cust_nbr,
    lv12.kortex_upld_ts, lv12.kortex_dprct_ts, lv12.src_nm
    from lv1
    inner join name as n1 on lv1.kunnr = n1.kunnr
    inner join lv1 as lv2 on lv2.hkunnr = lv1.kunnr
    inner join name as n2 on lv2.kunnr = n2.kunnr
    inner join lv1 as lv3 on lv3.hkunnr = lv2.kunnr
    inner join name as n3 on lv3.kunnr = n3.kunnr
    inner join lv1 as lv4 on lv4.hkunnr = lv3.kunnr
    inner join name as n4 on lv4.kunnr = n4.kunnr
    inner join lv1 as lv5 on lv5.hkunnr = lv4.kunnr
    inner join name as n5 on lv5.kunnr = n5.kunnr
    inner join lv1 as lv6 on lv6.hkunnr = lv5.kunnr
    inner join name as n6 on lv6.kunnr = n6.kunnr
    inner join lv1 as lv7 on lv7.hkunnr = lv6.kunnr
    inner join name as n7 on lv7.kunnr = n7.kunnr
    inner join lv1 as lv8 on lv8.hkunnr = lv7.kunnr
    inner join name as n8 on lv8.kunnr = n8.kunnr
    inner join lv1 as lv9 on lv9.hkunnr = lv8.kunnr
    inner join name as n9 on lv9.kunnr = n9.kunnr
    inner join lv1 as lv10 on lv10.hkunnr = lv9.kunnr
    inner join name as n10 on lv10.kunnr = n10.kunnr
    inner join lv1 as lv11 on lv11.hkunnr = lv10.kunnr
    inner join name as n11 on lv11.kunnr = n11.kunnr
    inner join lv1 as lv12 on lv12.hkunnr = lv11.kunnr
    inner join name as n12 on lv12.kunnr = n12.kunnr
    where lv1.hkunnr = ' '



    union all
    --level 13

    select current_date as hier_dt, lv1.hier_id, lv1.level1_nbr, n1.level1_nm,
    lv2.level2_nbr, n2.level2_nm,
    lv3.level3_nbr, n3.level3_nm,
    lv4.level4_nbr, n4.level4_nm,
    lv5.level5_nbr, n5.level5_nm,
    lv6.level6_nbr, n6.level6_nm,
    lv7.level7_nbr, n7.level7_nm,
    lv8.level8_nbr, n8.level8_nm,
    lv9.level9_nbr, n9.level9_nm,
    lv10.level10_nbr, n10.level10_nm,
    lv11.level11_nbr, n11.level11_nm,
    lv12.level12_nbr, n12.level12_nm,
    lv13.level13_nbr, n13.level13_nm,
    ' ' as level14_nbr, ' ' as level14_nm,
    ' ' as level15_nbr, ' ' as level15_nm,
    ' ' as level16_nbr, ' ' as level16_nm,
    ' ' as level17_nbr, ' ' as level17_nm,
    ' ' as level18_nbr, ' ' as level18_nm,
    ' ' as level19_nbr, ' ' as level19_nm,
    ' ' as level20_nbr, ' ' as level20_nm,
    '13' as lst_level_nbr, lv13.lst_level_cust_nbr,
    lv13.kortex_upld_ts, lv13.kortex_dprct_ts, lv13.src_nm
    from lv1
    inner join name as n1 on lv1.kunnr = n1.kunnr
    inner join lv1 as lv2 on lv2.hkunnr = lv1.kunnr
    inner join name as n2 on lv2.kunnr = n2.kunnr
    inner join lv1 as lv3 on lv3.hkunnr = lv2.kunnr
    inner join name as n3 on lv3.kunnr = n3.kunnr
    inner join lv1 as lv4 on lv4.hkunnr = lv3.kunnr
    inner join name as n4 on lv4.kunnr = n4.kunnr
    inner join lv1 as lv5 on lv5.hkunnr = lv4.kunnr
    inner join name as n5 on lv5.kunnr = n5.kunnr
    inner join lv1 as lv6 on lv6.hkunnr = lv5.kunnr
    inner join name as n6 on lv6.kunnr = n6.kunnr
    inner join lv1 as lv7 on lv7.hkunnr = lv6.kunnr
    inner join name as n7 on lv7.kunnr = n7.kunnr
    inner join lv1 as lv8 on lv8.hkunnr = lv7.kunnr
    inner join name as n8 on lv8.kunnr = n8.kunnr
    inner join lv1 as lv9 on lv9.hkunnr = lv8.kunnr
    inner join name as n9 on lv9.kunnr = n9.kunnr
    inner join lv1 as lv10 on lv10.hkunnr = lv9.kunnr
    inner join name as n10 on lv10.kunnr = n10.kunnr
    inner join lv1 as lv11 on lv11.hkunnr = lv10.kunnr
    inner join name as n11 on lv11.kunnr = n11.kunnr
    inner join lv1 as lv12 on lv12.hkunnr = lv11.kunnr
    inner join name as n12 on lv12.kunnr = n12.kunnr
    inner join lv1 as lv13 on lv13.hkunnr = lv12.kunnr
    inner join name as n13 on lv13.kunnr = n13.kunnr
    where lv1.hkunnr = ' '

    union all
    --level 14

    select current_date as hier_dt, lv1.hier_id, lv1.level1_nbr, n1.level1_nm,
    lv2.level2_nbr, n2.level2_nm,
    lv3.level3_nbr, n3.level3_nm,
    lv4.level4_nbr, n4.level4_nm,
    lv5.level5_nbr, n5.level5_nm,
    lv6.level6_nbr, n6.level6_nm,
    lv7.level7_nbr, n7.level7_nm,
    lv8.level8_nbr, n8.level8_nm,
    lv9.level9_nbr, n9.level9_nm,
    lv10.level10_nbr, n10.level10_nm,
    lv11.level11_nbr, n11.level11_nm,
    lv12.level12_nbr, n12.level12_nm,
    lv13.level13_nbr, n13.level13_nm,
    lv14.level14_nbr, n14.level14_nm,
    ' ' as level15_nbr, ' ' as level15_nm,
    ' ' as level16_nbr, ' ' as level16_nm,
    ' ' as level17_nbr, ' ' as level17_nm,
    ' ' as level18_nbr, ' ' as level18_nm,
    ' ' as level19_nbr, ' ' as level19_nm,
    ' ' as level20_nbr, ' ' as level20_nm,
    '14' as lst_level_nbr, lv14.lst_level_cust_nbr,
    lv14.kortex_upld_ts, lv14.kortex_dprct_ts, lv14.src_nm
    from lv1
    inner join name as n1 on lv1.kunnr = n1.kunnr
    inner join lv1 as lv2 on lv2.hkunnr = lv1.kunnr
    inner join name as n2 on lv2.kunnr = n2.kunnr
    inner join lv1 as lv3 on lv3.hkunnr = lv2.kunnr
    inner join name as n3 on lv3.kunnr = n3.kunnr
    inner join lv1 as lv4 on lv4.hkunnr = lv3.kunnr
    inner join name as n4 on lv4.kunnr = n4.kunnr
    inner join lv1 as lv5 on lv5.hkunnr = lv4.kunnr
    inner join name as n5 on lv5.kunnr = n5.kunnr
    inner join lv1 as lv6 on lv6.hkunnr = lv5.kunnr
    inner join name as n6 on lv6.kunnr = n6.kunnr
    inner join lv1 as lv7 on lv7.hkunnr = lv6.kunnr
    inner join name as n7 on lv7.kunnr = n7.kunnr
    inner join lv1 as lv8 on lv8.hkunnr = lv7.kunnr
    inner join name as n8 on lv8.kunnr = n8.kunnr
    inner join lv1 as lv9 on lv9.hkunnr = lv8.kunnr
    inner join name as n9 on lv9.kunnr = n9.kunnr
    inner join lv1 as lv10 on lv10.hkunnr = lv9.kunnr
    inner join name as n10 on lv10.kunnr = n10.kunnr
    inner join lv1 as lv11 on lv11.hkunnr = lv10.kunnr
    inner join name as n11 on lv11.kunnr = n11.kunnr
    inner join lv1 as lv12 on lv12.hkunnr = lv11.kunnr
    inner join name as n12 on lv12.kunnr = n12.kunnr
    inner join lv1 as lv13 on lv13.hkunnr = lv12.kunnr
    inner join name as n13 on lv13.kunnr = n13.kunnr
    inner join lv1 as lv14 on lv14.hkunnr = lv13.kunnr
    inner join name as n14 on lv14.kunnr = n14.kunnr
    where lv1.hkunnr = ' '

    union all
    --level 15

    select current_date as hier_dt, lv1.hier_id, lv1.level1_nbr, n1.level1_nm,
    lv2.level2_nbr, n2.level2_nm,
    lv3.level3_nbr, n3.level3_nm,
    lv4.level4_nbr, n4.level4_nm,
    lv5.level5_nbr, n5.level5_nm,
    lv6.level6_nbr, n6.level6_nm,
    lv7.level7_nbr, n7.level7_nm,
    lv8.level8_nbr, n8.level8_nm,
    lv9.level9_nbr, n9.level9_nm,
    lv10.level10_nbr, n10.level10_nm,
    lv11.level11_nbr, n11.level11_nm,
    lv12.level12_nbr, n12.level12_nm,
    lv13.level13_nbr, n13.level13_nm,
    lv14.level14_nbr, n14.level14_nm,
    lv15.level15_nbr, n15.level15_nm,
    ' ' as level16_nbr, ' ' as level16_nm,
    ' ' as level17_nbr, ' ' as level17_nm,
    ' ' as level18_nbr, ' ' as level18_nm,
    ' ' as level19_nbr, ' ' as level19_nm,
    ' ' as level20_nbr, ' ' as level20_nm,
    '15' as lst_level_nbr, lv15.lst_level_cust_nbr,
    lv15.kortex_upld_ts, lv15.kortex_dprct_ts, lv15.src_nm
    from lv1
    inner join name as n1 on lv1.kunnr = n1.kunnr
    inner join lv1 as lv2 on lv2.hkunnr = lv1.kunnr
    inner join name as n2 on lv2.kunnr = n2.kunnr
    inner join lv1 as lv3 on lv3.hkunnr = lv2.kunnr
    inner join name as n3 on lv3.kunnr = n3.kunnr
    inner join lv1 as lv4 on lv4.hkunnr = lv3.kunnr
    inner join name as n4 on lv4.kunnr = n4.kunnr
    inner join lv1 as lv5 on lv5.hkunnr = lv4.kunnr
    inner join name as n5 on lv5.kunnr = n5.kunnr
    inner join lv1 as lv6 on lv6.hkunnr = lv5.kunnr
    inner join name as n6 on lv6.kunnr = n6.kunnr
    inner join lv1 as lv7 on lv7.hkunnr = lv6.kunnr
    inner join name as n7 on lv7.kunnr = n7.kunnr
    inner join lv1 as lv8 on lv8.hkunnr = lv7.kunnr
    inner join name as n8 on lv8.kunnr = n8.kunnr
    inner join lv1 as lv9 on lv9.hkunnr = lv8.kunnr
    inner join name as n9 on lv9.kunnr = n9.kunnr
    inner join lv1 as lv10 on lv10.hkunnr = lv9.kunnr
    inner join name as n10 on lv10.kunnr = n10.kunnr
    inner join lv1 as lv11 on lv11.hkunnr = lv10.kunnr
    inner join name as n11 on lv11.kunnr = n11.kunnr
    inner join lv1 as lv12 on lv12.hkunnr = lv11.kunnr
    inner join name as n12 on lv12.kunnr = n12.kunnr
    inner join lv1 as lv13 on lv13.hkunnr = lv12.kunnr
    inner join name as n13 on lv13.kunnr = n13.kunnr
    inner join lv1 as lv14 on lv14.hkunnr = lv13.kunnr
    inner join name as n14 on lv14.kunnr = n14.kunnr
    inner join lv1 as lv15 on lv15.hkunnr = lv14.kunnr
    inner join name as n15 on lv15.kunnr = n15.kunnr
    where lv1.hkunnr = ' '


    union all
    --level 16

    select current_date as hier_dt, lv1.hier_id, lv1.level1_nbr, n1.level1_nm,
    lv2.level2_nbr, n2.level2_nm,
    lv3.level3_nbr, n3.level3_nm,
    lv4.level4_nbr, n4.level4_nm,
    lv5.level5_nbr, n5.level5_nm,
    lv6.level6_nbr, n6.level6_nm,
    lv7.level7_nbr, n7.level7_nm,
    lv8.level8_nbr, n8.level8_nm,
    lv9.level9_nbr, n9.level9_nm,
    lv10.level10_nbr, n10.level10_nm,
    lv11.level11_nbr, n11.level11_nm,
    lv12.level12_nbr, n12.level12_nm,
    lv13.level13_nbr, n13.level13_nm,
    lv14.level14_nbr, n14.level14_nm,
    lv15.level15_nbr, n15.level15_nm,
    lv16.level16_nbr, n16.level16_nm,
    ' ' as level17_nbr, ' ' as level17_nm,
    ' ' as level18_nbr, ' ' as level18_nm,
    ' ' as level19_nbr, ' ' as level19_nm,
    ' ' as level20_nbr, ' ' as level20_nm,
    '16' as lst_level_nbr, lv16.lst_level_cust_nbr,
    lv16.kortex_upld_ts, lv16.kortex_dprct_ts, lv16.src_nm
    from lv1
    inner join name as n1 on lv1.kunnr = n1.kunnr
    inner join lv1 as lv2 on lv2.hkunnr = lv1.kunnr
    inner join name as n2 on lv2.kunnr = n2.kunnr
    inner join lv1 as lv3 on lv3.hkunnr = lv2.kunnr
    inner join name as n3 on lv3.kunnr = n3.kunnr
    inner join lv1 as lv4 on lv4.hkunnr = lv3.kunnr
    inner join name as n4 on lv4.kunnr = n4.kunnr
    inner join lv1 as lv5 on lv5.hkunnr = lv4.kunnr
    inner join name as n5 on lv5.kunnr = n5.kunnr
    inner join lv1 as lv6 on lv6.hkunnr = lv5.kunnr
    inner join name as n6 on lv6.kunnr = n6.kunnr
    inner join lv1 as lv7 on lv7.hkunnr = lv6.kunnr
    inner join name as n7 on lv7.kunnr = n7.kunnr
    inner join lv1 as lv8 on lv8.hkunnr = lv7.kunnr
    inner join name as n8 on lv8.kunnr = n8.kunnr
    inner join lv1 as lv9 on lv9.hkunnr = lv8.kunnr
    inner join name as n9 on lv9.kunnr = n9.kunnr
    inner join lv1 as lv10 on lv10.hkunnr = lv9.kunnr
    inner join name as n10 on lv10.kunnr = n10.kunnr
    inner join lv1 as lv11 on lv11.hkunnr = lv10.kunnr
    inner join name as n11 on lv11.kunnr = n11.kunnr
    inner join lv1 as lv12 on lv12.hkunnr = lv11.kunnr
    inner join name as n12 on lv12.kunnr = n12.kunnr
    inner join lv1 as lv13 on lv13.hkunnr = lv12.kunnr
    inner join name as n13 on lv13.kunnr = n13.kunnr
    inner join lv1 as lv14 on lv14.hkunnr = lv13.kunnr
    inner join name as n14 on lv14.kunnr = n14.kunnr
    inner join lv1 as lv15 on lv15.hkunnr = lv14.kunnr
    inner join name as n15 on lv15.kunnr = n15.kunnr
    inner join lv1 as lv16 on lv16.hkunnr = lv15.kunnr
    inner join name as n16 on lv16.kunnr = n16.kunnr
    where lv1.hkunnr = ' '

    union all
    --level 17

    select current_date as hier_dt, lv1.hier_id, lv1.level1_nbr, n1.level1_nm,
    lv2.level2_nbr, n2.level2_nm,
    lv3.level3_nbr, n3.level3_nm,
    lv4.level4_nbr, n4.level4_nm,
    lv5.level5_nbr, n5.level5_nm,
    lv6.level6_nbr, n6.level6_nm,
    lv7.level7_nbr, n7.level7_nm,
    lv8.level8_nbr, n8.level8_nm,
    lv9.level9_nbr, n9.level9_nm,
    lv10.level10_nbr, n10.level10_nm,
    lv11.level11_nbr, n11.level11_nm,
    lv12.level12_nbr, n12.level12_nm,
    lv13.level13_nbr, n13.level13_nm,
    lv14.level14_nbr, n14.level14_nm,
    lv15.level15_nbr, n15.level15_nm,
    lv16.level16_nbr, n16.level16_nm,
    lv17.level17_nbr, n17.level17_nm,
    ' ' as level18_nbr, ' ' as level18_nm,
    ' ' as level19_nbr, ' ' as level19_nm,
    ' ' as level20_nbr, ' ' as level20_nm,
    '17' as lst_level_nbr, lv17.lst_level_cust_nbr,
    lv17.kortex_upld_ts, lv17.kortex_dprct_ts, lv17.src_nm
    from lv1
    inner join name as n1 on lv1.kunnr = n1.kunnr
    inner join lv1 as lv2 on lv2.hkunnr = lv1.kunnr
    inner join name as n2 on lv2.kunnr = n2.kunnr
    inner join lv1 as lv3 on lv3.hkunnr = lv2.kunnr
    inner join name as n3 on lv3.kunnr = n3.kunnr
    inner join lv1 as lv4 on lv4.hkunnr = lv3.kunnr
    inner join name as n4 on lv4.kunnr = n4.kunnr
    inner join lv1 as lv5 on lv5.hkunnr = lv4.kunnr
    inner join name as n5 on lv5.kunnr = n5.kunnr
    inner join lv1 as lv6 on lv6.hkunnr = lv5.kunnr
    inner join name as n6 on lv6.kunnr = n6.kunnr
    inner join lv1 as lv7 on lv7.hkunnr = lv6.kunnr
    inner join name as n7 on lv7.kunnr = n7.kunnr
    inner join lv1 as lv8 on lv8.hkunnr = lv7.kunnr
    inner join name as n8 on lv8.kunnr = n8.kunnr
    inner join lv1 as lv9 on lv9.hkunnr = lv8.kunnr
    inner join name as n9 on lv9.kunnr = n9.kunnr
    inner join lv1 as lv10 on lv10.hkunnr = lv9.kunnr
    inner join name as n10 on lv10.kunnr = n10.kunnr
    inner join lv1 as lv11 on lv11.hkunnr = lv10.kunnr
    inner join name as n11 on lv11.kunnr = n11.kunnr
    inner join lv1 as lv12 on lv12.hkunnr = lv11.kunnr
    inner join name as n12 on lv12.kunnr = n12.kunnr
    inner join lv1 as lv13 on lv13.hkunnr = lv12.kunnr
    inner join name as n13 on lv13.kunnr = n13.kunnr
    inner join lv1 as lv14 on lv14.hkunnr = lv13.kunnr
    inner join name as n14 on lv14.kunnr = n14.kunnr
    inner join lv1 as lv15 on lv15.hkunnr = lv14.kunnr
    inner join name as n15 on lv15.kunnr = n15.kunnr
    inner join lv1 as lv16 on lv16.hkunnr = lv15.kunnr
    inner join name as n16 on lv16.kunnr = n16.kunnr
    inner join lv1 as lv17 on lv17.hkunnr = lv16.kunnr
    inner join name as n17 on lv17.kunnr = n17.kunnr
    where lv1.hkunnr = ' '


    union all
    --level 18

    select current_date as hier_dt, lv1.hier_id, lv1.level1_nbr, n1.level1_nm,
    lv2.level2_nbr, n2.level2_nm,
    lv3.level3_nbr, n3.level3_nm,
    lv4.level4_nbr, n4.level4_nm,
    lv5.level5_nbr, n5.level5_nm,
    lv6.level6_nbr, n6.level6_nm,
    lv7.level7_nbr, n7.level7_nm,
    lv8.level8_nbr, n8.level8_nm,
    lv9.level9_nbr, n9.level9_nm,
    lv10.level10_nbr, n10.level10_nm,
    lv11.level11_nbr, n11.level11_nm,
    lv12.level12_nbr, n12.level12_nm,
    lv13.level13_nbr, n13.level13_nm,
    lv14.level14_nbr, n14.level14_nm,
    lv15.level15_nbr, n15.level15_nm,
    lv16.level16_nbr, n16.level16_nm,
    lv17.level17_nbr, n17.level17_nm,
    lv18.level18_nbr, n18.level18_nm,
    ' ' as level19_nbr, ' ' as level19_nm,
    ' ' as level20_nbr, ' ' as level20_nm,
    '18' as lst_level_nbr, lv18.lst_level_cust_nbr,
    lv18.kortex_upld_ts, lv18.kortex_dprct_ts, lv18.src_nm
    from lv1
    inner join name as n1 on lv1.kunnr = n1.kunnr
    inner join lv1 as lv2 on lv2.hkunnr = lv1.kunnr
    inner join name as n2 on lv2.kunnr = n2.kunnr
    inner join lv1 as lv3 on lv3.hkunnr = lv2.kunnr
    inner join name as n3 on lv3.kunnr = n3.kunnr
    inner join lv1 as lv4 on lv4.hkunnr = lv3.kunnr
    inner join name as n4 on lv4.kunnr = n4.kunnr
    inner join lv1 as lv5 on lv5.hkunnr = lv4.kunnr
    inner join name as n5 on lv5.kunnr = n5.kunnr
    inner join lv1 as lv6 on lv6.hkunnr = lv5.kunnr
    inner join name as n6 on lv6.kunnr = n6.kunnr
    inner join lv1 as lv7 on lv7.hkunnr = lv6.kunnr
    inner join name as n7 on lv7.kunnr = n7.kunnr
    inner join lv1 as lv8 on lv8.hkunnr = lv7.kunnr
    inner join name as n8 on lv8.kunnr = n8.kunnr
    inner join lv1 as lv9 on lv9.hkunnr = lv8.kunnr
    inner join name as n9 on lv9.kunnr = n9.kunnr
    inner join lv1 as lv10 on lv10.hkunnr = lv9.kunnr
    inner join name as n10 on lv10.kunnr = n10.kunnr
    inner join lv1 as lv11 on lv11.hkunnr = lv10.kunnr
    inner join name as n11 on lv11.kunnr = n11.kunnr
    inner join lv1 as lv12 on lv12.hkunnr = lv11.kunnr
    inner join name as n12 on lv12.kunnr = n12.kunnr
    inner join lv1 as lv13 on lv13.hkunnr = lv12.kunnr
    inner join name as n13 on lv13.kunnr = n13.kunnr
    inner join lv1 as lv14 on lv14.hkunnr = lv13.kunnr
    inner join name as n14 on lv14.kunnr = n14.kunnr
    inner join lv1 as lv15 on lv15.hkunnr = lv14.kunnr
    inner join name as n15 on lv15.kunnr = n15.kunnr
    inner join lv1 as lv16 on lv16.hkunnr = lv15.kunnr
    inner join name as n16 on lv16.kunnr = n16.kunnr
    inner join lv1 as lv17 on lv17.hkunnr = lv16.kunnr
    inner join name as n17 on lv17.kunnr = n17.kunnr
    inner join lv1 as lv18 on lv18.hkunnr = lv17.kunnr
    inner join name as n18 on lv18.kunnr = n18.kunnr
    where lv1.hkunnr = ' '

    union all
    --level 19

    select current_date as hier_dt, lv1.hier_id, lv1.level1_nbr, n1.level1_nm,
    lv2.level2_nbr, n2.level2_nm,
    lv3.level3_nbr, n3.level3_nm,
    lv4.level4_nbr, n4.level4_nm,
    lv5.level5_nbr, n5.level5_nm,
    lv6.level6_nbr, n6.level6_nm,
    lv7.level7_nbr, n7.level7_nm,
    lv8.level8_nbr, n8.level8_nm,
    lv9.level9_nbr, n9.level9_nm,
    lv10.level10_nbr, n10.level10_nm,
    lv11.level11_nbr, n11.level11_nm,
    lv12.level12_nbr, n12.level12_nm,
    lv13.level13_nbr, n13.level13_nm,
    lv14.level14_nbr, n14.level14_nm,
    lv15.level15_nbr, n15.level15_nm,
    lv16.level16_nbr, n16.level16_nm,
    lv17.level17_nbr, n17.level17_nm,
    lv18.level18_nbr, n18.level18_nm,
    lv19.level19_nbr, n19.level19_nm,
    ' ' as level20_nbr, ' ' as level20_nm,
    '19' as lst_level_nbr, lv19.lst_level_cust_nbr,
    lv19.kortex_upld_ts, lv19.kortex_dprct_ts, lv19.src_nm
    from lv1
    inner join name as n1 on lv1.kunnr = n1.kunnr
    inner join lv1 as lv2 on lv2.hkunnr = lv1.kunnr
    inner join name as n2 on lv2.kunnr = n2.kunnr
    inner join lv1 as lv3 on lv3.hkunnr = lv2.kunnr
    inner join name as n3 on lv3.kunnr = n3.kunnr
    inner join lv1 as lv4 on lv4.hkunnr = lv3.kunnr
    inner join name as n4 on lv4.kunnr = n4.kunnr
    inner join lv1 as lv5 on lv5.hkunnr = lv4.kunnr
    inner join name as n5 on lv5.kunnr = n5.kunnr
    inner join lv1 as lv6 on lv6.hkunnr = lv5.kunnr
    inner join name as n6 on lv6.kunnr = n6.kunnr
    inner join lv1 as lv7 on lv7.hkunnr = lv6.kunnr
    inner join name as n7 on lv7.kunnr = n7.kunnr
    inner join lv1 as lv8 on lv8.hkunnr = lv7.kunnr
    inner join name as n8 on lv8.kunnr = n8.kunnr
    inner join lv1 as lv9 on lv9.hkunnr = lv8.kunnr
    inner join name as n9 on lv9.kunnr = n9.kunnr
    inner join lv1 as lv10 on lv10.hkunnr = lv9.kunnr
    inner join name as n10 on lv10.kunnr = n10.kunnr
    inner join lv1 as lv11 on lv11.hkunnr = lv10.kunnr
    inner join name as n11 on lv11.kunnr = n11.kunnr
    inner join lv1 as lv12 on lv12.hkunnr = lv11.kunnr
    inner join name as n12 on lv12.kunnr = n12.kunnr
    inner join lv1 as lv13 on lv13.hkunnr = lv12.kunnr
    inner join name as n13 on lv13.kunnr = n13.kunnr
    inner join lv1 as lv14 on lv14.hkunnr = lv13.kunnr
    inner join name as n14 on lv14.kunnr = n14.kunnr
    inner join lv1 as lv15 on lv15.hkunnr = lv14.kunnr
    inner join name as n15 on lv15.kunnr = n15.kunnr
    inner join lv1 as lv16 on lv16.hkunnr = lv15.kunnr
    inner join name as n16 on lv16.kunnr = n16.kunnr
    inner join lv1 as lv17 on lv17.hkunnr = lv16.kunnr
    inner join name as n17 on lv17.kunnr = n17.kunnr
    inner join lv1 as lv18 on lv18.hkunnr = lv17.kunnr
    inner join name as n18 on lv18.kunnr = n18.kunnr
    inner join lv1 as lv19 on lv19.hkunnr = lv18.kunnr
    inner join name as n19 on lv19.kunnr = n19.kunnr
    where lv1.hkunnr = ' '


    union all
    --level 20

    select current_date as hier_dt, lv1.hier_id, lv1.level1_nbr, n1.level1_nm,
    lv2.level2_nbr, n2.level2_nm,
    lv3.level3_nbr, n3.level3_nm,
    lv4.level4_nbr, n4.level4_nm,
    lv5.level5_nbr, n5.level5_nm,
    lv6.level6_nbr, n6.level6_nm,
    lv7.level7_nbr, n7.level7_nm,
    lv8.level8_nbr, n8.level8_nm,
    lv9.level9_nbr, n9.level9_nm,
    lv10.level10_nbr, n10.level10_nm,
    lv11.level11_nbr, n11.level11_nm,
    lv12.level12_nbr, n12.level12_nm,
    lv13.level13_nbr, n13.level13_nm,
    lv14.level14_nbr, n14.level14_nm,
    lv15.level15_nbr, n15.level15_nm,
    lv16.level16_nbr, n16.level16_nm,
    lv17.level17_nbr, n17.level17_nm,
    lv18.level18_nbr, n18.level18_nm,
    lv19.level19_nbr, n19.level19_nm,
    lv20.level20_nbr, n20.level20_nm,
    '20' as lst_level_nbr, lv20.lst_level_cust_nbr,
    lv20.kortex_upld_ts, lv20.kortex_dprct_ts, lv20.src_nm
    from lv1
    inner join name as n1 on lv1.kunnr = n1.kunnr
    inner join lv1 as lv2 on lv2.hkunnr = lv1.kunnr
    inner join name as n2 on lv2.kunnr = n2.kunnr
    inner join lv1 as lv3 on lv3.hkunnr = lv2.kunnr
    inner join name as n3 on lv3.kunnr = n3.kunnr
    inner join lv1 as lv4 on lv4.hkunnr = lv3.kunnr
    inner join name as n4 on lv4.kunnr = n4.kunnr
    inner join lv1 as lv5 on lv5.hkunnr = lv4.kunnr
    inner join name as n5 on lv5.kunnr = n5.kunnr
    inner join lv1 as lv6 on lv6.hkunnr = lv5.kunnr
    inner join name as n6 on lv6.kunnr = n6.kunnr
    inner join lv1 as lv7 on lv7.hkunnr = lv6.kunnr
    inner join name as n7 on lv7.kunnr = n7.kunnr
    inner join lv1 as lv8 on lv8.hkunnr = lv7.kunnr
    inner join name as n8 on lv8.kunnr = n8.kunnr
    inner join lv1 as lv9 on lv9.hkunnr = lv8.kunnr
    inner join name as n9 on lv9.kunnr = n9.kunnr
    inner join lv1 as lv10 on lv10.hkunnr = lv9.kunnr
    inner join name as n10 on lv10.kunnr = n10.kunnr
    inner join lv1 as lv11 on lv11.hkunnr = lv10.kunnr
    inner join name as n11 on lv11.kunnr = n11.kunnr
    inner join lv1 as lv12 on lv12.hkunnr = lv11.kunnr
    inner join name as n12 on lv12.kunnr = n12.kunnr
    inner join lv1 as lv13 on lv13.hkunnr = lv12.kunnr
    inner join name as n13 on lv13.kunnr = n13.kunnr
    inner join lv1 as lv14 on lv14.hkunnr = lv13.kunnr
    inner join name as n14 on lv14.kunnr = n14.kunnr
    inner join lv1 as lv15 on lv15.hkunnr = lv14.kunnr
    inner join name as n15 on lv15.kunnr = n15.kunnr
    inner join lv1 as lv16 on lv16.hkunnr = lv15.kunnr
    inner join name as n16 on lv16.kunnr = n16.kunnr
    inner join lv1 as lv17 on lv17.hkunnr = lv16.kunnr
    inner join name as n17 on lv17.kunnr = n17.kunnr
    inner join lv1 as lv18 on lv18.hkunnr = lv17.kunnr
    inner join name as n18 on lv18.kunnr = n18.kunnr
    inner join lv1 as lv19 on lv19.hkunnr = lv18.kunnr
    inner join name as n19 on lv19.kunnr = n19.kunnr
    inner join lv1 as lv20 on lv20.hkunnr = lv19.kunnr
    inner join name as n20 on lv20.kunnr = n20.kunnr
    where lv1.hkunnr = ' '
)

select * from cust_hier_unrvl_i_cur_d