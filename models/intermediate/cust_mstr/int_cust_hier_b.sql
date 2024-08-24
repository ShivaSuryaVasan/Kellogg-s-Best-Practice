/*
-- klg_nga_kna.cust_mstr.sp_cust_hier_b

-- ===================================================================================================================================================================================================================
Authors    : Surya
Create Date: 07/16/2024
Description: Customer hierarchy (SAP)
Name       : cust_mstr.sp_cust_hier_b
Revisions  :
Version     Date        Author           Description
---------  ----------  ---------------  ------------------------------------------------------------------
1.0        07/16/2024    Surya           Initial Version.
-- ===================================================================================================================================================================================================================
*/

{{
    config(
        materialized='ephemeral'
    )
}}

with int_cust_hier_unrvl_i_cur_b as (
    select * from (
            {{ generate_hierarchy("datab <= current_date and datbi >= current_date", "'B'") }} 
        ) as knvh_current
),

int_cust_hier_unrvl_i_fut_b as (
    select * from (
            {{ generate_hierarchy("datab <= to_date('99991231','YYYYMMDD') and datbi >= to_date('99991231','YYYYMMDD')", "'B'", true) }}
        ) as knvh_future
),

final as (
    select 
        src_nm,
        case when t1.hier_dt = '9999-12-31' then 'FUTURE'
        else 'CURRENT'
        end as hier_tmfrm_id,
        MD5( 'kna_ecc' ||
             hier_id ||
             ( case when t1.hier_dt = '9999-12-31' then 'FUTURE'
        else 'CURRENT'
        end ) ||
        lst_level_cust_nbr ) as hash_key,
        -- generating column hash_key
        hier_dt,
        hier_id,
        level1_nbr as co_nbr,
        level2_nbr as ctry_nbr,
        level3_nbr as chnl_nbr,
        level4_nbr as level4_misc_nbr,
        level5_nbr as rgn_nbr,
        level6_nbr as area_nbr,
        level7_nbr as terr_nbr,
        level8_nbr as sold_to_nbr,
        level9_nbr as level9_nbr,
        level10_nbr,
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
        level3_nm as chnl_nm,
        level4_nm as level4_misc_nm,
        level5_nm as rgn_nm,
        level6_nm as area_nm,
        level7_nm as terr_nm,
        level8_nm as sold_to_nm,
        level9_nm as level9_nm,
        level10_nm,
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
        int_cust_hier_unrvl_i_cur_b as t1

    union all

    select
        src_nm,
        case when t2.hier_dt = '9999-12-31' then 'FUTURE'
        else 'CURRENT'
        end as hier_tmfrm_id,
        MD5( 'kna_ecc' ||
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
        level3_nbr as chnl_nbr,
        level4_nbr as level4_misc_nbr,
        level5_nbr as rgn_nbr,
        level6_nbr as area_nbr,
        level7_nbr as terr_nbr,
        level8_nbr as sold_to_nbr,
        level9_nbr as level9_nbr,
        level10_nbr,
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
        level3_nm as chnl_nm,
        level4_nm as level4_misc_nm,
        level5_nm as rgn_nm,
        level6_nm as area_nm,
        level7_nm as terr_nm,
        level8_nm as sold_to_nm,
        level9_nm as level9_nm,
        level10_nm,
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
        int_cust_hier_unrvl_i_fut_b as t2
)

select * from final