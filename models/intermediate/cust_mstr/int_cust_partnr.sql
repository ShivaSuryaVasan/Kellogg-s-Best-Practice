/*
===========================================================================================================================================
Authors    : Roshni Balaji
Create Date: 29/07/2024
Description: Customer Master : Customer_Partner
Name       : cust_mstr.sp_cust_partnr
Revisions         :
Input Parameter   :
Output Parameter  :
Version     Date        Author           Description
---------  ----------  ---------------  ------------------------------------------------------------------
1.0        29/07/2024   Roshni Balaji      Initial Version.

===================================================================================================================
*/

{{
    config(
        materialized='ephemeral'
    )
}}

with kna_ecc_cust_mstr_knvp as(
    select * from {{ source('stg_kna_ecc_cust_mstr', 'kna_ecc_cust_mstr_knvp') }}
),

kna_ecc_cust_mstr_tpart as(
    select * from {{ source('stg_kna_ecc_cust_mstr', 'kna_ecc_cust_mstr_tpart') }}
),

kna_ecc_lng_mstr_t002 as(
    select * from {{source('stg_kna_ecc_cust_mstr', 'kna_ecc_lng_mstr_t002')}}
),

cust_spin as(
    select * from {{source('stg_cust_spin', 'cust_spin')}}
),

final as (
    select
        prt.partnr_fctn_cd,
        t.partnr_fctn_desc,
        prt.cust_nbr,
        prt.sales_org_cd,
        prt.distbn_chnl_cd,
        prt.div_cd,
        prt.partnr_ctr_nbr,
        prt.partnr_cust_nbr,
        l.lng_cd,
        md5(prt.src_nm || prt.cust_nbr || prt.sales_org_cd || prt.distbn_chnl_cd || prt.div_cd || prt.partnr_ctr_nbr) as hash_key,
        -- generating column hash key
        prt.kortex_dprct_ts,
        prt.kortex_upld_ts,
        prt.src_nm
    from
        kna_ecc_cust_mstr_knvp as prt
    inner join kna_ecc_cust_mstr_tpart as t on prt.partnr_fctn_cd = t.PARVW and t.SPRAS = 'E'
    left outer join kna_ecc_lng_mstr_t002 as l on l.SPRAS = 'E'

    /*--------------------------KNA WKKC Cereal Split Filter---------------------------*/
    left outer join cust_spin cs on prt.cust_nbr = cs.cust_nbr
    left outer join cust_spin cs2 on prt.partnr_cust_nbr = cs2.cust_nbr
    where
            (cs.spin_owner_nm <> 'CEREAL' OR cs.spin_owner_nm is null)
            and
            (cs2.spin_owner_nm <> 'CEREAL' OR cs2.spin_owner_nm is null)
            /*--------------------------KNA WKKC Cereal Split Filter---------------------------*/


)

select * from final