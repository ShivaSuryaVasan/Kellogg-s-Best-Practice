/*
-- ===================================================================================================================================================================================================================
Authors    : Roshni Balaji 
Create Date: 18/07/2024
Description: Customer Master - Company Code
Name       : cust_mstr.int_cust_co_cd
Revisions  :
Version     Date        Author           Description
---------  ----------  ---------------  ------------------------------------------------------------------
1.0        18/07/2024   Roshni Balaji      Initial Version.
-- ===================================================================================================================================================================================================================
*/

{{
    config(
        materialized='ephemeral'
    )
}}

with kna_ecc_cust_mstr_kna1 as(
    select * from {{ source('stg_kna_ecc_cust_mstr', 'kna_ecc_cust_mstr_kna1') }}
),

cust_spin as(
    select * from {{ source('stg_cust_spin', 'cust_spin') }}
),

ref_co_spin as(
    select * from {{ source('stg_cust_spin', 'ref_co_spin') }}
),

kna_ecc_cust_mstr_knb1 as(
    select * from {{ source('stg_kna_ecc_cust_mstr', 'kna_ecc_cust_mstr_knb1') }}
),

final as(
    select
        k.cust_nbr,
        b.co_cd,
        b.tdlinx_nbr,
        b.co_cd_del_ind,
        md5(k.src_nm || k.cust_nbr) as hash_key,
        -- generating hash key by combining two column 
        k.kortex_dprct_ts,
        k.kortex_upld_ts,
        k.src_nm
    from
        kna_ecc_cust_mstr_kna1 as k
        join kna_ecc_cust_mstr_knb1 as b on
        k.cust_nbr = b.KUNNR
        --inner join with kna_ecc_cust_mstr_knb1

        left outer join cust_spin as custspin
        on k.cust_nbr = custspin.cust_nbr
        left outer join ref_co_spin as refspin
        on b.co_cd = refspin.co_cd
    where
        (custspin.spin_owner_nm <> 'CEREAL' or custspin.spin_owner_nm is null)
    and
        (refspin.spin_owner_nm <> 'CEREAL' or refspin.spin_owner_nm is null)


)

select * from final