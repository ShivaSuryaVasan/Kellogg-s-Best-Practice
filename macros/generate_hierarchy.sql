-- Macro to generate a hierarchical structure of customers
-- Arguments:
--   date_condition: Condition to filter data based on date
--   hityp_value: Specific hierarchy type to filter on
--   future_date: Boolean flag to determine if future date or current date should be used

{% macro generate_hierarchy(date_condition, hityp_value, future_date=false, cust_mstr_spin=false) %}
{% if future_date %}
    -- If future_date is true, use a far future date as the hierarchy date
    {% set hier_dt = "to_date('99991231','YYYYMMDD')" %}
{% else %}
    -- If future_date is false, use the current date as the hierarchy date
    {% set hier_dt = "current_date" %}
{% endif %}

with kna_ecc_cust_mstr_kna1 as (
    select * from {{ source('stg_kna_ecc_cust_mstr','kna_ecc_cust_mstr_kna1') }}
),

kna_ecc_cust_mstr_knvh as (
    select * from {{ source('stg_kna_ecc_cust_mstr','kna_ecc_cust_mstr_knvh')}}
),

cust_spin as (
    select * from {{ source('stg_cust_spin','cust_spin')}}
),

kna1_base as (
    select 
        distinct kunnr, name1 
    from
        kna_ecc_cust_mstr_kna1 as kna
    left outer join 
        cust_spin as custspin on kna.kunnr = custspin.cust_nbr
    where 
    {% if cust_mstr_spin %}
        custspin.spin_owner_nm = 'CEREAL'
    {% else %}
        custspin.spin_owner_nm <> 'CEREAL'
    {% endif %}
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
        {{ date_condition }} and hityp = {{ hityp_value }} 
    group by 
        kunnr, hkunnr, hityp, src_nm
),

name as (
    select kunnr, name1 
    from kna1_base
),

-- Building hierarchical structure up to 20 levels deep
cust_hier_unrvl as (
    {% set max_level = 20 %}
    {% for level in range(1, max_level + 1) %}
        select {{ hier_dt }} as hier_dt, 
            lv1.hityp as hier_id, 
            lv1.kunnr as level1_nbr, 
            n1.name1 as level1_nm,
            {% for lvl in range(2, max_level + 1) %}
                {% if lvl <= level %}
                    lv{{ lvl }}.kunnr as level{{ lvl }}_nbr, 
                    n{{ lvl }}.name1 as level{{ lvl }}_nm,
                {% else %}
                    ' ' as level{{ lvl }}_nbr, 
                    ' ' as level{{ lvl }}_nm,
                {% endif %}
            {% endfor %}
            '{{ level }}' as lst_level_nbr, 
            lv{{ level }}.kunnr as lst_level_cust_nbr,
            lv{{ level }}.kortex_upld_ts, 
            lv{{ level }}.kortex_dprct_ts, 
            lv{{ level }}.src_nm
        from lv1
        inner join name as n1 on lv1.kunnr = n1.kunnr
        {% for lvl in range(2, level + 1) %}
            inner join lv1 as lv{{ lvl }} on lv{{ lvl }}.hkunnr = lv{{ lvl-1 }}.kunnr
            inner join name as n{{ lvl }} on lv{{ lvl }}.kunnr = n{{ lvl }}.kunnr
        {% endfor %}
        where lv1.hkunnr = ' '
        {% if not loop.last %}
        union all
        {% endif %}
    {% endfor %}
)

select * from cust_hier_unrvl

{% endmacro %}