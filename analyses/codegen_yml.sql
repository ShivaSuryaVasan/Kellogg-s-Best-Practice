{{
   codegen.generate_source(
       schema_name="PREP",
       database_name="ASGMT2",
       table_names=["kna_ecc_cust_mstr_kna1","kna_ecc_cust_mstr_knvh","cust_spin"],
       generate_columns=True,
       include_descriptions=True,
       name="stg_",
   )
}}