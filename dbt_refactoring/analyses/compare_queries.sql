{% set old_etl_relation=ref('warehouse_outbound') %} 

{% set dbt_relation=ref('fct_warehouse') %}  {{ 

audit_helper.compare_relations(
        a_relation=old_etl_relation,
        b_relation=dbt_relation,
        primary_key="order_id"
    ) }}