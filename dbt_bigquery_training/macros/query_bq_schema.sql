{% macro query_bq_schema(schema=target.schema, role=target.role) %}

    {% set query %}
        select 1
    {% endset %}
    
    {{ log('printing config about the schema ' ~ schema ~ ' using the role ' ~ role, info=True) }}
    
    {% do run_query(query) %}

{% endmacro %}