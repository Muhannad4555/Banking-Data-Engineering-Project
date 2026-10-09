{# By default dbt names schemas <target_schema>_<custom>, e.g. CLEANED_BUSINESS_READY. Use the custom name as-is. #}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {{ custom_schema_name | trim if custom_schema_name else target.schema }}
{%- endmacro %}
