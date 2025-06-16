{% macro c_to_f(column) -%}
  ({{ column }} * 9.0 / 5.0 + 32.0)
{%- endmacro %}
