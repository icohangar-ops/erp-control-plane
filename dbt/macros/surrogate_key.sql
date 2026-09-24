{# Deterministic surrogate key from raw SQL fragments (columns or literals).
   Call shape: sk("source_system", "order_no", "line_no") or sk("'ship_to'", "customer_no").
   All positional args land in Jinja's `varargs`; each is cast to varchar and
   joined with ':', then hashed. #}
{% macro sk() -%}
md5(concat_ws(':', {% for p in varargs %}{{ "," if not loop.first }} cast({{ p }} as varchar){% endfor %}))
{%- endmacro %}
