{#
    Demo staging selector (GenBI end-to-end demo).

    The canonical models read staging through this macro instead of a hard-coded
    `ref('stg_csvsftp__*')`, so the SAME marts can be built from the seeded
    dealer export loaded through the legacy connector pack (the demo Informix
    path) by passing:

        dbt build --vars '{"demo_source": "informix"}'

    Default remains the CSV/SFTP path, so `make demo` and CI are unchanged.
    Both staging layers expose identical model shapes; dbt tests run per layer.
#}
{% macro demo_staging(entity) %}
    {{ ref('stg_' ~ var('demo_source', 'csvsftp') ~ '__' ~ entity) }}
{% endmacro %}
