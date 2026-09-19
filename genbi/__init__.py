"""GenBI extension (WrenAI) — shared connection config and MDL tooling.

Layout:
    genbi/connection.py  — the single READ_ONLY DuckDB URI source (spec §2.3)
    genbi/mdl_gen/       — dbt→MDL generator, MDL validator, dbt↔MDL coupling check
    genbi/mdl/           — the curated MDL (versioned, served to WrenAI)
    genbi/ai-service/    — wren-ai-service config templates (secrets-free)
"""
