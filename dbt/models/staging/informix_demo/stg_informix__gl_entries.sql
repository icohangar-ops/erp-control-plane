select
    journal_no,
    line_no,
    entry_date,
    branch_code,
    account,
    description,
    debit_amt,
    credit_amt,
    source_system,
    source_id,
    source_file,
    source_row_no,
    batch_id,
    loaded_at
from {{ source('informix_demo', 'gl_entries') }}
