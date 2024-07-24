# The ARGA operation log CLI

ARGA tracks changes to data by using CRDTs backed by operation log tables in PostgreSQL. This tool decomposes dataset exports into operations for every field, deduplicate them, and associate new changes with appropriate attribution.
