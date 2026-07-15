# Changelog

## Unreleased

- Make verified Supabase subtitle publication the readiness boundary. Website
  D1/KV catalog synchronization is now a non-blocking substate with bounded
  retries, sanitized diagnostics, and warnings that cannot downgrade ready jobs.
- Send `subtitle.ready` immediately after verified Supabase publication.
- Add a dry-run-first reconciliation command for historical
  `catalog_sync:*` false failures, including exact Database and Storage receipt
  verification before any local compare-and-swap repair.
