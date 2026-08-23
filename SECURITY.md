# Security

FlowDE is designed so that runtime data stays outside version control.

- Never commit `config.json`, `.env` files, API tokens, cookies, store credentials, runtime state, audit ledgers, product images, supplier links, or business reports.
- Resolve marketplace and ERP credentials from environment variables or a dedicated secret manager inside your engine adapter.
- Before making a fork public, inspect the complete Git history as well as the current working tree.

If you discover a security issue in FlowDE itself, report it privately to the repository owner rather than opening a public issue with sensitive details.
