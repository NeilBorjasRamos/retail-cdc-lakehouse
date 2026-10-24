# Security

This repository contains fictional order data. The local Kafka service uses plaintext,
binds its external listener to loopback and is intended for disposable development.
The SQLite/warehouse directory is trusted local state, not a multi-tenant boundary.

Keep secrets in your deployment's secret manager. Do not place credentials in JSONL,
notebooks, Git issues, workflow output or committed environment files. .env.example
contains only a local address. The repository checker covers a few common patterns;
it is not a comprehensive secret scanner or vulnerability assessment.

For an actual source, apply authentication, TLS, least privilege, encryption, retention
and auditing before ingesting any sensitive data. A CDC logical delete retains Bronze,
history and old snapshots; it does not fulfill physical erasure requirements.

Do not disclose a suspected secret or security vulnerability in a public issue. Use
the maintainer's private GitHub contact or private vulnerability reporting if enabled.
No private reporting endpoint is assumed to have been configured by this repository.
