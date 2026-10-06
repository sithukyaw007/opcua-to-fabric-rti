# Security

This repository is a sample provided as-is. It is not an official Microsoft product, and there is no
support agreement or response-time commitment.

## Reporting a vulnerability

Please **don't** open a public issue for security problems. Report them privately through
**Security > Report a vulnerability** on this repository (GitHub private vulnerability reporting),
with steps to reproduce and the affected version or commit.

## Using this sample safely

* Never commit `.env`, connection strings, client secrets, certificates or private keys.
* Prefer Microsoft Entra ID over the Eventstream SAS key, and rotate keys you have shared.
* Use OPC UA `SignAndEncrypt` and pin the server certificate (see the README).
* Keep the bridge outbound-only. Don't open inbound access into the machine network.
