# Security Policy

## About InfiniteFS Security

InfiniteFS is an experimental Rust project exploring filesystem-like access to local and cloud-backed storage.

Security is an important design goal. However, the project is under development and has not been independently audited. Do not assume it is production-ready or suitable for sensitive data.

## Reporting a Vulnerability

If you discover a potential security vulnerability, please report it privately rather than publishing exploit details in a public issue.

You can contact the repository maintainer through GitHub to arrange private communication.

Please include, where possible:

- A description of the vulnerability.
- The affected component or file.
- Steps to reproduce the issue.
- The potential impact.
- Any suggested mitigation.

Do not include real credentials, personal information, or sensitive user data in your report.

Please allow the maintainer reasonable time to investigate and address the issue before publicly disclosing technical details.

## Security Areas of Interest

The following areas are particularly important to InfiniteFS:

- Path traversal and filesystem boundary enforcement.
- Symbolic-link handling.
- File and directory access.
- Credential management and AWS permissions.
- Cloud storage access controls.
- Temporary files, caching, and data exposure.
- Dependency vulnerabilities.

## Security Guidelines for Contributors

- Never commit API keys, passwords, access tokens, private keys, or other secrets.
- Do not weaken path validation or filesystem isolation without appropriate justification and tests.
- Avoid including sensitive data in logs, test fixtures, or issue reports.
- Explain security-relevant changes in pull requests.
- Test filesystem changes carefully in an isolated development environment.

## Supported Versions

InfiniteFS is currently in early development. No production release or formal security-support schedule has been established.

## Scope and Disclaimer

Security reports are welcome, but the maintainer cannot guarantee a particular response time or that every reported issue will be fixed.

InfiniteFS is provided under the terms of the MIT Licence. Refer to the `LICENSE` file for the licence terms.
