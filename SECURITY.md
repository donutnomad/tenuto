# Security Policy

## Reporting a vulnerability

Report security vulnerabilities through GitHub private vulnerability reporting:
open the repository's **Security** tab and choose **Report a vulnerability**
(<https://github.com/donutnomad/tenuto/security/advisories/new>).

Do not open a public issue, pull request or discussion for a suspected
vulnerability.

Include in the report:

- the affected component (Go SDK, worker, compiler, server, console) and commit or version;
- the backend in use (memory or MySQL) and relevant configuration;
- steps to reproduce, or a proof of concept;
- the impact you expect (data exposure, privilege escalation, denial of service, and so on).

The maintainers acknowledge reports through the advisory thread, keep the
reporter informed while a fix is prepared, and credit the reporter in the
published advisory unless the reporter asks to stay anonymous.

## Supported versions

Tenuto has no tagged release yet. Security fixes land on the `main` branch.
