# Security

Report vulnerabilities privately to the repository owner. Do not include live
credentials, private payloads, or personal destinations in an issue.

This adapter is intentionally not a generic webhook client. It accepts only
implemented action kinds and operator-registered opaque destination references.
The first release supports bounded GitHub issue mutations against the fixed
GitHub API origin. Provider credentials stay in independent local files.

A provider timeout, transport failure, malformed success response, or server
error after the mutation boundary is treated as an ambiguous result. The
occurrence is durably marked `unknown` and is not sent again automatically.
