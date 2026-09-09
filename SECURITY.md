# Security Policy

## Supported versions

Security fixes are applied to the most recent minor release. Older versions are not
patched; upgrade to the latest release before reporting.

## Reporting a vulnerability

**Do not open a public issue for a security problem.**

Report it privately through
[GitHub's private vulnerability reporting](https://github.com/AgentOxygen/GenTS/security/advisories/new),
which requires no setup beyond enabling it once in the repository's Security settings.

Please include:

- The version of GenTS affected and how it was installed.
- What an attacker can do with the issue.
- Steps to reproduce, ideally with a minimal example.

### What to expect

- An acknowledgement within 1 month.
- An assessment of whether the report is in scope, and if so a rough timeline.
- Credit in the release notes when the fix ships, unless you prefer otherwise.

## Scope

GenTS reads netCDF files produced by Earth System Models and writes netCDF files. The
most relevant concerns are therefore:

- Handling of malformed or malicious netCDF input causing crashes, resource exhaustion,
  or arbitrary file writes.
- Path handling that could write output outside the requested output directory.
- Unsafe deserialization of YAML model configuration files.

### Out of scope

- Vulnerabilities in dependencies (`netCDF4`, `numpy`, `cftime`, `PyYAML`). If GenTS's declared version range forces a vulnerable version, that *is* in scope; tell us and we will move the floor.
- Anything requiring the reporter to already have write access to the machine running
  GenTS or to the history files being read.