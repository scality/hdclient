# Hdclient Release

This document contains the steps to be followed to release
the project hdclient.

## Release Process

To release hdclient:

* Create a new branch: `git checkout -b feature/TICKET-123-release-x.y.z`
* Update the `version` field on the [package.json] file on to the desired tag.
* Commit your changes: `git commit -m "TICKET-123 release version $tag"`
* Create a PR and merge the [package.json] change.
* Trigger the [release] workflow.

[release]: https://github.com/scality/hdclient/actions/workflows/release.yaml
[package.json]: ./package.json
