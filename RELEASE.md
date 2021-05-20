# Hdclient Release

This document contains the steps to be followed to release
the project hdclient.

## Release Process

To release hdclient:

* Create a new branch: `git checkout -b feature/TICKET-123-release-x.y.z`
* Update the `version` field on the [package.json] file on to the desired tag.
* Commit your changes: `git commit -m "TICKET-123 release version $tag"`
* Create a PR and merge the [package.json] change.
* Checkout the change in the development branch:
  1. `git checkout development/x.y`
  1. `git pull`
* Tag the repository using the same tag:
  1. `git tag --annotate $tag`
  1. `git push origin $tag`

Note: You can also perform the last two steps through
the [GitHub Release] UI.

[package.json]: ./package.json
[GitHub Release]: https://github.com/scality/hdclient/releases/new
