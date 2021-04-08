## Description
<!-- Add a description of your PR here.
A good description should include pointers to an issue or design document, etc.
-->
## Upgrade Notes
Does this PR prevent a zero down-time upgrade? (Assume upgrade order: Controller, Broker, Server, Minion)
* [ ] Yes (Please label as **<code>backward-incompat</code>**, and complete the section below on Release Notes)

Does this PR fix a zero-downtime upgrade introduced earlier?
* [ ] Yes (Please label this as **<code>backward-incompat</code>**, and complete the section below on Release Notes)

Does this PR otherwise need attention when creating release notes? Things to consider:
- New configuration options
- Deprecation of configurations
- Signature changes to public methods/interfaces
- New plugins added or old plugins removed
* [ ] Yes (Please label this PR as **<code>release-notes</code>** and complete the section on Release Notes)
## Release Notes
<!-- If you have tagged this as either backward-incompat or release-notes,
you MUST add text here that you would like to see appear in release notes of the
next release. -->

<!-- If you have a series of commits adding or enabling a feature, then
add this section only in final commit that marks the feature completed.
Refer to earlier release notes to see examples of text.
-->
## Documentation
<!-- If you have introduced a new feature or configuration, please add it to the documentation as well.
See https://docs.pinot.apache.org/developers/developers-and-contributors/update-document
-->
