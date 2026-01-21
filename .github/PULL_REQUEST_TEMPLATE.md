Instructions:
1. The PR has to be tagged with at least one of the following labels (*):
   1. `feature`
   2. `bugfix`
   3. `performance`
   4. `ui`
   5. `backward-incompat`
   6. `release-notes` (**)
2. Update the [dev documentation](/docs/dev) if you are adding new features or changing existing ones.
   This is important to keep the documentation in sync with the codebase.
3. Remove these instructions before publishing the PR.
 
(*) Other labels to consider:
- `testing`
- `dependencies`
- `docker`
- `kubernetes`
- `observability`
- `security`
- `code-style`
- `extension-point`
- `refactor`
- `cleanup`

(**) Use `release-notes` label for scenarios like:
- New configuration options
- Deprecation of configurations
- Signature changes to public methods/interfaces
- New plugins added or old plugins removed
