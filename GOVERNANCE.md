# Governance

The 21 Cloud runtime is maintained by the 21 Cloud team with contributions from the community.

## Roles

| Role | Who | Can |
|---|---|---|
| Contributor | anyone who opens an issue, a discussion or a PR | propose changes |
| Maintainer | listed in [.github/CODEOWNERS](.github/CODEOWNERS) | review and merge PRs, triage issues, cut releases |
| Lead maintainer | the 21 Cloud project lead | final say when maintainers cannot agree; security response; adds or removes maintainers |

## Decisions

- Day-to-day changes: a PR with one maintainer approval and green CI.
- Larger changes (a new `std:*` module, a breaking API or config change, a new trust boundary): an issue labeled
  `proposal` first, open for comment for at least 7 days, then accepted or declined by the maintainers with the
  reasons written down in the issue.
- Lazy consensus: silence after the comment period counts as agreement; any maintainer may ask for more time.

## Becoming a maintainer

Sustained, high-quality contributions and reviews over several months. An existing maintainer nominates; the lead
maintainer decides after asking the others.

## Releases

Maintainers cut releases by merging the release-please PR and pushing the `vX.Y.Z` tag
([.github/BRANCHING.md](.github/BRANCHING.md)). Security fixes follow [SECURITY.md](SECURITY.md).

## Code of conduct

Everyone follows the [Code of Conduct](CODE_OF_CONDUCT.md).
