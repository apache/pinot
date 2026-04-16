# Code Coverage in Apache Pinot

## Overview

Apache Pinot uses [Codecov](https://codecov.io/) to track and report code coverage for all pull requests and commits to the main branch.

## How It Works

- Coverage reports are generated automatically via CI/CD pipelines (GitHub Actions) after each test run.
- The coverage report is uploaded to Codecov and linked back to the pull request as a comment.
- The Codecov badge and detailed report are available on the [Codecov dashboard](https://app.codecov.io/gh/apache/pinot).

## Coverage Requirements

- New code introduced in pull requests should be covered by unit and/or integration tests wherever feasible.
- Coverage diff is shown on each PR to help reviewers assess the impact of changes on overall coverage.

## Viewing Coverage Reports

1. Open a pull request on GitHub.
2. Look for the **Codecov** comment posted automatically by the Codecov bot.
3. Click the link in the comment to view the detailed coverage breakdown for the PR.

## Running Coverage Locally

To generate a coverage report locally, run:

```bash
mvn test -Pcoverage
```

The HTML report will be generated under `target/site/jacoco/index.html` for each module.

## Configuration

The Codecov configuration is located in [`codecov.yml`](../codecov.yml) at the repository root. It controls:

- Coverage targets (project and patch thresholds)
- Paths to ignore (e.g., generated code, test utilities)
- Comment behavior on pull requests

## Ignoring Files from Coverage

To exclude specific files or packages from coverage tracking, update the `ignore` section in `codecov.yml`:

```yaml
ignore:
  - "**/generated/**"
  - "**/thrift/**"
```

## References

- [Codecov Documentation](https://docs.codecov.com/docs)
- [JaCoCo Maven Plugin](https://www.jacoco.org/jacoco/trunk/doc/maven.html)
- [Apache Pinot Codecov Dashboard](https://app.codecov.io/gh/apache/pinot)
