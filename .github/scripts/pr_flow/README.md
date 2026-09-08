<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->

# PR flow

This workflow adds a compact, colored Mermaid overview to a bot-owned block in a
PR description while preserving the author's text. It uses OpenRouter's free
models and the repository secret `OPEN_ROUTER_API_KEY`. The feature becomes
active when these workflows and scripts are merged into `apache/pinot`'s default
branch; adding the files in an unmerged PR does not deploy it.

## Triggers and controls

- PRs targeting `master` signal on open, new commits, reopen, becoming ready for
  review, and description edits. `PR flow signal` has no credentials, checkout,
  or PR-controlled shell input. After a successful signal, `PR flow` runs trusted
  default-branch scripts and obtains the PR's public source evidence through the
  GitHub API. It never checks out or executes the PR's code.
- An hourly run at minute 17 scans at most 200 candidates in a rotating window
  and selects a bounded backlog, by default at most three PRs. Rotation gives
  later PRs a turn when earlier ones repeatedly fail. This also recovers work
  when an external contributor's signal is waiting for approval or an earlier
  generation failed. Scheduling and free capacity do not guarantee immediate
  updates.
- Check the regenerate checkbox inside an existing bot block to request another
  generation. A signature made with an HMAC key derived from the OpenRouter
  secret identifies the bot-owned block. The raw secret is never embedded in the
  PR. Author text edits refresh the explanation; bot-only updates and unrelated
  commits to `master` do not spend quota when the PR diff is unchanged.
- Set repository variable `PR_FLOW_ENABLED=false` to stop generation. Any other
  value, including an unset variable, enables it. `PR_FLOW_MODEL` selects the
  model; its default is `nvidia/nemotron-3-super-120b-a12b:free`.

Only model IDs ending in `:free` are accepted, with routing restricted to zero
input/output prices. There is no paid fallback. Free model availability,
per-minute limits and the account's daily request quota can stop a run. Retries
also consume capacity. An hourly scan is bounded recovery, not a way around
OpenRouter's account limits; monitor its account dashboard and the Actions run
summary. If the selected free model disappears, choose another supported `:free`
model in `PR_FLOW_MODEL` and preview it before relying on its output.

## Preview and regenerate

Maintainers can generate a preview without editing the PR:

```sh
gh workflow run pr-flow.yml -R apache/pinot --ref master \
  -f pr_number=12345 -f preview=true
```

Add `-f force=true` to regenerate an already current revision. Omit
`-f preview=true` to publish the result. Leave `pr_number` empty to process the
bounded backlog; `-f max_prs=3` controls the selection bound, accepting 1–10.
Manual dispatch
executes scripts from the selected workflow ref, so use `master` or an explicitly
reviewed, trusted maintainer ref. Never dispatch a privileged run against
untrusted PR code.

The Actions run summary reports the outcome. Artifacts retain the generated
`flow.md`, sanitized `usage.json` and `status.json`, and the public PR's previous
description in `previous-description.json` for recovery, for seven days.
Publication also retains `publication-edit-history.json`. GitHub offers no atomic
conditional update of a PR description: a human edit in the final read/write
interval can still be overwritten. The publisher audits edit history afterward,
retains intervening edits for recovery, and reports a conflict instead of success.
It stops before writing when history cannot be read. Resolve a reported conflict
from the retained snapshots before regenerating the flow.
Failed generation, invalid output, or exhausted free
capacity preserves the previous diagram. The publisher rechecks the PR head
before writing so an obsolete generation cannot overwrite a newer revision's
flow. Per-PR concurrency serializes writers; it does not cancel an active job.

Rotating `OPEN_ROUTER_API_KEY` changes the derived signing key. Existing block
signatures become unverified, and `force` does not bypass signature checks or
overwrite manually edited blocks. A maintainer must remove the old managed block
from the PR description once, preserving the author's text, then force a new
generation.

## Evidence and maintenance

The diagram is a model-generated overview, not a human review, approval, test
result, or guarantee of complete runtime behavior. Source collection and prompt
size are bounded. The output identifies omitted or truncated evidence; large
PRs can therefore receive a partial overview. The model may still miss behavior
or connections even when the diagram renders successfully.

The workflows use Python 3.12 and SHA-pinned GitHub actions. The repository's
existing Dependabot `github-actions` entry maintains action updates. Privileged
runs check out only the workflow scripts at the trusted workflow SHA with
persisted Git credentials disabled. The signal/check workflows receive no
OpenRouter secret. The main workflow separates read-only planning from
PR-description writing and avoids `pull_request_target`, following the
[ASF GitHub Actions policy](https://infra.apache.org/github-actions-policy.html).

Run the focused checks locally:

```sh
python3 -m unittest discover -s .github/scripts/pr_flow -p 'test_*.py'
actionlint .github/workflows/pr-flow-signal.yml \
  .github/workflows/pr-flow.yml .github/workflows/pr-flow-checks.yml
```
