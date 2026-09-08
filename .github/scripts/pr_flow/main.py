# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Maintain a signed PR-description diagram using public API data, never PR code."""

import base64
import hashlib
import hmac
import http.client
import json
import math
import os
from pathlib import Path
import re
import sys
import time
from urllib.parse import urlencode

from model import DEFAULT_MODEL, ModelError, QuotaError, generate, render

REPOSITORY = "apache/pinot"
POLICY = "pinot-pr-flow-v1"
START = "<!-- pinot-pr-flow:start -->"
END = "<!-- pinot-pr-flow:end -->"
CHECKBOX = "- [ ] Regenerate PR flow"
META_RE = re.compile(r"<!-- pinot-pr-flow:meta ([A-Za-z0-9_-]+) -->")
SIGNATURE_RE = re.compile(r"\n<!-- pinot-pr-flow:signature ([a-f0-9]{64}) -->")
MAX_EVIDENCE_BYTES = 180_000
MAX_PATCH_BYTES = 24_000
MAX_PR_FILES = 3_000
MAX_SCAN_PRS = 200


class FlowError(RuntimeError):
    """A safe, operator-facing failure; never contains response bodies or tokens."""


def packed(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"))


def digest(value):
    return hashlib.sha256(packed(value).encode()).hexdigest()


def number(value):
    if not re.fullmatch(r"[1-9][0-9]{0,19}", str(value)):
        raise FlowError("PR number must be a positive integer")
    return int(value)


def boolean(value):
    if value not in ("", "true", "false", None):
        raise FlowError("Boolean options must be true or false")
    return value == "true"


def sha(value):
    if not isinstance(value, str) or not re.fullmatch(r"[a-f0-9]{40}", value):
        raise FlowError("GitHub returned an invalid revision")
    return value


class GitHub:
    """Bounded GitHub-only transport with no redirects, proxy use, or shell calls."""

    def __init__(self, token):
        if not token or any(c in token for c in "\r\n"):
            raise FlowError("GITHUB_TOKEN is required")
        self.token = token
        self.calls = 0
        self.viewer = None
        self.history_metadata = {}

    def request(self, path, payload=None, graphql=False):
        self.calls += 1
        if self.calls > 800 or not (path.startswith("/repos/apache/pinot/") or graphql and path == "/graphql"):
            raise FlowError("GitHub request is outside the bounded repository scope")
        connection = http.client.HTTPSConnection("api.github.com", timeout=60)
        try:
            connection.request("POST" if graphql else "PATCH" if payload is not None else "GET", path,
                               body=packed(payload).encode() if payload is not None else None,
                               headers={"Authorization": "Bearer " + self.token,
                                        "Accept": "application/vnd.github+json",
                                        "Content-Type": "application/json",
                                        "X-GitHub-Api-Version": "2022-11-28",
                                        "User-Agent": "apache-pinot-pr-flow"})
            response = connection.getresponse()
            if response.status not in (200, 201):
                raise FlowError(f"GitHub HTTP {response.status}; see workflow permissions or API limits")
            raw = response.read(8_000_001)
            if len(raw) > 8_000_000:
                raise FlowError("GitHub response exceeded the size limit")
            return json.loads(raw)
        except (OSError, http.client.HTTPException, ValueError, RecursionError):
            raise FlowError("GitHub request failed; no response body was logged") from None
        finally:
            connection.close()

    def get(self, suffix):
        return self.request("/repos/" + REPOSITORY + "/" + suffix)

    def pr(self, pr_number):
        return self.get("pulls/" + str(number(pr_number)))

    def update(self, pr_number, body):
        return self.request("/repos/" + REPOSITORY + "/pulls/" + str(number(pr_number)), {"body": body})

    def history(self, pr_number):
        query = """query($number:Int!) {
          viewer { login }
          repository(owner:"apache", name:"pinot") {
            pullRequest(number:$number) {
              createdAt
              includesCreatedEdit
              userContentEdits(first:20) {
                nodes { id editedAt diff editor { login } }
              }
            }
          }
        }"""
        response = self.request("/graphql", {"query": query, "variables": {"number": number(pr_number)}},
                                graphql=True)
        if response.get("errors"):
            raise FlowError("PR edit history is unavailable; preserving the description")
        self.viewer = response["data"]["viewer"]["login"]
        if not isinstance(self.viewer, str) or not self.viewer:
            raise FlowError("Cannot verify the publication identity")
        pull = response["data"]["repository"]["pullRequest"]
        history = pull["userContentEdits"]
        nodes = history["nodes"]
        if not isinstance(nodes, list) or any(not isinstance(item, dict) or not isinstance(item.get("id"), str)
                                              or "diff" not in item
                                              or item["diff"] is not None and not isinstance(item["diff"], str)
                                              for item in nodes):
            raise FlowError("PR edit history cannot be audited; preserving the description")
        if (not isinstance(pull.get("createdAt"), str) or not pull["createdAt"]
                or type(pull.get("includesCreatedEdit")) is not bool):
            raise FlowError("PR edit history metadata is unavailable; preserving the description")
        self.history_metadata[number(pr_number)] = {
            "created_at": pull["createdAt"], "includes_created_edit": pull["includesCreatedEdit"]}
        return nodes

    def pages(self, suffix, maximum):
        items = []
        separator = "&" if "?" in suffix else "?"
        for page in range(1, maximum // 100 + 2):
            batch = self.get(f"{suffix}{separator}per_page=100&page={page}")
            if not isinstance(batch, list) or len(batch) > 100:
                raise FlowError("GitHub returned an invalid paginated inventory")
            items.extend(batch)
            if len(items) > maximum:
                raise FlowError("GitHub inventory exceeds the configured limit")
            if len(batch) < 100:
                return items
        raise FlowError("GitHub inventory was not completely paginated")


def bounds(body):
    if START not in body and END not in body:
        return None
    if body.count(START) != 1 or body.count(END) != 1:
        raise FlowError("Duplicate or incomplete PR flow markers; preserving the description")
    start, end = body.index(START), body.index(END) + len(END)
    if start >= end - len(END) or not body.startswith("\n\n", end):
        raise FlowError("Malformed PR flow boundaries; preserving the description")
    return start, end + 2


def signature(unsigned, pr_number, key):
    signing_key = hashlib.sha256(("apache-pinot-pr-flow-signing-v1\0" + key).encode()).digest()
    normalized = unsigned.replace("- [x] Regenerate PR flow", CHECKBOX).replace(
        "- [X] Regenerate PR flow", CHECKBOX)
    return hmac.new(signing_key, packed([REPOSITORY, number(pr_number), normalized]).encode(),
                    hashlib.sha256).hexdigest()


def section(body, pr_number, key):
    """Authenticate the complete block, allowing only the regeneration checkbox to change."""
    span = bounds(body)
    if span is None:
        return None
    block = body[span[0]:span[1]]
    signatures, metadata = SIGNATURE_RE.findall(block), META_RE.findall(block)
    if len(signatures) != 1 or len(metadata) != 1:
        raise FlowError("Unrecognized PR flow ownership; preserving the description")
    unsigned = SIGNATURE_RE.sub("", block)
    if not hmac.compare_digest(signatures[0], signature(unsigned, pr_number, key)):
        raise FlowError("PR flow was edited or its signing key changed; preserving the description")
    try:
        meta = json.loads(base64.urlsafe_b64decode(metadata[0] + "=" * (-len(metadata[0]) % 4)))
    except (ValueError, UnicodeError, RecursionError):
        raise FlowError("Invalid PR flow metadata") from None
    if not isinstance(meta, dict):
        raise FlowError("Invalid PR flow metadata")
    return {"metadata": meta, "span": span,
            "requested": "- [x] Regenerate PR flow" in block or "- [X] Regenerate PR flow" in block}


def author_body(pr, key):
    body = pr.get("body") or ""
    owned = section(body, pr["number"], key)
    return body if owned is None else body[:owned["span"][0]] + body[owned["span"][1]:]


def context_hash(pr, key):
    return digest({"title": pr["title"], "body": author_body(pr, key)})


def merge_base(client, pr):
    base, head = sha(pr["base"]["sha"]), sha(pr["head"]["sha"])
    comparison = client.get(f"compare/{base}...{head}?per_page=1")
    return sha(comparison["merge_base_commit"]["sha"])


def eligible(pr):
    return (pr.get("state") == "open" and pr["base"]["repo"]["full_name"] == REPOSITORY
            and pr["base"]["ref"] == "master" and pr["head"].get("repo") is not None)


def current(client, pr, key, model):
    owned = section(pr.get("body") or "", pr["number"], key)
    if not owned or owned["requested"]:
        return False
    meta = owned["metadata"]
    if (meta.get("policy") != POLICY or meta.get("model") != model
            or meta.get("head_sha") != pr["head"]["sha"]
            or meta.get("context_hash") != context_hash(pr, key)):
        return False
    # Unrelated master commits do not spend model quota on unchanged PR diffs.
    return (meta.get("observed_base_sha") == pr["base"]["sha"]
            or meta.get("base_sha") == merge_base(client, pr))


def resolve_run(client, event):
    """Resolve fork PRs from authenticated run metadata, never untrusted artifacts."""
    run_id = number(event["workflow_run"]["id"])
    run = client.get(f"actions/runs/{run_id}")
    workflow = client.get("actions/workflows/pr-flow-signal.yml")
    if (run.get("event") != "pull_request" or run.get("status") != "completed"
            or run.get("conclusion") != "success" or run.get("workflow_id") != workflow.get("id")
            or run.get("path", "").split("@")[0] != ".github/workflows/pr-flow-signal.yml"
            or run.get("repository", {}).get("full_name") != REPOSITORY):
        raise FlowError("Triggering run is not a successful PR flow signal")
    repository = run.get("head_repository") or {}
    full_name, branch = repository.get("full_name", ""), run.get("head_branch")
    if not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", full_name) or not branch:
        raise FlowError("Triggering run is missing its fork identity")
    head = sha(run.get("head_sha"))
    query = urlencode({"state": "open", "base": "master", "head": full_name.split("/")[0] + ":" + branch})
    candidates = client.pages("pulls?" + query, 1000)
    matches = [pr for pr in candidates if eligible(pr)
               and pr["base"]["repo"]["id"] == run["repository"]["id"]
               and pr["head"]["repo"]["id"] == repository.get("id")
               and pr["head"]["ref"] == branch and pr["head"]["sha"] == head]
    if len(matches) > 1:
        raise FlowError("Triggering run has ambiguous PR ownership")
    return matches  # A superseded head or closed PR needs no work.


def plan(client, key, model, event, event_name, pr_number="", force=False, limit=3):
    if force and not pr_number:
        raise FlowError("Forced regeneration requires one explicit PR number")
    if not 1 <= limit <= 10:
        raise FlowError("max_prs must be between 1 and 10")
    if event_name == "workflow_run":
        candidates = resolve_run(client, event)
    elif event_name == "workflow_dispatch" and pr_number:
        candidates = [client.pr(number(pr_number))]
    elif event_name in ("schedule", "workflow_dispatch"):
        candidates = client.pages("pulls?state=open&base=master&sort=created&direction=asc", 1000)
        # Bound comparisons and rotate the scan so failing PRs cannot starve the backlog.
        if candidates:
            stride = MAX_SCAN_PRS
            while math.gcd(stride, len(candidates)) != 1:
                stride += 1
            offset = (int(time.time() // 3600) * stride) % len(candidates)
            candidates = (candidates[offset:] + candidates[:offset])[:MAX_SCAN_PRS]
    else:
        raise FlowError("Unsupported workflow event")
    chosen = []
    for candidate in candidates:
        expected_head = ""
        pr = candidate  # Planning needs only list metadata; generation fetches the full live PR.
        if event_name == "workflow_run":
            expected_head = sha(candidate["head"]["sha"])
            pr = client.pr(candidate["number"])
            if pr["head"]["sha"] != expected_head:
                continue
        if not eligible(pr):
            continue
        try:
            section(pr.get("body") or "", pr["number"], key)
            if force or not current(client, pr, key, model):
                entry = {"pr_number": pr["number"]}
                if expected_head:
                    entry["expected_head_sha"] = expected_head
                chosen.append(entry)
        except FlowError as error:
            if pr_number or event_name == "workflow_run":
                raise
            print(f"PR #{pr['number']} skipped: {error}")
        if len(chosen) >= limit:
            break
    return {"include": chosen}


def clip(value, budget):
    raw = value.encode("utf-8")
    return raw[:budget].decode("utf-8", errors="ignore")


def collect(client, pr, key):
    total = pr.get("changed_files")
    if type(total) is not int or not 0 < total <= MAX_PR_FILES:
        raise FlowError("PR changed-file count is outside the supported range (1-3000)")
    files = client.pages(f"pulls/{pr['number']}/files", MAX_PR_FILES)
    if len(files) != total or len({f["filename"] for f in files}) != total:
        raise FlowError("PR file inventory changed or is incomplete; retry after the next sweep")
    evidence = {"repository": REPOSITORY, "pr_number": pr["number"],
                "title": clip(pr["title"], 1024), "description": clip(author_body(pr, key), 16_000),
                "head_sha": sha(pr["head"]["sha"]), "base_sha": merge_base(client, pr),
                "files": [], "coverage": {"total_files": total, "files_with_patches": 0,
                                           "omitted_files": 0, "truncated_files": 0}}
    # Keep broad source coverage before large test fixtures and generated patches.
    files.sort(key=lambda item: (bool(re.search(r"(^|/)(test|tests|resources|generated)(/|$)", item["filename"])),
                                 item["filename"]))
    for index, item in enumerate(files, 1):
        patch = item.get("patch") or ""
        limited = clip(patch, MAX_PATCH_BYTES)
        if limited != patch:
            limited = limited.rsplit("\n", 1)[0]
        added = sum(line.startswith("+") for line in patch.splitlines())
        deleted = sum(line.startswith("-") for line in patch.splitlines())
        complete = bool(patch) and limited == patch and added == item["additions"] and deleted == item["deletions"]
        record = {"id": f"F{index}", "path": item["filename"], "status": item["status"],
                  "additions": item["additions"], "deletions": item["deletions"],
                  "patch": limited, "patch_complete": complete}
        if item.get("previous_filename"):
            record["previous_path"] = item["previous_filename"]
        if len(packed(evidence).encode()) + len(packed(record).encode()) + 100 > MAX_EVIDENCE_BYTES:
            evidence["coverage"]["omitted_files"] += 1
            continue
        evidence["files"].append(record)
        if limited:
            evidence["coverage"]["files_with_patches"] += 1
            evidence["coverage"]["truncated_files"] += int(not complete)
        else:
            evidence["coverage"]["omitted_files"] += 1
    if not evidence["coverage"]["files_with_patches"]:
        raise FlowError("PR has no usable text diff; no behavioral diagram was generated")
    return evidence


def make_block(content, meta, pr_number, key):
    encoded = base64.urlsafe_b64encode(packed(meta).encode()).decode().rstrip("=")
    unsigned = (START + "\n\n" + content.rstrip() + "\n\n" + CHECKBOX + "\n\n"
                + f"<!-- pinot-pr-flow:meta {encoded} -->\n" + END + "\n\n")
    signed = signature(unsigned, pr_number, key)
    return unsigned.replace("\n" + END, f"\n<!-- pinot-pr-flow:signature {signed} -->\n" + END)


def upsert(body, block, pr_number, key):
    owned = section(body, pr_number, key)
    if owned is None:
        return block + body
    start, end = owned["span"]
    return body[:start] + block + body[end:]


def save(directory, name, value):
    directory.mkdir(parents=True, exist_ok=True)
    (directory / name).write_text(value if isinstance(value, str) else packed(value) + "\n", encoding="utf-8")


def audit_publication(client, pr_number, before, original, desired, directory):
    """Detect the non-atomic PATCH race and retain any intervening author edits."""
    anchor = before[0]["id"] if before else None
    for attempt in range(3):
        after = client.history(pr_number)
        new = []
        reached_anchor = anchor is None
        for edit in after:
            if edit["id"] == anchor:
                reached_anchor = True
                break
            new.append(edit)
        metadata = client.history_metadata.get(pr_number, {})
        # The first edit can expose the creation snapshot for the first time.
        # Only the oldest entry can be that baseline; retain every intervening edit.
        if (not before and new and metadata.get("includes_created_edit") is True
                and new[-1].get("editedAt") == metadata.get("created_at")
                and (new[-1]["diff"] if new[-1]["diff"] is not None else "") == original):
            new = new[:-1]
        save(directory, "publication-edit-history.json", {
            "before": before, "after": after, "after_metadata": metadata})
        # GitHub can expose the edit history shortly after the PR body update.
        if not new and attempt < 2:
            time.sleep(1)
            continue
        if (reached_anchor and len(new) == 1 and new[0]["diff"] == desired
                and (new[0].get("editor") or {}).get("login") == client.viewer):
            return
        raise FlowError("Concurrent edit or unverified publication history; inspect publication-edit-history.json")


def run(client, key, model, pr_number, directory, force=False, preview=False, expected_head_sha=""):
    pr = client.pr(pr_number)
    if not eligible(pr):
        return "skipped_closed_or_unsupported_base"
    if expected_head_sha and sha(expected_head_sha) != pr["head"]["sha"]:
        return "skipped_superseded_signal"
    if not force and current(client, pr, key, model):
        return "current"
    evidence = collect(client, pr, key)
    graph, usage = generate(evidence, key, model)
    save(directory, "usage.json", usage)
    content = "### PR flow\n\n" + render(graph, evidence)
    save(directory, "flow.md", content)
    original = pr.get("body") or ""
    fresh = client.pr(pr_number)
    if (not eligible(fresh) or fresh["head"]["sha"] != evidence["head_sha"]
            or (fresh.get("body") or "") != original or fresh["title"] != pr["title"]
            or merge_base(client, fresh) != evidence["base_sha"]):
        return "deferred_pr_changed_during_generation"
    meta = {"policy": POLICY, "model": model, "head_sha": evidence["head_sha"],
            "base_sha": evidence["base_sha"], "observed_base_sha": fresh["base"]["sha"],
            "context_hash": context_hash(fresh, key), "generated_at": int(time.time())}
    desired = upsert(original, make_block(content, meta, pr_number, key), pr_number, key)
    if len(desired.encode()) > 65_536:
        raise FlowError("PR description would exceed GitHub's size limit")
    save(directory, "proposed-description.md", desired)
    if preview:
        return "preview_only"
    save(directory, "previous-description.json", {"repository": REPOSITORY, "pr_number": pr_number,
                                                 "head_sha": evidence["head_sha"], "body": original})
    # GitHub has no atomic conditional PR-body PATCH. Narrow the race with a final read and verify afterward.
    before_history = client.history(pr_number)
    save(directory, "publication-edit-history.json", {"before": before_history})
    last = client.pr(pr_number)
    if (not eligible(last) or (last.get("body") or "") != original or last["title"] != fresh["title"]
            or last["head"]["sha"] != fresh["head"]["sha"] or last["base"]["sha"] != fresh["base"]["sha"]):
        return "deferred_pr_changed_before_publication"
    client.update(pr_number, desired)
    audit_publication(client, pr_number, before_history, original, desired, directory)
    verified = client.pr(pr_number)
    if (verified.get("body") != desired or verified["head"]["sha"] != evidence["head_sha"]
            or verified["title"] != fresh["title"] or verified["base"]["sha"] != fresh["base"]["sha"]):
        raise FlowError("PR changed during publication; inspect the retained recovery copy")
    return "published"


def main():
    if os.environ.get("GITHUB_REPOSITORY") != REPOSITORY:
        raise FlowError("This workflow is restricted to apache/pinot")
    key = os.environ.get("OPEN_ROUTER_API_KEY", "").strip()
    if not key or any(c in key for c in "\r\n"):
        raise FlowError("Repository secret OPEN_ROUTER_API_KEY is required")
    model = os.environ.get("PR_FLOW_MODEL") or DEFAULT_MODEL
    if not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.:-]+:free", model):
        raise FlowError("PR_FLOW_MODEL must be an explicit free OpenRouter model")
    client = GitHub(os.environ.get("GITHUB_TOKEN"))
    force = boolean(os.environ.get("FORCE"))
    pr_number = os.environ.get("PR_NUMBER", "")
    if len(sys.argv) != 2 or sys.argv[1] not in ("plan", "run"):
        raise FlowError("Expected plan or run")
    if sys.argv[1] == "plan":
        event_name = os.environ.get("GITHUB_EVENT_NAME")
        event_path = Path(os.environ["GITHUB_EVENT_PATH"])
        if event_path.stat().st_size > 2_000_000:
            raise FlowError("Workflow event exceeded the size limit")
        event = json.loads(event_path.read_text())
        matrix = plan(client, key, model, event, event_name, pr_number, force,
                      number(os.environ.get("MAX_PRS") or "3"))
        with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as stream:
            stream.write("matrix=" + packed(matrix) + "\n")
            stream.write("has_prs=" + str(bool(matrix["include"])).lower() + "\n")
        print(f"Selected {len(matrix['include'])} PR(s)")
        return
    directory = Path(os.environ["PR_FLOW_OUTPUT_DIR"])
    try:
        status = run(client, key, model, number(pr_number), directory, force,
                     boolean(os.environ.get("PREVIEW")),
                     expected_head_sha=os.environ.get("EXPECTED_HEAD_SHA", ""))
    except QuotaError:
        status = "deferred_openrouter_quota"
        print("::warning::OpenRouter free capacity unavailable; existing flow preserved. An hourly sweep will retry.")
    save(directory, "status.json", {"status": status, "pr_number": number(pr_number)})
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a", encoding="utf-8") as stream:
            stream.write(f"PR #{number(pr_number)}: **{status}**. Model: `{model}` (free-only).\n")
            if (directory / "flow.md").exists():
                stream.write("\n" + (directory / "flow.md").read_text())
    print(f"PR #{number(pr_number)}: {status}")


if __name__ == "__main__":
    try:
        main()
    except (FlowError, ModelError) as error:
        print(f"::error::{error}")
        sys.exit(1)
    except (KeyError, TypeError, ValueError, OSError, RecursionError):
        print("::error::Invalid workflow configuration or API data; no response body was logged")
        sys.exit(1)
