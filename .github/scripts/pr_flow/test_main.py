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

"""Publication, revision and fork-signal regression tests without network access."""

import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

import main as flow


KEY = "test-only-openrouter-key"
HEAD = "a" * 40
BASE = "b" * 40
MERGE_BASE = "c" * 40
MODEL = flow.DEFAULT_MODEL


def pr():
    return {"number": 42, "state": "open", "title": "Cache parsed metadata", "body": "Author text\n\n",
            "changed_files": 1, "base": {"sha": BASE, "ref": "master",
                                          "repo": {"id": 19961085, "full_name": "apache/pinot"}},
            "head": {"sha": HEAD, "ref": "topic", "repo": {"id": 7, "full_name": "contributor/pinot"}}}


def metadata(pull):
    return {"policy": flow.POLICY, "model": MODEL, "head_sha": HEAD, "base_sha": MERGE_BASE,
            "observed_base_sha": BASE, "context_hash": flow.context_hash(pull, KEY)}


def with_flow(pull=None):
    pull = copy.deepcopy(pull or pr())
    pull["body"] = flow.make_block("### PR flow\nDiagram", metadata(pull), 42, KEY) + pull["body"]
    return pull


def client():
    api = Mock()
    api.pr.return_value = pr()
    api.get.return_value = {"merge_base_commit": {"sha": MERGE_BASE}}
    api.pages.return_value = [{"filename": "src/Cache.java", "status": "modified", "additions": 1,
                               "deletions": 1, "patch": "@@ -1 +1 @@\n-old\n+new"}]
    api.history.return_value = []
    api.history_metadata = {}
    api.viewer = "github-actions[bot]"
    return api


class OwnershipTest(unittest.TestCase):
    def test_preserves_author_text_exactly_on_insert_and_replace(self):
        pull = pr()
        original = pull["body"]
        first = flow.make_block("### PR flow\nfirst", metadata(pull), 42, KEY)
        pull["body"] = flow.upsert(original, first, 42, KEY)
        self.assertEqual(flow.author_body(pull, KEY), original)
        second = flow.make_block("### PR flow\nsecond", metadata(pull), 42, KEY)
        pull["body"] = flow.upsert(pull["body"], second, 42, KEY)
        self.assertEqual(flow.author_body(pull, KEY), original)
        self.assertEqual(pull["body"].count(flow.START), 1)
        self.assertNotIn("first", pull["body"])

    def test_only_checkbox_can_change_without_invalidating_signature(self):
        pull = with_flow()
        self.assertFalse(flow.section(pull["body"], 42, KEY)["requested"])
        for checked in ("[x]", "[X]"):
            body = pull["body"].replace("[ ]", checked)
            self.assertTrue(flow.section(body, 42, KEY)["requested"])
        with self.assertRaises(flow.FlowError):
            flow.section(pull["body"].replace("Diagram", "edited"), 42, KEY)

    def test_copying_or_key_rotation_cannot_adopt_a_block(self):
        body = with_flow()["body"]
        for number, key in ((43, KEY), (42, "rotated-key")):
            with self.assertRaises(flow.FlowError):
                flow.section(body, number, key)

    def test_malformed_markers_fail_closed(self):
        for body in (flow.START + "user text", flow.END + "\n\n" + flow.START,
                     with_flow()["body"] + flow.END, with_flow()["body"].replace(flow.END + "\n\n", flow.END)):
            with self.assertRaises(flow.FlowError):
                flow.section(body, 42, KEY)

    def test_author_edits_outside_owned_section_are_preserved(self):
        pull = with_flow()
        pull["body"] = "Preface\n" + pull["body"] + "Suffix\n"
        self.assertEqual(flow.author_body(pull, KEY), "Preface\nAuthor text\n\nSuffix\n")


class PlanningTest(unittest.TestCase):
    def test_current_flow_skips_model_work_but_edit_or_checkbox_invalidates(self):
        api = client()
        pull = with_flow()
        self.assertTrue(flow.current(api, pull, KEY, MODEL))
        api.get.assert_not_called()
        pull["body"] += "New author context"
        self.assertFalse(flow.current(api, pull, KEY, MODEL))
        pull = with_flow()
        pull["body"] = pull["body"].replace("[ ]", "[x]")
        self.assertFalse(flow.current(api, pull, KEY, MODEL))

    def test_unrelated_master_movement_does_not_regenerate(self):
        api = client()
        pull = with_flow()
        pull["base"]["sha"] = "d" * 40
        self.assertTrue(flow.current(api, pull, KEY, MODEL))
        api.get.return_value = {"merge_base_commit": {"sha": "e" * 40}}
        self.assertFalse(flow.current(api, pull, KEY, MODEL))

    def signal(self):
        return {"workflow_run": {"id": 34171724224}}

    def run_data(self):
        return {"event": "pull_request", "status": "completed", "conclusion": "success",
                "workflow_id": 77, "path": ".github/workflows/pr-flow-signal.yml", "pull_requests": [],
                "repository": {"id": 19961085, "full_name": "apache/pinot"},
                "head_repository": {"id": 7, "full_name": "contributor/pinot"},
                "head_branch": "topic", "head_sha": HEAD}

    def test_resolves_fork_even_when_run_pull_requests_is_empty(self):
        api = Mock()
        api.get.side_effect = [self.run_data(), {"id": 77}]
        api.pages.return_value = [pr()]
        self.assertEqual(flow.resolve_run(api, self.signal()), [pr()])
        self.assertIn("head=contributor%3Atopic", api.pages.call_args.args[0])

    def test_spoofed_workflow_and_fork_mismatch_do_not_generate(self):
        for field, value in (("workflow_id", 78), ("event", "push"), ("conclusion", "failure"),
                             ("path", ".github/workflows/attacker.yml")):
            run = self.run_data()
            run[field] = value
            api = Mock()
            api.get.side_effect = [run, {"id": 77}]
            with self.assertRaises(flow.FlowError):
                flow.resolve_run(api, self.signal())
        api = Mock()
        api.get.side_effect = [self.run_data(), {"id": 77}]
        candidate = pr()
        candidate["head"]["repo"]["id"] = 99
        api.pages.return_value = [candidate]
        self.assertEqual(flow.resolve_run(api, self.signal()), [])

    def test_superseded_head_is_a_noop_and_ambiguity_fails(self):
        for candidates in ([pr(), pr()], [pr()]):
            api = Mock()
            run = self.run_data()
            if len(candidates) == 1:
                run["head_sha"] = "e" * 40
            api.get.side_effect = [run, {"id": 77}]
            api.pages.return_value = candidates
            if len(candidates) == 2:
                with self.assertRaises(flow.FlowError):
                    flow.resolve_run(api, self.signal())
            else:
                self.assertEqual(flow.resolve_run(api, self.signal()), [])

    def test_signal_head_change_during_plan_refresh_selects_nothing(self):
        api = Mock()
        api.get.side_effect = [self.run_data(), {"id": 77}]
        api.pages.return_value = [pr()]
        fresh = pr()
        fresh["head"]["sha"] = "f" * 40
        api.pr.return_value = fresh
        with patch("main.current") as current:
            self.assertEqual(flow.plan(api, KEY, MODEL, self.signal(), "workflow_run"), {"include": []})
            current.assert_not_called()

    def test_signal_revision_survives_matrix_and_skips_a_superseded_queued_run(self):
        api = Mock()
        api.get.side_effect = [self.run_data(), {"id": 77}]
        api.pages.return_value = [pr()]
        api.pr.return_value = pr()
        matrix = flow.plan(api, KEY, MODEL, self.signal(), "workflow_run")
        self.assertEqual(matrix, {"include": [{"pr_number": 42, "expected_head_sha": HEAD}]})
        fresh = pr()
        fresh["head"]["sha"] = "f" * 40
        api.pr.return_value = fresh
        with tempfile.TemporaryDirectory() as directory, patch("main.current") as current, \
                patch("main.collect") as collect, patch("main.generate") as generate:
            entry = matrix["include"][0]
            status = flow.run(api, KEY, MODEL, entry["pr_number"], Path(directory),
                              expected_head_sha=entry["expected_head_sha"])
            self.assertEqual(status, "skipped_superseded_signal")
            current.assert_not_called()
            collect.assert_not_called()
            generate.assert_not_called()
            api.update.assert_not_called()

    def test_large_current_backlog_bounds_and_rotates_comparisons_without_pr_reads(self):
        inventory = []
        for index in range(450):
            pull = pr()
            pull["number"] = index + 1
            pull["body"] = flow.make_block("Diagram", metadata(pull), pull["number"], KEY) + pull["body"]
            pull["base"]["sha"] = "d" * 40
            pull.pop("changed_files")  # The list API does not provide this generation-only field.
            inventory.append(pull)
        for hour in (0, 1):
            api = Mock()
            api.pages.return_value = inventory
            api.get.return_value = {"merge_base_commit": {"sha": MERGE_BASE}}
            with patch("main.time.time", return_value=hour * 3600), \
                    patch("main.current", wraps=flow.current) as current:
                self.assertEqual(flow.plan(api, KEY, MODEL, {}, "schedule"), {"include": []})
            api.pages.assert_called_once_with("pulls?state=open&base=master&sort=created&direction=asc", 1000)
            api.pr.assert_not_called()
            self.assertEqual(api.get.call_count, 200)
            visited = [call.args[1]["number"] for call in current.call_args_list]
            self.assertEqual(visited, list(range(hour * 203 + 1, hour * 203 + 201)))

    def test_permanently_failing_prefix_cannot_starve_a_scan_sized_inventory(self):
        for size in (100, 200, 400):
            candidates = []
            for index in range(size):
                candidate = pr()
                candidate["number"] = index + 1
                candidates.append(candidate)
            api = client()
            api.pages.return_value = candidates
            first_candidates = set()
            # No diagram succeeds; the candidates remain identical on each sweep.
            for hour in range(size):
                with patch("main.time.time", return_value=hour * 3600):
                    chosen = flow.plan(api, KEY, MODEL, {}, "schedule")
                first_candidates.add(chosen["include"][0]["pr_number"])
            self.assertEqual(first_candidates, set(range(1, size + 1)))

    def test_force_requires_explicit_pr_and_cannot_bypass_ownership(self):
        api = client()
        with self.assertRaises(flow.FlowError):
            flow.plan(api, KEY, MODEL, {}, "workflow_dispatch", force=True)
        pull = with_flow()
        pull["body"] = pull["body"].replace("Diagram", "edited")
        api.pr.return_value = pull
        with self.assertRaises(flow.FlowError):
            flow.plan(api, KEY, MODEL, {}, "workflow_dispatch", pr_number="42", force=True)

    def test_sweep_continues_past_invalid_owned_block(self):
        api = client()
        bad = with_flow()
        bad["body"] = bad["body"].replace("Diagram", "edited")
        good = pr()
        good["number"] = 43
        api.pages.return_value = [bad, good]
        api.pr.side_effect = lambda value: bad if value == 42 else good
        with patch("main.time.time", return_value=0):
            self.assertEqual(flow.plan(api, KEY, MODEL, {}, "schedule"), {"include": [{"pr_number": 43}]})


class CollectionTest(unittest.TestCase):
    def test_complete_and_missing_or_truncated_diff_are_distinguished(self):
        api = client()
        evidence = flow.collect(api, pr(), KEY)
        self.assertEqual(evidence["base_sha"], MERGE_BASE)
        self.assertTrue(evidence["files"][0]["patch_complete"])
        self.assertEqual(evidence["coverage"]["truncated_files"], 0)
        api.pages.return_value[0]["additions"] = 20
        self.assertEqual(flow.collect(api, pr(), KEY)["coverage"]["truncated_files"], 1)
        api.pages.return_value[0]["patch"] = ""
        with self.assertRaises(flow.FlowError):
            flow.collect(api, pr(), KEY)

    def test_full_inventory_and_size_limits_are_enforced(self):
        api = client()
        api.pages.return_value = []
        with self.assertRaises(flow.FlowError):
            flow.collect(api, pr(), KEY)
        api = client()
        api.pages.return_value[0]["patch"] = "+x\n" * 50000
        evidence = flow.collect(api, pr(), KEY)
        self.assertLessEqual(len(flow.packed(evidence).encode()), flow.MAX_EVIDENCE_BYTES)
        self.assertEqual(evidence["coverage"]["truncated_files"], 1)

    def test_pagination_never_silently_accepts_overflow(self):
        api = flow.GitHub("fake")
        api.get = Mock(side_effect=[[{}] * 100, [{}]])
        with self.assertRaises(flow.FlowError):
            api.pages("pulls", 100)


class HistoryTest(unittest.TestCase):
    def test_nullable_old_diff_and_creation_metadata_are_preserved(self):
        api = flow.GitHub("test-token")
        nodes = [{"id": "old-empty", "editedAt": "2026-09-08T00:00:00Z", "diff": None,
                  "editor": {"login": "contributor"}}]
        api.request = Mock(return_value={"data": {
            "viewer": {"login": "github-actions[bot]"}, "repository": {"pullRequest": {
                "createdAt": "2026-09-08T00:00:00Z",
                "includesCreatedEdit": True, "userContentEdits": {"nodes": nodes}}}}})
        self.assertEqual(api.history(42), nodes)
        self.assertEqual(api.history_metadata[42], {
            "created_at": "2026-09-08T00:00:00Z", "includes_created_edit": True})
        query = api.request.call_args.args[1]["query"]
        self.assertIn("createdAt", query)
        self.assertIn("includesCreatedEdit", query)
        # GitHub exposes this field on PullRequest, not UserContentEditConnection.
        self.assertRegex(query, r"createdAt\s+includesCreatedEdit\s+userContentEdits")


class PublicationTest(unittest.TestCase):
    def execute(self, api, directory, **options):
        with patch("main.generate", return_value=({"graph": "validated"}, {"total_tokens": 123})), \
                patch("main.render", return_value="### PR flow\nflowchart"):
            return flow.run(api, KEY, MODEL, 42, Path(directory), **options)

    def test_preview_does_not_write(self):
        api = client()
        with tempfile.TemporaryDirectory() as directory:
            self.assertEqual(self.execute(api, directory, preview=True), "preview_only")
            api.update.assert_not_called()
            self.assertTrue((Path(directory) / "flow.md").exists())
            self.assertTrue((Path(directory) / "usage.json").exists())

    def test_new_push_author_edit_or_merge_base_change_prevents_publication(self):
        for change in ("head", "body", "base", "closed", "title"):
            api = client()
            updated = pr()
            if change == "head":
                updated["head"]["sha"] = "f" * 40
            elif change == "body":
                updated["body"] = "Human edit"
            elif change == "closed":
                updated["state"] = "closed"
            elif change == "title":
                updated["title"] = "New title"
            else:
                api.get.side_effect = [{"merge_base_commit": {"sha": MERGE_BASE}},
                                       {"merge_base_commit": {"sha": "f" * 40}}]
            api.pr.side_effect = [pr(), updated]
            with tempfile.TemporaryDirectory() as directory:
                self.assertEqual(self.execute(api, directory), "deferred_pr_changed_during_generation")
                api.update.assert_not_called()

    def test_final_read_catches_a_late_author_edit(self):
        api = client()
        late = pr()
        late["body"] = "Late author edit"
        api.pr.side_effect = [pr(), pr(), late]
        with tempfile.TemporaryDirectory() as directory:
            self.assertEqual(self.execute(api, directory), "deferred_pr_changed_before_publication")
            api.update.assert_not_called()

    def test_publication_keeps_recovery_and_verifies_server_result(self):
        api = client()
        server = pr()
        api.pr.side_effect = lambda value: copy.deepcopy(server)
        api.update.side_effect = lambda value, body: server.update(body=body)
        api.history.side_effect = lambda value: ([] if server["body"] == pr()["body"] else [
            {"id": "own-edit", "diff": server["body"], "editor": {"login": "github-actions[bot]"}}])
        with tempfile.TemporaryDirectory() as directory:
            self.assertEqual(self.execute(api, directory), "published")
            saved = json.loads((Path(directory) / "previous-description.json").read_text())
            self.assertEqual(saved["body"], pr()["body"])
            self.assertEqual(flow.author_body(server, KEY), pr()["body"])
            self.assertTrue(flow.section(server["body"], 42, KEY))

    def creation_history(self, api, server, original, creation_diff, intervening=None):
        created_at = "2026-09-08T00:00:00Z"

        def history(value):
            published = server["body"] != original
            api.history_metadata[value] = {"created_at": created_at, "includes_created_edit": published}
            if not published:
                return []
            return ([{"id": "own-edit", "diff": server["body"], "editor": {"login": "github-actions[bot]"},
                      "editedAt": "2026-09-08T01:00:00Z"}]
                    + ([intervening] if intervening else [])
                    + [{"id": "creation", "diff": creation_diff, "editor": {"login": "contributor"},
                        "editedAt": created_at}])

        api.pr.side_effect = lambda value: copy.deepcopy(server)
        api.update.side_effect = lambda value, body: server.update(body=body)
        api.history.side_effect = history

    def test_first_publication_accepts_lazily_exposed_creation_snapshot(self):
        api, server = client(), pr()
        original = server["body"]
        self.creation_history(api, server, original, original)
        with tempfile.TemporaryDirectory() as directory:
            self.assertEqual(self.execute(api, directory), "published")
            self.assertEqual(flow.author_body(server, KEY), original)
            saved = json.loads((Path(directory) / "publication-edit-history.json").read_text())
            self.assertEqual(saved["before"], [])
            self.assertEqual([edit["id"] for edit in saved["after"]], ["own-edit", "creation"])
            self.assertTrue(saved["after_metadata"]["includes_created_edit"])

    def test_empty_original_accepts_null_creation_snapshot(self):
        for original in ("", None):
            with self.subTest(original=original):
                api, server = client(), pr()
                server["body"] = original
                self.creation_history(api, server, original, None)
                with tempfile.TemporaryDirectory() as directory:
                    self.assertEqual(self.execute(api, directory), "published")
                    self.assertEqual(flow.author_body(server, KEY), "")

    def test_older_null_history_does_not_block_publication(self):
        api, server = client(), pr()
        original = server["body"]
        old = {"id": "old-empty", "diff": None, "editor": {"login": "contributor"}}
        api.pr.side_effect = lambda value: copy.deepcopy(server)
        api.update.side_effect = lambda value, body: server.update(body=body)
        api.history.side_effect = lambda value: ([old] if server["body"] == original else [
            {"id": "own-edit", "diff": server["body"], "editor": {"login": "github-actions[bot]"}}, old])
        with tempfile.TemporaryDirectory() as directory:
            self.assertEqual(self.execute(api, directory), "published")

    def test_creation_snapshot_does_not_hide_a_genuine_intervening_edit(self):
        api, server = client(), pr()
        original = server["body"]
        late = {"id": "late-edit", "diff": "Intervening human edit", "editor": {"login": "contributor"},
                "editedAt": "2026-09-08T00:59:59Z"}
        self.creation_history(api, server, original, original, intervening=late)
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(flow.FlowError, "Concurrent edit"):
                self.execute(api, directory)
            saved = json.loads((Path(directory) / "publication-edit-history.json").read_text())
            self.assertEqual(saved["after"][1], late)

    def test_creation_baseline_requires_exact_metadata_and_original_content(self):
        own = {"id": "own-edit", "diff": "Desired", "editor": {"login": "github-actions[bot]"},
               "editedAt": "2026-09-08T01:00:00Z"}
        baseline = {"id": "creation", "diff": "Original", "editor": {"login": "contributor"},
                    "editedAt": "2026-09-08T00:00:00Z"}
        for mismatch in ("timestamp", "content", "includes_created_edit"):
            with self.subTest(mismatch=mismatch):
                api = client()
                creation = copy.deepcopy(baseline)
                metadata = {"created_at": baseline["editedAt"], "includes_created_edit": True}
                if mismatch == "timestamp":
                    creation["editedAt"] = "2026-09-08T00:00:01Z"
                elif mismatch == "content":
                    creation["diff"] = "A different original body"
                else:
                    metadata["includes_created_edit"] = False
                api.history_metadata[42] = metadata
                api.history.return_value = [own, creation]
                with tempfile.TemporaryDirectory() as directory:
                    with self.assertRaisesRegex(flow.FlowError, "Concurrent edit"):
                        flow.audit_publication(api, 42, [], "Original", "Desired", Path(directory))

    def test_edit_between_final_read_and_patch_is_retained_and_reported(self):
        api = client()
        server = pr()
        api.pr.side_effect = lambda value: copy.deepcopy(server)
        late_edit = "Human edit saved after the final read"
        api.update.side_effect = lambda value, body: server.update(body=body)
        api.history.side_effect = lambda value: ([] if server["body"] == pr()["body"] else [
            {"id": "own-edit", "diff": server["body"], "editor": {"login": "github-actions[bot]"}},
            {"id": "late-edit", "diff": late_edit, "editor": {"login": "contributor"}}])
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(flow.FlowError, "Concurrent edit"):
                self.execute(api, directory)
            saved = json.loads((Path(directory) / "publication-edit-history.json").read_text())
            self.assertEqual(saved["after"][1]["diff"], late_edit)

    def test_unavailable_edit_history_prevents_write(self):
        api = client()
        api.history.side_effect = flow.FlowError("History unavailable")
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaises(flow.FlowError):
                self.execute(api, directory)
            api.update.assert_not_called()

    def test_provider_error_preserves_existing_flow(self):
        api = client()
        api.pr.return_value = with_flow()
        with tempfile.TemporaryDirectory() as directory, patch("main.generate", side_effect=flow.ModelError("failed")):
            with self.assertRaises(flow.ModelError):
                flow.run(api, KEY, MODEL, 42, Path(directory), force=True)
            api.update.assert_not_called()


class MainStatusTest(unittest.TestCase):
    def test_main_quota_writes_status_and_summary(self):
        api = client()
        api.pr.return_value = with_flow()
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "output"
            summary = Path(directory) / "summary.md"
            environment = {"GITHUB_REPOSITORY": flow.REPOSITORY, "GITHUB_TOKEN": "test-github-token",
                           "OPEN_ROUTER_API_KEY": KEY, "PR_FLOW_MODEL": MODEL,
                           "PR_NUMBER": "42", "FORCE": "true", "PREVIEW": "false",
                           "PR_FLOW_OUTPUT_DIR": str(output), "GITHUB_STEP_SUMMARY": str(summary)}
            with patch.dict("os.environ", environment, clear=True), patch("sys.argv", ["main.py", "run"]), \
                    patch("main.GitHub", return_value=api), \
                    patch("main.generate", side_effect=flow.QuotaError(429)), patch("builtins.print"):
                flow.main()
            self.assertEqual(json.loads((output / "status.json").read_text()),
                             {"pr_number": 42, "status": "deferred_openrouter_quota"})
            self.assertIn("PR #42: **deferred_openrouter_quota**", summary.read_text())
            self.assertIn(MODEL, summary.read_text())
            api.update.assert_not_called()


if __name__ == "__main__":
    unittest.main()
