#
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
#

"""Exercise compatibility server startup against real HTTP responses without building Pinot."""

import http.server
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import threading
import time
import unittest


HELPER = Path(__file__).with_name("server-readiness.inc").resolve()


class ReadinessTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name).resolve()
        self.logs = self.root / "logs"
        self.pids = self.root / "pids"
        self.bin = self.root / "bin"
        for directory in (self.logs, self.pids, self.bin):
            directory.mkdir()
        self.trace = self.root / "trace"
        self.env = dict(os.environ, LOG_DIR=str(self.logs), PID_DIR=str(self.pids),
                        JAVA_HOME="", PATH=str(self.bin) + os.pathsep + os.environ["PATH"],
                        TRACE=str(self.trace), SERVER_READY_TIMEOUT_SECONDS="2",
                        SERVER_CONF_2=str(self.root / "server2.properties"),
                        keepClusterOnFailure="false", HELPER=str(HELPER))
        self.processes = []
        self.servers = []
        self.write_script("jstack", 'kill -0 "$2" || exit 1\necho "thread dump"\necho dump >> "$TRACE"')

    def tearDown(self):
        for process in self.processes:
            process.terminate()
            process.wait()
        # Processes started by the shell launcher are not Python's children.
        for pid_file in self.pids.glob("*.pid"):
            try:
                os.kill(int(pid_file.read_text()), 15)
            except ProcessLookupError:
                pass
        for server in self.servers:
            server.shutdown()
            server.server_close()
        self.temp.cleanup()

    def write_script(self, name, body):
        script = self.bin / name
        script.write_text("#!/bin/bash\n" + body + "\n")
        script.chmod(0o755)
        return script

    def server_process(self, name="server"):
        process = subprocess.Popen(["sleep", "60"])
        self.processes.append(process)
        log_dir = self.logs / (name + ".1")
        log_dir.mkdir()
        (self.pids / (name + ".pid")).write_text(str(process.pid))
        (self.pids / (name + ".logdir")).write_text(str(log_dir))
        return process, log_dir

    def http_server(self, statuses=(503,), delay=0):
        class Handler(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                index = self.server.requests
                self.server.requests += 1
                time.sleep(delay)
                self.send_response(statuses[min(index, len(statuses) - 1)])
                self.end_headers()
                try:
                    self.wfile.write(b"readiness response")
                except BrokenPipeError:
                    pass

            def log_message(self, *args):
                pass

        server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        server.daemon_threads = True
        server.requests = 0
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.servers.append(server)
        return server

    def run_shell(self, body, **env):
        preamble = r'''
source "$HELPER"
function stopServices() {
  echo cleanup >> "$TRACE"
  for pidFile in "$PID_DIR"/*.pid; do
    kill "$(cat "$pidFile")" 2>/dev/null || true
  done
}
'''
        return subprocess.run(["bash", "-c", preamble + body], env=dict(self.env, **env),
                              capture_output=True, text=True, timeout=20, cwd=self.root)

    def test_retries_503_until_200(self):
        _, log_dir = self.server_process()
        server = self.http_server((503, 200))
        result = self.run_shell('waitForServerReady', SERVER_ADMIN_PORT=str(server.server_port),
                                SERVER_READY_TIMEOUT_SECONDS="5")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(server.requests, 2)
        self.assertIn("HTTP 503", (log_dir / "readiness.log").read_text())
        self.assertIn("HTTP 200", (log_dir / "readiness.log").read_text())
        self.assertFalse(self.trace.exists())

    def test_timeout_collects_threads_before_cleanup_and_aborts(self):
        process, log_dir = self.server_process()
        server = self.http_server()
        result = self.run_shell('waitForServerReady\necho should-not-run',
                                SERVER_ADMIN_PORT=str(server.server_port))
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertNotIn("should-not-run", result.stdout)
        self.assertEqual(self.trace.read_text().splitlines(), ["dump", "cleanup"])
        self.assertIn("thread dump", (log_dir / "threads.txt").read_text())
        self.assertIn("HTTP 503", (log_dir / "health-on-failure.txt").read_text())
        process.wait(timeout=2)

    def test_dead_process_fails_without_http_request(self):
        process, _ = self.server_process()
        process.terminate()
        process.wait()
        server = self.http_server((200,))
        result = self.run_shell('waitForServerReadiness server "$SERVER_ADMIN_PORT"',
                                SERVER_ADMIN_PORT=str(server.server_port))
        self.assertEqual(result.returncode, 1)
        self.assertEqual(server.requests, 0)
        self.assertIn("exited before becoming ready", result.stdout)

    def test_hanging_http_request_obeys_overall_deadline(self):
        self.server_process()
        server = self.http_server((200,), delay=60)
        result = self.run_shell('waitForServerReadiness server "$SERVER_ADMIN_PORT"',
                                SERVER_ADMIN_PORT=str(server.server_port))
        self.assertEqual(result.returncode, 1)

    def test_server2_failure_keeps_cluster_when_requested(self):
        process, log_dir = self.server_process("server2")
        Path(self.env["SERVER_CONF_2"]).touch()
        server = self.http_server()
        result = self.run_shell('waitForServer2Ready', SERVER_ADMIN_PORT="1",
                                SERVER_2_ADMIN_PORT=str(server.server_port), keepClusterOnFailure="true")
        self.assertEqual(result.returncode, 1)
        self.assertEqual(self.trace.read_text().splitlines(), ["dump"])
        self.assertIsNone(process.poll())
        self.assertTrue((log_dir / "threads.txt").exists())

    def test_stuck_thread_dump_is_bounded(self):
        process, log_dir = self.server_process()
        server = self.http_server()
        self.write_script("jstack", "exec sleep 60")
        result = self.run_shell('collectServerDiagnostics server "$SERVER_ADMIN_PORT"',
                                SERVER_ADMIN_PORT=str(server.server_port))
        self.assertEqual(result.returncode, 0)
        self.assertIn("Thread dump timed out", (log_dir / "threads.txt").read_text())
        self.assertIsNone(process.poll())

    def test_each_start_has_its_own_logs_and_preserves_the_working_directory(self):
        launcher = self.write_script("launcher", 'pwd > "$LOG_ROOT/working-directory.txt"\n'
                                     'echo "$LOG4J_CONFIGURATION_FILE" > "$LOG_ROOT/logging-config.txt"\n'
                                     'echo "$JAVA_OPTS" > "$LOG_ROOT/java-options.txt"\n'
                                     'echo server-and-helix > "$LOG_ROOT/pinot-$PINOT_COMPONENT.log"\n'
                                     'exec sleep 60')
        helper_dir = self.root / "helper with spaces"
        helper_dir.mkdir()
        copied_helper = helper_dir / HELPER.name
        shutil.copyfile(HELPER, copied_helper)
        copied_config = helper_dir / "server-log4j2.xml"
        shutil.copyfile(HELPER.with_name(copied_config.name), copied_config)
        launch_dir = self.root / "distribution-bin"
        launch_dir.mkdir()
        java_options = "-Xmx256m -DServerAdminPort=8097"
        result = self.run_shell(r'''
cd "$LAUNCH_DIR"
logCount=1
startServerWithLogs server "$LAUNCHER" missing-config
logCount=2
startServerWithLogs server2 "$LAUNCHER" missing-config
''', LAUNCHER=str(launcher), HELPER=str(copied_helper), LAUNCH_DIR=str(launch_dir), JAVA_OPTS=java_options)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        for name, number in (("server", 1), ("server2", 2)):
            directory = self.logs / (name + "." + str(number))
            log_file = directory / ("pinot-" + name + ".log")
            deadline = time.monotonic() + 2
            while not log_file.exists() and time.monotonic() < deadline:
                time.sleep(0.01)
            self.assertEqual((directory / "working-directory.txt").read_text().strip(), str(launch_dir))
            self.assertEqual((directory / "logging-config.txt").read_text().strip(), str(copied_config))
            self.assertEqual((directory / "java-options.txt").read_text().strip(), java_options)
            self.assertEqual(log_file.read_text().strip(), "server-and-helix")


if __name__ == "__main__":
    unittest.main()
