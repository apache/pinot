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

"""Verify the compatibility bootstrap in separate JVMs without building Pinot."""

import os
from pathlib import Path
import subprocess
import tempfile
import unittest


RUNNER = Path(__file__).with_name("compCheck.sh").resolve()


class ServerStartupTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="compat startup ")
        self.root = Path(self.temp.name).resolve()
        for directory in ("logs", "pids", "sources"):
            (self.root / directory).mkdir()
        self.trace = self.root / "trace"
        self.config = self.root / "server config.properties"
        self.config.touch()
        self.env = dict(os.environ, RUNNER=str(RUNNER), WORK_DIR=str(self.root), TRACE=str(self.trace),
                        LOG_DIR=str(self.root / "logs"), PID_DIR=str(self.root / "pids"),
                        CONFIG=str(self.config), JAVA_OPTS="-Xmx64m")

    def tearDown(self):
        self.temp.cleanup()

    def run_shell(self, body, **env):
        return subprocess.run(["bash", "-c", 'source "$RUNNER"\n' + body],
                              env=dict(self.env, **env), cwd=self.root,
                              capture_output=True, text=True, timeout=30)

    def distribution(self, version, include_lucene=True):
        distribution = self.root / (version + " distribution")
        package = distribution / "pinot-tools/target/pinot-tools-pkg"
        (package / "bin").mkdir(parents=True)
        classes = package / "lib"
        classes.mkdir()
        index_writer = self.root / "sources/IndexWriter.java"
        index_writer.write_text('''
package org.apache.lucene.index;
import java.nio.file.*;
public class IndexWriter {
  static {
    try {
      Files.writeString(Path.of(System.getenv("TRACE")), "lucene:" + ProcessHandle.current().pid() + "\\n",
          StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    } catch (Exception e) { throw new RuntimeException(e); }
  }
}
''')
        administrator = self.root / "sources/PinotAdministrator.java"
        administrator.write_text('''
package org.apache.pinot.tools.admin;
import java.nio.file.*;
public class PinotAdministrator {
  public static void main(String[] args) throws Exception {
    if (System.getenv("FAIL_ADMIN") != null) throw new IllegalStateException("administrator failed");
    Files.writeString(Path.of(System.getenv("TRACE")),
        "admin:" + ProcessHandle.current().pid() + ":VERSION:" + String.join("|", args) + "\\n"
        + System.getProperty("launcher.setting") + "\\n" + System.getProperty("app.home") + "\\n",
        StandardOpenOption.APPEND);
  }
}
'''.replace("VERSION", version))
        compiler = str(Path(os.environ["JAVA_HOME"]) / "bin/javac") if os.environ.get("JAVA_HOME") else "javac"
        sources = [str(administrator)] + ([str(index_writer)] if include_lucene else [])
        subprocess.run([compiler, "--release", "11", "-d", str(classes), *sources], check=True, capture_output=True)
        launcher = package / "bin/pinot-admin.sh"
        launcher.write_text('''#!/bin/bash
BASEDIR=$(cd "$(dirname "$0")/.." && pwd)
JAVACMD="${JAVA_HOME:+$JAVA_HOME/bin/}java"
exec "$JAVACMD" $JAVA_OPTS -Dlauncher.setting="preserved setting" -Dapp.home="$BASEDIR" \\
  -classpath "$CLASSPATH_PREFIX:$BASEDIR/lib" \\
  org.apache.pinot.tools.admin.PinotAdministrator \\
  "$@"
''')
        launcher.chmod(0o755)
        return distribution, launcher

    def test_initial_upgrade_and_rollback_initialize_in_the_server_jvm(self):
        old, old_launcher = self.distribution("old")
        new, _ = self.distribution("new")
        original_launcher = old_launcher.read_text()
        result = self.run_shell('''
setupCompatibilityServerStartup "$WORK_DIR" || exit 1
for distribution in "$OLD_DISTRIBUTION" "$NEW_DISTRIBUTION" "$OLD_DISTRIBUTION"; do
  startService server "$distribution" "$CONFIG" || exit 1
  pid=$(cat "$PID_DIR/server.pid")
  wait "$pid" || exit 1
  echo "$pid" >> "$WORK_DIR/launched-pids"
done
''', OLD_DISTRIBUTION=str(old), NEW_DISTRIBUTION=str(new))
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        pids = (self.root / "launched-pids").read_text().splitlines()
        lines = self.trace.read_text().splitlines()
        self.assertEqual(len(lines), 12)
        for index, (pid, version, distribution) in enumerate(zip(pids, ("old", "new", "old"), (old, new, old))):
            self.assertEqual(lines[4 * index], "lucene:" + pid)
            self.assertEqual(lines[4 * index + 1],
                             f"admin:{pid}:{version}:StartServer|-configFileName|{self.config}")
            self.assertEqual(lines[4 * index + 2], "preserved setting")
            self.assertEqual(lines[4 * index + 3], str(distribution / "pinot-tools/target/pinot-tools-pkg"))
        self.assertEqual(old_launcher.read_text(), original_launcher)
        bytecode = self.root / "server-startup/org/apache/pinot/compat/CompatibilityServerStarter.class"
        self.assertEqual(int.from_bytes(bytecode.read_bytes()[6:8], "big"), 55)

    def test_missing_lucene_fails_before_starting_administrator(self):
        distribution, _ = self.distribution("missing-lucene", include_lucene=False)
        result = self.run_shell('''
setupCompatibilityServerStartup "$WORK_DIR" || exit 1
startService server "$DISTRIBUTION" "$CONFIG" || exit 1
wait "$(cat "$PID_DIR/server.pid")"
''', DISTRIBUTION=str(distribution))
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(self.trace.exists())
        self.assertIn("ClassNotFoundException: org.apache.lucene.index.IndexWriter",
                      (self.root / "logs/server.1.log").read_text())

    def test_administrator_failure_is_propagated(self):
        distribution, _ = self.distribution("failing")
        result = self.run_shell('''
setupCompatibilityServerStartup "$WORK_DIR" || exit 1
startService server2 "$DISTRIBUTION" "$CONFIG" || exit 1
wait "$(cat "$PID_DIR/server2.pid")"
''', DISTRIBUTION=str(distribution), FAIL_ADMIN="true")
        self.assertNotEqual(result.returncode, 0)
        output = (self.root / "logs/server2.1.log").read_text()
        self.assertIn("IllegalStateException: administrator failed", output)
        self.assertNotIn("InvocationTargetException", output)

    def test_unrecognized_launcher_is_rejected(self):
        launcher = self.root / "unknown-launcher.sh"
        for contents in ("#!/bin/bash\necho unexpected-launcher\n",
                         "org.apache.pinot.tools.admin.PinotAdministrator\n" * 2):
            launcher.write_text(contents)
            result = self.run_shell('prepareCompatibilityServerLauncher "$LAUNCHER"', LAUNCHER=str(launcher))
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("Cannot locate exactly one", result.stderr)
            self.assertEqual(launcher.read_text(), contents)
            self.assertFalse((self.root / "unknown-launcher-compat.sh").exists())


if __name__ == "__main__":
    unittest.main()
