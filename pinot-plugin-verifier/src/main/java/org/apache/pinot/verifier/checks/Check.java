/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.verifier.checks;

import org.apache.pinot.verifier.PluginVerifier.CheckContext;


/**
 * One verifier per plugin SPI. Implementations exercise the SAME factory API a real broker /
 * server / minion uses to instantiate that kind of plugin — so a green run means "this
 * plugin would actually load in production", not "the class file is on the classpath".
 */
public interface Check {

  /** Display name printed at the top of the section. */
  String name();

  /**
   * Run the check and return a per-plugin tally. Implementations should not throw — failures
   * belong inside the {@link Outcome}.
   */
  Outcome run(CheckContext context);

  /** Per-section result. {@code passed + failed} equals the number of plugin classes attempted. */
  record Outcome(int passed, int failed) {
    public static Outcome empty() {
      return new Outcome(0, 0);
    }
  }
}
