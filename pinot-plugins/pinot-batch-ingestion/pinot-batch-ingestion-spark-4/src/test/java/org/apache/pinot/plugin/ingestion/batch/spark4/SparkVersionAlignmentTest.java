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
package org.apache.pinot.plugin.ingestion.batch.spark4;

import java.util.Map;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/**
 * Pins the cross-module invariant that this module's local {@code netty.version} override
 * (see pom.xml) is in the netty 4.2.x line — required by Spark 4.1+, which uses
 * {@code io.netty.channel.kqueue.KQueueIoHandler} / {@code EpollIoHandler} (new APIs
 * introduced in netty 4.2). If a future {@code spark4.version} bump silently re-shadows
 * netty back to 4.1.x or forward to 4.3.x, this test fails at build time on every platform
 * — instead of failing only on macOS (kqueue) / Linux (epoll) at job runtime.
 */
public class SparkVersionAlignmentTest {

  @Test
  public void nettyOnTestClasspathIsInThe42xLine()
      throws ClassNotFoundException {
    // io.netty.util.Version#identify() reads each netty module's own manifest under the same
    // classloader and returns a Map<artifactId, Version>. Using it (instead of
    // Package#getImplementationVersion on a single class) lets us cross-check that *every*
    // netty module on the classpath is 4.2.x — the actual failure mode this test exists to
    // catch is a mixed-version classpath where some modules are 4.1 and some are 4.2.
    Map<String, io.netty.util.Version> versions = io.netty.util.Version.identify();
    assertFalse(versions.isEmpty(),
        "io.netty.util.Version#identify() returned empty — netty modules are not on the "
            + "test classpath at all. The spark4.version override in pom.xml is broken.");
    for (Map.Entry<String, io.netty.util.Version> e : versions.entrySet()) {
      String artifactVersion = e.getValue().artifactVersion();
      assertTrue(artifactVersion.startsWith("4.2."),
          "Expected netty 4.2.x for every netty module on the spark-4 batch-ingestion test "
              + "classpath; module '" + e.getKey() + "' reports " + artifactVersion + ". This "
              + "usually means a spark4.version bump pulled in a different netty major or "
              + "shadowed only some modules; update the netty.version override in pom.xml or "
              + "revert the bump.");
    }

    // KQueueIoHandler is netty 4.2-only (does not exist in 4.1.x). Force the class to load
    // via Class.forName so we hit the same resolver path Spark's NettyUtils.createEventLoop
    // uses on macOS at runtime — a NoClassDefFoundError here means the module's pom override
    // has stopped winning even if Version#identify() reports 4.2.x.
    Class<?> kqueueIoHandler = Class.forName("io.netty.channel.kqueue.KQueueIoHandler");
    assertNotNull(kqueueIoHandler);
  }
}
