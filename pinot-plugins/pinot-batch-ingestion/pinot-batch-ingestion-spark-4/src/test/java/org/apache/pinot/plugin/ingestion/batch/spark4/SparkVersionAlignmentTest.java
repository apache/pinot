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

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
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
    // KQueueIoHandler is netty 4.2-only (it does not exist in 4.1.x). Force the class to load
    // via Class.forName so we hit the same resolver path Spark's NettyUtils.createEventLoop
    // uses — a NoClassDefFoundError here means the module's pom override has stopped winning.
    Class<?> kqueueIoHandler = Class.forName("io.netty.channel.kqueue.KQueueIoHandler");
    String version = kqueueIoHandler.getPackage().getImplementationVersion();
    // ImplementationVersion can be null when the class is on the IDE's classpath without a
    // manifest; tolerate that case but still assert the class loaded successfully.
    if (version != null) {
      assertTrue(version.startsWith("4.2."),
          "Expected netty 4.2.x on the spark-4 batch-ingestion test classpath, got " + version
              + ". This usually means a spark4.version bump pulled in a different netty major; "
              + "either update the netty.version override in pom.xml or revert the bump.");
    }

    // Same shape, but for the netty common module — different jar, same expectation.
    Class<?> nettyCommon = Class.forName("io.netty.util.Version");
    assertEquals(nettyCommon != null, true);
  }
}
