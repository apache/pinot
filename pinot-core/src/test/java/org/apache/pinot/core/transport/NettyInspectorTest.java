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
package org.apache.pinot.core.transport;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


public class NettyInspectorTest {

  @Test
  public void checkUnsafePassesWhenNettyHasUnsafe() {
    NettyInspector.CheckResult result = NettyInspector.checkUnsafe(new FakeNettyInstance(null));

    assertEquals(result.getStatus(), NettyInspector.CheckResult.Status.PASS);
    assertNull(result.getMessage());
  }

  @Test
  public void checkUnsafeFailsWhenNettyHasNoUnsafe() {
    Throwable cause = new UnsupportedOperationException("io.netty.noUnsafe=true by default on Java 25+");

    NettyInspector.CheckResult result = NettyInspector.checkUnsafe(new FakeNettyInstance(cause));

    assertEquals(result.getStatus(), NettyInspector.CheckResult.Status.FAIL);
    String message = result.getMessage();
    assertNotNull(message);
    assertTrue(message.contains("fake"), message);
    assertTrue(message.contains("io.netty.noUnsafe=true by default on Java 25+"), message);
    assertTrue(message.contains("--sun-misc-unsafe-memory-access=allow"), message);
  }

  @Test
  public void defaultChecksIncludeUnsafe() {
    NettyInstance instance = new FakeNettyInstance(new UnsupportedOperationException("no unsafe"));

    boolean failed = NettyInspector.CHECKS.stream()
        .map(check -> check.apply(instance))
        .anyMatch(result -> result.getStatus() == NettyInspector.CheckResult.Status.FAIL);

    assertTrue(failed);
  }

  /// A [NettyInstance] whose Unsafe availability is fixed by the test. Reflection access is always available.
  private static class FakeNettyInstance extends NettyInstance {
    private final Throwable _unsafeUnavailabilityCause;

    FakeNettyInstance(Throwable unsafeUnavailabilityCause) {
      _unsafeUnavailabilityCause = unsafeUnavailabilityCause;
    }

    @Override
    public String getName() {
      return "fake";
    }

    @Override
    public String getShadePrefix() {
      return "";
    }

    @Override
    public boolean isExplicitTryReflectionSetAccessible() {
      return true;
    }

    @Override
    public boolean hasUnsafe() {
      return _unsafeUnavailabilityCause == null;
    }

    @Override
    public Throwable getUnsafeUnavailabilityCause() {
      return _unsafeUnavailabilityCause;
    }

    @Override
    public long getUsedDirectMemory() {
      return 0;
    }

    @Override
    public long getMaxDirectMemory() {
      return 0;
    }
  }
}
