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
package org.apache.pinot.spi.query;

import java.util.ArrayList;
import java.util.List;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


public class QueryThreadContextTest {

  @Test
  public void testCloseReleasesRegisteredResources() {
    List<String> closed = new ArrayList<>();
    try (QueryThreadContext ignore = QueryThreadContext.openForSseTest()) {
      QueryThreadContext.registerResource(() -> closed.add("a"));
      QueryThreadContext.registerResource(() -> closed.add("b"));
      assertTrue(closed.isEmpty());
    }
    assertEquals(closed.size(), 2);
    assertTrue(closed.contains("a") && closed.contains("b"));
  }

  @Test
  public void testCloseSkipsUnregisteredResources() {
    List<String> closed = new ArrayList<>();
    try (QueryThreadContext ignore = QueryThreadContext.openForSseTest()) {
      AutoCloseable released = () -> closed.add("released");
      QueryThreadContext.registerResource(released);
      QueryThreadContext.registerResource(() -> closed.add("leaked"));
      // The owner closes the resource itself on the normal path
      QueryThreadContext.unregisterResource(released);
    }
    assertEquals(closed, List.of("leaked"));
  }

  @Test
  public void testCloseContinuesAfterResourceFailure() {
    List<String> closed = new ArrayList<>();
    try (QueryThreadContext ignore = QueryThreadContext.openForSseTest()) {
      QueryThreadContext.registerResource(() -> {
        throw new IllegalStateException("boom");
      });
      QueryThreadContext.registerResource(() -> closed.add("other"));
    }
    assertEquals(closed, List.of("other"));
    // The thread context is removed even when a resource fails to close
    assertNull(QueryThreadContext.getIfAvailable());
  }

  @Test
  public void testCloseCleansUpThreadAfterResourceError() {
    List<String> closed = new ArrayList<>();
    QueryThreadContext threadContext = QueryThreadContext.openForSseTest();
    QueryThreadContext.registerResource(() -> {
      throw new AssertionError("boom");
    });
    QueryThreadContext.registerResource(() -> closed.add("other"));
    AssertionError error = expectThrows(AssertionError.class, threadContext::close);
    assertEquals(error.getMessage(), "boom");
    // The other resources are still closed, and the thread does not keep the context of the finished task
    assertEquals(closed, List.of("other"));
    assertNull(QueryThreadContext.getIfAvailable());
  }

  @Test
  public void testResourceUnregisteringItselfOnClose() {
    List<String> closed = new ArrayList<>();
    try (QueryThreadContext ignore = QueryThreadContext.openForSseTest()) {
      AutoCloseable[] holder = new AutoCloseable[1];
      holder[0] = () -> {
        QueryThreadContext.unregisterResource(holder[0]);
        closed.add("self");
      };
      QueryThreadContext.registerResource(holder[0]);
    }
    assertEquals(closed, List.of("self"));
  }

  @Test
  public void testRegisterWithoutThreadContextOrNullResource() {
    List<String> closed = new ArrayList<>();
    // No QueryThreadContext: nothing tracks the resource, and nothing fails
    QueryThreadContext.registerResource(() -> closed.add("untracked"));
    QueryThreadContext.unregisterResource(() -> closed.add("untracked"));
    try (QueryThreadContext ignore = QueryThreadContext.openForSseTest()) {
      QueryThreadContext.registerResource(null);
      QueryThreadContext.unregisterResource(null);
    }
    assertTrue(closed.isEmpty());
  }

  @Test
  public void testResourcesDoNotLeakIntoNextContext() {
    List<String> closed = new ArrayList<>();
    try (QueryThreadContext ignore = QueryThreadContext.openForSseTest()) {
      QueryThreadContext.registerResource(() -> closed.add("first"));
    }
    try (QueryThreadContext ignore = QueryThreadContext.openForSseTest()) {
      QueryThreadContext.registerResource(() -> closed.add("second"));
    }
    assertEquals(closed, List.of("first", "second"));
  }
}
