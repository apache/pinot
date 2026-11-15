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
package org.apache.pinot.query.planner.optimizer;

import java.util.List;
import java.util.Set;
import org.apache.pinot.query.planner.spi.QueryOptimizerProvider;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;


public class QueryOptimizerRegistryTest {

  @Test
  public void testRegisterAndLookupByName() {
    QueryOptimizerProvider cbo = provider("cbo");
    QueryOptimizerProvider other = provider("other");

    QueryOptimizerRegistry registry = new QueryOptimizerRegistry(List.of(cbo, other));

    assertSame(registry.get("cbo"), cbo);
    assertSame(registry.get("other"), other);
    assertNull(registry.get("unknown"));
    assertEquals(registry.getNames(), Set.of("cbo", "other"));
  }

  @Test
  public void testDuplicateNameLastWins() {
    QueryOptimizerProvider first = provider("cbo");
    QueryOptimizerProvider second = provider("cbo");

    QueryOptimizerRegistry registry = new QueryOptimizerRegistry(List.of(first, second));

    assertSame(registry.get("cbo"), second);
    assertEquals(registry.getNames(), Set.of("cbo"));
  }

  private static QueryOptimizerProvider provider(String name) {
    QueryOptimizerProvider provider = mock(QueryOptimizerProvider.class);
    when(provider.getName()).thenReturn(name);
    return provider;
  }
}
