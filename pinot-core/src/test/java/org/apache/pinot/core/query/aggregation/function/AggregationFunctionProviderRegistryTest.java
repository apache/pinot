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
package org.apache.pinot.core.query.aggregation.function;

import java.util.List;
import java.util.Map;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.expectThrows;


/// Verifies classpath provider discovery and deterministic rejection of conflicting aggregate registrations.
public class AggregationFunctionProviderRegistryTest {
  @Test
  public void testDeclaredFamiliesAreDiscoverable() {
    for (AggregationFunctionType type : List.of(AggregationFunctionType.MODE, AggregationFunctionType.FIRSTWITHTIME,
        AggregationFunctionType.LASTWITHTIME, AggregationFunctionType.ANYVALUE, AggregationFunctionType.ARRAYAGG,
        AggregationFunctionType.PINOTPARENTAGGEXPRMIN, AggregationFunctionType.PINOTPARENTAGGEXPRMAX,
        AggregationFunctionType.PINOTCHILDAGGEXPRMIN, AggregationFunctionType.PINOTCHILDAGGEXPRMAX)) {
      AggregationFunctionProvider provider = AggregationFunctionProviderRegistry.getProvider(type);
      assertNotNull(provider, type.name());
      assertEquals(provider.getType(), type);
    }
    assertNull(AggregationFunctionProviderRegistry.getProvider(AggregationFunctionType.COUNT));
  }

  @Test
  public void testDuplicateProvidersFailAndRegistryIsImmutable() {
    AggregationFunctionProvider provider = new ModeAggregationFunction.Provider();
    Map<AggregationFunctionType, AggregationFunctionProvider> registry =
        AggregationFunctionProviderRegistry.loadProviders(List.of(provider));
    assertSame(registry.get(AggregationFunctionType.MODE), provider);
    expectThrows(UnsupportedOperationException.class, registry::clear);
    expectThrows(IllegalStateException.class, () -> AggregationFunctionProviderRegistry.loadProviders(
        List.of(provider, new ModeAggregationFunction.Provider())));
  }
}
