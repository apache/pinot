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
package org.apache.pinot.spi.metrics;

import java.util.Map;
import org.apache.pinot.spi.annotations.metrics.PinotMetricsFactory;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/// Verifies the SPI-level back-compat guarantees for the tag-first naming extension.
///
/// <p>A legacy {@link PinotMetricsFactory} implementation that only overrides the 2-arg
/// {@link PinotMetricsFactory#makePinotMetricName(Class, String)} must automatically get the correct 4-arg behaviour
/// via the default method (which ignores {@code baseName} and {@code tags} and delegates to the 2-arg form).
public class PinotMetricNameSpiTest {

  /// A minimal factory that only implements the 2-arg overload — the 4-arg default must delegate to it.
  private static class MinimalFactory extends PinotMetricsFactory.Noop {
    @Override
    public PinotMetricName makePinotMetricName(Class<?> klass, String name) {
      return () -> "legacy:" + name;
    }
  }

  /// The 4-arg default method must delegate to the 2-arg overload (using the flat name, ignoring base/tags)
  /// so legacy backends are byte-identical to before the tag-first extension.
  @Test
  public void fourArgDefaultDelegatesToFlatName() {
    MinimalFactory factory = new MinimalFactory();
    PinotMetricName twoArg = factory.makePinotMetricName(String.class, "flat.name");
    PinotMetricName fourArg = factory.makePinotMetricName(String.class, "flat.name", "base.name",
        Map.of("table", "myTable"));
    assertEquals(fourArg.getMetricName(), twoArg.getMetricName(),
        "4-arg default must use the flat name and ignore baseName/tags");
    assertTrue(fourArg.getTags().isEmpty(),
        "4-arg default on a legacy factory must return empty tags (not the passed-in tags)");
  }

  /// The {@link PinotMetricName#getTags()} default must return an empty map (not null) for any implementation
  /// that does not override it.
  @Test
  public void defaultGetTagsReturnsEmptyMap() {
    PinotMetricName name = () -> "plainName";
    assertTrue(name.getTags().isEmpty(), "getTags() default must return empty map");
  }
}
