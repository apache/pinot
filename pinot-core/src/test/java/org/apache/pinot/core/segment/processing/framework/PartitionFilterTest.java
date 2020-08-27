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
package org.apache.pinot.core.segment.processing.framework;

import org.apache.pinot.core.segment.processing.partitioner.FunctionEvaluatorPartitionFilter;
import org.apache.pinot.core.segment.processing.partitioner.NoOpPartitionFilter;
import org.apache.pinot.core.segment.processing.partitioner.PartitionFilter;
import org.apache.pinot.core.segment.processing.partitioner.PartitionerFactory;
import org.apache.pinot.core.segment.processing.partitioner.PartitioningConfig;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


/**
 * Tests for {@link PartitionFilter}
 */
public class PartitionFilterTest {

  @Test
  public void getPartitionFilterTest() {
    PartitioningConfig partitioningConfig = new PartitioningConfig.Builder().build();
    PartitionFilter partitionFilter = PartitionerFactory.getPartitionFilter(partitioningConfig);
    assertEquals(partitionFilter.getClass(), NoOpPartitionFilter.class);

    partitioningConfig = new PartitioningConfig.Builder().setFilterFunction("bad function").build();
    try {
      PartitionerFactory.getPartitionFilter(partitioningConfig);
      fail("Should not pass for invalid filter function");
    } catch (IllegalArgumentException e) {
      // expected
    }
    partitioningConfig = new PartitioningConfig.Builder().setFilterFunction("Groovy({arg0 == 3},arg0)").build();
    partitionFilter = PartitionerFactory.getPartitionFilter(partitioningConfig);
    assertEquals(partitionFilter.getClass(), FunctionEvaluatorPartitionFilter.class);
  }

  @Test
  public void testPartitionFilter() {
    PartitioningConfig partitioningConfig = new PartitioningConfig.Builder()
        .setFilterFunction("Groovy({Integer.valueOf(arg0) < 10 || Integer.valueOf(arg0) > 20},arg0)").build();
    PartitionFilter partitionFilter = PartitionerFactory.getPartitionFilter(partitioningConfig);

    assertTrue(partitionFilter.filter("5"));
    assertTrue(partitionFilter.filter("25"));
    assertFalse(partitionFilter.filter("10"));
    assertFalse(partitionFilter.filter("15"));
  }
}
