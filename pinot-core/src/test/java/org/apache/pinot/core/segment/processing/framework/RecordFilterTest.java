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

import org.apache.pinot.core.segment.processing.filter.FunctionEvaluatorRecordFilter;
import org.apache.pinot.core.segment.processing.filter.NoOpRecordFilter;
import org.apache.pinot.core.segment.processing.filter.RecordFilter;
import org.apache.pinot.core.segment.processing.filter.RecordFilterConfig;
import org.apache.pinot.core.segment.processing.filter.RecordFilterFactory;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;


/**
 * Tests for {@link RecordFilter}
 */
public class RecordFilterTest {

  @Test
  public void getPartitionFilterTest() {
    RecordFilterConfig recordFilterConfig = new RecordFilterConfig.Builder().build();
    RecordFilter recordFilter = RecordFilterFactory.getRecordFilter(recordFilterConfig);
    assertEquals(recordFilter.getClass(), NoOpRecordFilter.class);

    recordFilterConfig =
        new RecordFilterConfig.Builder().setRecordFilterType(RecordFilterFactory.RecordFilterType.FILTER_FUNCTION)
            .setFilterFunction("badFunction()").build();
    try {
      RecordFilterFactory.getRecordFilter(recordFilterConfig);
      fail("Should not pass for invalid filter function");
    } catch (IllegalStateException e) {
      // expected
    }
    recordFilterConfig =
        new RecordFilterConfig.Builder().setRecordFilterType(RecordFilterFactory.RecordFilterType.FILTER_FUNCTION)
            .setFilterFunction("Groovy({colA == 3},colA)").build();
    recordFilter = RecordFilterFactory.getRecordFilter(recordFilterConfig);
    assertEquals(recordFilter.getClass(), FunctionEvaluatorRecordFilter.class);
  }

  @Test
  public void testPartitionFilter() {
    RecordFilterConfig filterConfig =
        new RecordFilterConfig.Builder().setRecordFilterType(RecordFilterFactory.RecordFilterType.FILTER_FUNCTION)
            .setFilterFunction("Groovy({Integer.valueOf(colA) < 10 || Integer.valueOf(colB) > 20},colA, colB)").build();
    RecordFilter recordFilter = RecordFilterFactory.getRecordFilter(filterConfig);

    GenericRow row = new GenericRow();
    row.putValue("colA", "5");
    row.putValue("colB", "5");
    assertTrue(recordFilter.filter(row));
    row.putValue("colA", "15");
    row.putValue("colB", "30");
    assertTrue(recordFilter.filter(row));
    row.putValue("colA", 5);
    row.putValue("colB", 15);
    assertTrue(recordFilter.filter(row));
    row.putValue("colA", "10");
    row.putValue("colB", "20");
    assertFalse(recordFilter.filter(row));
  }
}
