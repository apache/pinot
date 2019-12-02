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
package org.apache.pinot.query.pruner;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import junit.framework.Assert;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.utils.request.FilterQueryTree;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.core.query.pruner.ColumnValueSegmentPruner;
import org.apache.pinot.core.segment.index.ColumnMetadata;
import org.apache.pinot.core.segment.index.readers.BloomFilterReader;
import org.apache.pinot.pql.parsers.Pql2Compiler;
import org.testng.annotations.Test;


/**
 * Unit tests for {@link ColumnValueSegmentPruner} class.
 */
public class ColumnValueSegmentPrunerTest {
  private static final Pql2Compiler COMPILER = new Pql2Compiler();
  private static final Map<String, ColumnMetadata> COLUMN_METADATA_MAP = new HashMap<>();
  private static final Map<String, BloomFilterReader> BLOOM_FILTER_MAP = new HashMap<>();

  static {
    COLUMN_METADATA_MAP.put("time",
        new ColumnMetadata.Builder().setColumnName("time").setFieldType(FieldSpec.FieldType.TIME)
            .setDataType(FieldSpec.DataType.INT).setTimeUnit(TimeUnit.DAYS).setMinValue(10).setMaxValue(20).build());
    COLUMN_METADATA_MAP.put("foo",
        new ColumnMetadata.Builder().setColumnName("foo").setFieldType(FieldSpec.FieldType.DIMENSION)
            .setDataType(FieldSpec.DataType.STRING).build());
  }

  @Test
  public void test() {
    // Query without time predicate
    Assert.assertFalse(runPruner("SELECT COUNT(*) FROM table WHERE foo = 'bar'"));

    // Equality predicate
    Assert.assertTrue(runPruner("SELECT COUNT(*) FROM table WHERE time = 0"));
    Assert.assertFalse(runPruner("SELECT COUNT(*) FROM table WHERE time = 10"));
    Assert.assertFalse(runPruner("SELECT COUNT(*) FROM table WHERE time = 20"));
    Assert.assertTrue(runPruner("SELECT COUNT(*) FROM table WHERE time = 30"));

    // Range predicate
    Assert.assertTrue(runPruner("SELECT COUNT(*) FROM table WHERE time < 10"));
    Assert.assertFalse(runPruner("SELECT COUNT(*) FROM table WHERE time <= 10"));
    Assert.assertFalse(runPruner("SELECT COUNT(*) FROM table WHERE time >= 10"));
    Assert.assertTrue(runPruner("SELECT COUNT(*) FROM table WHERE time > 20"));
    Assert.assertFalse(runPruner("SELECT COUNT(*) FROM table WHERE time BETWEEN 20 AND 30"));
    Assert.assertTrue(runPruner("SELECT COUNT(*) FROM table WHERE time BETWEEN 30 AND 40"));

    // Invalid range predicate
    Assert.assertTrue(runPruner("SELECT COUNT(*) FROM table WHERE time BETWEEN 20 AND 10"));
    Assert.assertTrue(runPruner("SELECT COUNT(*) FROM table WHERE time BETWEEN 30 AND 20"));
    Assert.assertFalse(runPruner("SELECT COUNT(*) FROM table WHERE time BETWEEN 10 AND 10"));
    Assert.assertFalse(runPruner("SELECT COUNT(*) FROM table WHERE time BETWEEN 20 AND 20"));

    // AND operator
    Assert.assertTrue(runPruner("SELECT COUNT(*) FROM table WHERE time = 0 AND time > 10"));
    Assert.assertTrue(runPruner("SELECT COUNT(*) FROM table WHERE time > 0 AND time < 10"));
    Assert.assertFalse(runPruner("SELECT COUNT(*) FROM table WHERE time >= 0 AND time <= 10"));
    Assert.assertTrue(runPruner("SELECT COUNT(*) FROM table WHERE time > 20 AND time < 10"));
    Assert.assertFalse(runPruner("SELECT COUNT(*) FROM table WHERE time >= 20 AND time < 30"));
    Assert.assertFalse(runPruner("SELECT COUNT(*) FROM table WHERE time > 0 AND time BETWEEN 0 AND 10"));

    // OR operator
    Assert.assertFalse(runPruner("SELECT COUNT(*) FROM table WHERE time = 0 OR time > 10"));
    Assert.assertTrue(runPruner("SELECT COUNT(*) FROM table WHERE time = 0 OR time < 10"));
    Assert.assertFalse(runPruner("SELECT COUNT(*) FROM table WHERE time >= 0 OR time <= 10"));
    Assert.assertTrue(runPruner("SELECT COUNT(*) FROM table WHERE time > 30 OR time < 10"));
    Assert.assertTrue(runPruner("SELECT COUNT(*) FROM table WHERE time BETWEEN 0 AND 5 OR time BETWEEN 30 AND 35"));
  }

  private boolean runPruner(String query) {
    BrokerRequest brokerRequest = COMPILER.compileToBrokerRequest(query);
    FilterQueryTree filterQueryTree = RequestUtils.generateFilterQueryTree(brokerRequest);
    ColumnValueSegmentPruner pruner = new ColumnValueSegmentPruner();
    return pruner.pruneSegment(filterQueryTree, COLUMN_METADATA_MAP, BLOOM_FILTER_MAP);
  }
}
