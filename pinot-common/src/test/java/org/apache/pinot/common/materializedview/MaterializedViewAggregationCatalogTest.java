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
package org.apache.pinot.common.materializedview;

import java.util.Map;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


/// L1 — Static regression on the MV aggregation catalog.
///
/// These tests pin the supported-set and the type mapping so a future refactor cannot
/// silently drop or remap an entry without a test failure. In particular:
///
///   - the {@code DISTINCTCOUNTRAW*} entries are pinned to {@link DataType#BYTES} even
///     though the v1 aggregation functions return {@code STRING} from
///     {@code getFinalResultColumnType()} — this divergence is intentional (see catalog
///     class javadoc) and load-bearing for MV ingestion / rewrite correctness.
public class MaterializedViewAggregationCatalogTest {

  @Test
  public void testSupportedAggregationsExactSet() {
    // Snapshot the exact contents.  A future refactor that adds or removes an entry must
    // update this assertion deliberately, which forces a code-review conversation about
    // whether the rewrite engine and the schema inferer have been updated in lockstep.
    Map<String, DataType> expected = Map.of(
        "SUM", DataType.DOUBLE,
        "MIN", DataType.DOUBLE,
        "MAX", DataType.DOUBLE,
        "COUNT", DataType.LONG,
        "DISTINCTCOUNTRAWHLL", DataType.BYTES,
        "DISTINCTCOUNTRAWHLLPLUS", DataType.BYTES,
        "DISTINCTCOUNTRAWTHETASKETCH", DataType.BYTES);
    assertEquals(MaterializedViewAggregationCatalog.SUPPORTED_AGGREGATIONS, expected);
  }

  @Test
  public void testSupportedOperatorsAllUpperCase() {
    for (String op : MaterializedViewAggregationCatalog.getSupportedOperators()) {
      assertEquals(op, op.toUpperCase(java.util.Locale.ROOT),
          "Catalog operator '" + op + "' must be canonical (uppercase); the catalog's lookup "
              + "methods normalise the caller's input to upper-case so the keys MUST be stored "
              + "upper-case to match.");
    }
  }

  @Test
  public void testGetInferredDataTypeNumericAggregations() {
    assertEquals(MaterializedViewAggregationCatalog.getInferredDataType("sum"), DataType.DOUBLE);
    assertEquals(MaterializedViewAggregationCatalog.getInferredDataType("SUM"), DataType.DOUBLE);
    assertEquals(MaterializedViewAggregationCatalog.getInferredDataType("Min"), DataType.DOUBLE);
    assertEquals(MaterializedViewAggregationCatalog.getInferredDataType("max"), DataType.DOUBLE);
  }

  @Test
  public void testGetInferredDataTypeCount() {
    assertEquals(MaterializedViewAggregationCatalog.getInferredDataType("count"), DataType.LONG);
  }

  @Test
  public void testGetInferredDataTypeSketches() {
    // CRITICAL: see catalog class javadoc.  These MUST be BYTES even though the v1 engine's
    // getFinalResultColumnType() reports STRING — the MV's stored column is the serialized
    // sketch, not the hex-encoded scalar response.  Storing STRING would silently route the
    // rewrite through the "hash the literal" branch of DistinctCountHLLAggregationFunction
    // and produce wrong answers.
    assertEquals(MaterializedViewAggregationCatalog.getInferredDataType("DISTINCTCOUNTRAWHLL"),
        DataType.BYTES);
    assertEquals(MaterializedViewAggregationCatalog.getInferredDataType("DISTINCTCOUNTRAWHLLPLUS"),
        DataType.BYTES);
    assertEquals(MaterializedViewAggregationCatalog.getInferredDataType("DISTINCTCOUNTRAWTHETASKETCH"),
        DataType.BYTES);
  }

  @Test
  public void testIsSupportedCaseInsensitive() {
    assertTrue(MaterializedViewAggregationCatalog.isSupported("sum"));
    assertTrue(MaterializedViewAggregationCatalog.isSupported("SUM"));
    assertTrue(MaterializedViewAggregationCatalog.isSupported("Sum"));
  }

  @Test
  public void testIsSupportedRejectsUnknownAggregation() {
    // Pinot aggregations the MV rewrite engine cannot re-aggregate (PR 2).  A future PR may
    // add re-aggregation support for some of these — when it does, it MUST update both the
    // catalog and this test in the same commit.
    assertFalse(MaterializedViewAggregationCatalog.isSupported("DISTINCTCOUNTHLL"));
    assertFalse(MaterializedViewAggregationCatalog.isSupported("AVG"));
    assertFalse(MaterializedViewAggregationCatalog.isSupported("PERCENTILE"));
    assertFalse(MaterializedViewAggregationCatalog.isSupported("HISTOGRAM"));
  }

  @Test
  public void testIsSupportedNullOperator() {
    assertFalse(MaterializedViewAggregationCatalog.isSupported(null));
  }

  @Test
  public void testGetInferredDataTypeNullOperator() {
    assertNull(MaterializedViewAggregationCatalog.getInferredDataType(null));
  }

  @Test
  public void testGetInferredDataTypeUnknownOperator() {
    assertNull(MaterializedViewAggregationCatalog.getInferredDataType("avg"));
  }
}
