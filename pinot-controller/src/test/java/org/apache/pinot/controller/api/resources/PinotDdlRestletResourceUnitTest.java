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
package org.apache.pinot.controller.api.resources;

import java.util.Arrays;
import java.util.Collections;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.MetricFieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.testng.annotations.Test;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


/**
 * Unit tests for {@link PinotDdlRestletResource#describeColumnShapeMismatch}. This is the
 * hybrid-table CREATE gate that decides whether a DDL-compiled schema is compatible with the
 * schema already stored in ZK for a hybrid pair's sibling variant. The comparator must accept
 * differences in schema-level metadata a DDL column list cannot express (primary keys, tags,
 * null-handling) and reject differences in per-column attributes that the DDL does control.
 */
public class PinotDdlRestletResourceUnitTest {

  /** Compiled DDL with matching columns but no primary keys must accept a stored schema that has them. */
  @Test
  public void acceptsMatchingColumnsWhenStoredHasExtraPrimaryKeyMetadata() {
    Schema stored = new Schema();
    stored.setSchemaName("t");
    stored.addField(new DimensionFieldSpec("id", DataType.INT, true));
    stored.setPrimaryKeyColumns(Collections.singletonList("id"));
    stored.setEnableColumnBasedNullHandling(true);

    Schema compiled = new Schema();
    compiled.setSchemaName("t");
    compiled.addField(new DimensionFieldSpec("id", DataType.INT, true));

    assertNull(PinotDdlRestletResource.describeColumnShapeMismatch(stored, compiled),
        "schema-level metadata the DDL column list cannot express must not drive a mismatch");
  }

  /** A missing or extra column must be rejected with a named column set in the message. */
  @Test
  public void rejectsColumnSetDifference() {
    Schema stored = new Schema();
    stored.setSchemaName("t");
    stored.addField(new DimensionFieldSpec("id", DataType.INT, true));
    stored.addField(new DimensionFieldSpec("name", DataType.STRING, true));

    Schema compiled = new Schema();
    compiled.setSchemaName("t");
    compiled.addField(new DimensionFieldSpec("id", DataType.INT, true));

    String msg = PinotDdlRestletResource.describeColumnShapeMismatch(stored, compiled);
    assertNotNull(msg);
    assertTrue(msg.contains("column sets differ") && msg.contains("name"),
        "message should call out the offending column set difference: " + msg);
  }

  /** Different data type for the same column must be rejected. */
  @Test
  public void rejectsDataTypeMismatch() {
    Schema stored = new Schema();
    stored.setSchemaName("t");
    stored.addField(new DimensionFieldSpec("id", DataType.INT, true));

    Schema compiled = new Schema();
    compiled.setSchemaName("t");
    compiled.addField(new DimensionFieldSpec("id", DataType.LONG, true));

    String msg = PinotDdlRestletResource.describeColumnShapeMismatch(stored, compiled);
    assertNotNull(msg);
    assertTrue(msg.contains("data type differs"), msg);
  }

  /** DIMENSION vs METRIC for the same column must be rejected. */
  @Test
  public void rejectsFieldTypeMismatch() {
    Schema stored = new Schema();
    stored.setSchemaName("t");
    stored.addField(new DimensionFieldSpec("v", DataType.LONG, true));

    Schema compiled = new Schema();
    compiled.setSchemaName("t");
    compiled.addField(new MetricFieldSpec("v", DataType.LONG));

    String msg = PinotDdlRestletResource.describeColumnShapeMismatch(stored, compiled);
    assertNotNull(msg);
    assertTrue(msg.contains("field type differs"), msg);
  }

  /** Single-value vs multi-value mismatch must be rejected. */
  @Test
  public void rejectsSingleValuedFlagMismatch() {
    Schema stored = new Schema();
    stored.setSchemaName("t");
    stored.addField(new DimensionFieldSpec("tags", DataType.STRING, true));

    Schema compiled = new Schema();
    compiled.setSchemaName("t");
    compiled.addField(new DimensionFieldSpec("tags", DataType.STRING, false));

    String msg = PinotDdlRestletResource.describeColumnShapeMismatch(stored, compiled);
    assertNotNull(msg);
    assertTrue(msg.contains("single-valued flag"), msg);
  }

  /** NOT NULL flag mismatch must be rejected. */
  @Test
  public void rejectsNotNullFlagMismatch() {
    Schema stored = new Schema();
    stored.setSchemaName("t");
    FieldSpec storedSpec = new DimensionFieldSpec("id", DataType.INT, true);
    storedSpec.setNotNull(true);
    stored.addField(storedSpec);

    Schema compiled = new Schema();
    compiled.setSchemaName("t");
    compiled.addField(new DimensionFieldSpec("id", DataType.INT, true));

    String msg = PinotDdlRestletResource.describeColumnShapeMismatch(stored, compiled);
    assertNotNull(msg);
    assertTrue(msg.contains("NOT NULL flag"), msg);
  }

  /** Default-null-value mismatch must be rejected. */
  @Test
  public void rejectsDefaultNullValueMismatch() {
    Schema stored = new Schema();
    stored.setSchemaName("t");
    FieldSpec storedSpec = new DimensionFieldSpec("id", DataType.INT, true);
    storedSpec.setDefaultNullValue(-1);
    stored.addField(storedSpec);

    Schema compiled = new Schema();
    compiled.setSchemaName("t");
    FieldSpec compiledSpec = new DimensionFieldSpec("id", DataType.INT, true);
    compiledSpec.setDefaultNullValue(0);
    compiled.addField(compiledSpec);

    String msg = PinotDdlRestletResource.describeColumnShapeMismatch(stored, compiled);
    assertNotNull(msg);
    assertTrue(msg.contains("default null value"), msg);
  }

  /** DATETIME format mismatch must be rejected. */
  @Test
  public void rejectsDateTimeFormatMismatch() {
    Schema stored = new Schema();
    stored.setSchemaName("t");
    stored.addField(new DateTimeFieldSpec("ts", DataType.LONG,
        "1:MILLISECONDS:EPOCH", "1:MILLISECONDS"));

    Schema compiled = new Schema();
    compiled.setSchemaName("t");
    compiled.addField(new DateTimeFieldSpec("ts", DataType.LONG,
        "1:SECONDS:EPOCH", "1:MILLISECONDS"));

    String msg = PinotDdlRestletResource.describeColumnShapeMismatch(stored, compiled);
    assertNotNull(msg);
    assertTrue(msg.contains("DATETIME format"), msg);
  }

  /** DATETIME granularity mismatch must be rejected. */
  @Test
  public void rejectsDateTimeGranularityMismatch() {
    Schema stored = new Schema();
    stored.setSchemaName("t");
    stored.addField(new DateTimeFieldSpec("ts", DataType.LONG,
        "1:MILLISECONDS:EPOCH", "1:MILLISECONDS"));

    Schema compiled = new Schema();
    compiled.setSchemaName("t");
    compiled.addField(new DateTimeFieldSpec("ts", DataType.LONG,
        "1:MILLISECONDS:EPOCH", "1:SECONDS"));

    String msg = PinotDdlRestletResource.describeColumnShapeMismatch(stored, compiled);
    assertNotNull(msg);
    assertTrue(msg.contains("DATETIME granularity"), msg);
  }

  /** Matching multi-column, multi-type schemas must be accepted. */
  @Test
  public void acceptsMatchingMixedColumnSchema() {
    Schema stored = new Schema();
    stored.setSchemaName("t");
    stored.addField(new DimensionFieldSpec("id", DataType.INT, true));
    stored.addField(new MetricFieldSpec("value", DataType.DOUBLE));
    stored.addField(new DateTimeFieldSpec("ts", DataType.LONG,
        "1:MILLISECONDS:EPOCH", "1:MILLISECONDS"));
    stored.setPrimaryKeyColumns(Arrays.asList("id"));

    Schema compiled = new Schema();
    compiled.setSchemaName("t");
    compiled.addField(new DimensionFieldSpec("id", DataType.INT, true));
    compiled.addField(new MetricFieldSpec("value", DataType.DOUBLE));
    compiled.addField(new DateTimeFieldSpec("ts", DataType.LONG,
        "1:MILLISECONDS:EPOCH", "1:MILLISECONDS"));

    assertNull(PinotDdlRestletResource.describeColumnShapeMismatch(stored, compiled));
  }
}
