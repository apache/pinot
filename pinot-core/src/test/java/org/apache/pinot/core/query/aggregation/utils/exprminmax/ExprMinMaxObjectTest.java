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
package org.apache.pinot.core.query.aggregation.utils.exprminmax;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.operator.docvalsets.RowBasedBlockValSet;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class ExprMinMaxObjectTest {
  private static final DataSchema MEASURING_SCHEMA =
      new DataSchema(new String[]{"measure"}, new ColumnDataType[]{ColumnDataType.INT});
  private static final DataSchema PROJECTION_SCHEMA =
      new DataSchema(new String[]{"projection"}, new ColumnDataType[]{ColumnDataType.STRING});

  @Test
  public void testTiedSerializedResultsKeepAllRows()
      throws IOException {
    // The broker merges deserialized (immutable) server results. Tied rows from every server must survive, including
    // the first one, and the merged result must still expose its key for the next merge.
    ExprMinMaxObject merged = serialized(5, "a1", "a2")
        .merge(serialized(5, "b1"), true)
        .merge(serialized(5, "c1"), true)
        .merge(serialized(3, "d1"), true);

    assertEquals(merged.getExtremumKey(), new Comparable[]{5});
    assertEquals(projections(merged), List.of("a1", "a2", "b1", "c1"));
  }

  @Test
  public void testTiedSerializedResultSurvivesReserialization()
      throws IOException {
    ExprMinMaxObject merged = serialized(5, "a1").merge(serialized(5, "b1"), false);
    ExprMinMaxObject roundTripped = ExprMinMaxObject.fromBytes(merged.toBytes());

    assertEquals(roundTripped.getExtremumKey(), new Comparable[]{5});
    assertEquals(projections(roundTripped), List.of("a1", "b1"));
  }

  /// Builds a single-server result through the segment accumulation path, then serializes it as a server would.
  private static ExprMinMaxObject serialized(int key, String... projections)
      throws IOException {
    List<Object[]> rows = new ArrayList<>();
    for (String projection : projections) {
      rows.add(new Object[]{key, projection});
    }
    List<ExprMinMaxMeasuringValSetWrapper> measuring =
        List.of(new ExprMinMaxMeasuringValSetWrapper(new RowBasedBlockValSet(ColumnDataType.INT, rows, 0, false)));
    List<ExprMinMaxProjectionValSetWrapper> projection =
        List.of(new ExprMinMaxProjectionValSetWrapper(new RowBasedBlockValSet(ColumnDataType.STRING, rows, 1, false)));
    ExprMinMaxObject object = new ExprMinMaxObject(MEASURING_SCHEMA, PROJECTION_SCHEMA);
    for (int i = 0; i < rows.size(); i++) {
      int result = object.compareAndSetKey(measuring, i, true);
      if (result > 0) {
        object.setToNewVal(projection, i);
      } else if (result == 0) {
        object.addVal(projection, i);
      }
    }
    return ExprMinMaxObject.fromBytes(object.toBytes());
  }

  private static List<Object> projections(ExprMinMaxObject object) {
    List<Object> values = new ArrayList<>();
    for (int i = 0; i < object.getNumberOfRows(); i++) {
      values.add(object.getField(i, 0));
    }
    return values;
  }
}
