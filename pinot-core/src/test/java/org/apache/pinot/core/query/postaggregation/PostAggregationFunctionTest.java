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
package org.apache.pinot.core.query.postaggregation;

import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.segment.local.utils.GeometrySerializer;
import org.apache.pinot.segment.local.utils.GeometryUtils;
import org.locationtech.jts.geom.Coordinate;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.assertNull;


public class PostAggregationFunctionTest {

  @Test
  public void testPostAggregationFunction() {
    // Plus
    PostAggregationFunction function =
        new PostAggregationFunction("plus", new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.LONG});
    assertEquals(function.getResultType(), ColumnDataType.DOUBLE);
    assertEquals(function.invoke(new Object[]{1, 2L}), 3.0);

    // Minus
    function = new PostAggregationFunction("MINUS", new ColumnDataType[]{ColumnDataType.FLOAT, ColumnDataType.DOUBLE});
    assertEquals(function.getResultType(), ColumnDataType.DOUBLE);
    assertEquals(function.invoke(new Object[]{3f, 4.0}), -1.0);

    // Times
    function = new PostAggregationFunction("tImEs", new ColumnDataType[]{ColumnDataType.STRING, ColumnDataType.INT});
    assertEquals(function.getResultType(), ColumnDataType.DOUBLE);
    assertEquals(function.invoke(new Object[]{"5", 6}), 30.0);

    // Reverse
    function = new PostAggregationFunction("reverse", new ColumnDataType[]{ColumnDataType.LONG});
    assertEquals(function.getResultType(), ColumnDataType.STRING);
    assertEquals(function.invoke(new Object[]{"1234567890"}), "0987654321");

    // ST_AsText
    function = new PostAggregationFunction("ST_As_Text", new ColumnDataType[]{ColumnDataType.BYTES});
    assertEquals(function.getResultType(), ColumnDataType.STRING);
    assertEquals(function.invoke(
        new Object[]{GeometrySerializer.serialize(GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(10, 20)))}),
        "POINT (10 20)");

    // Cast
    function = new PostAggregationFunction("cast", new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.STRING});
    assertEquals(function.getResultType(), ColumnDataType.OBJECT);
    assertEquals(function.invoke(new Object[]{1, "LONG"}), 1L);

    // NOT
    function = new PostAggregationFunction("not", new ColumnDataType[]{ColumnDataType.BOOLEAN});
    assertEquals(function.getResultType(), ColumnDataType.BOOLEAN);
    assertFalse((Boolean) function.invoke(new Object[]{true}));
    assertTrue((Boolean) function.invoke(new Object[]{false}));

    // isDistinctFrom
    function = new PostAggregationFunction("isDistinctFrom",
        new ColumnDataType[]{ColumnDataType.STRING, ColumnDataType.STRING});
    assertEquals(function.getResultType(), ColumnDataType.BOOLEAN);
    assertFalse((Boolean) function.invoke(new Object[]{null, null}));
    assertFalse((Boolean) function.invoke(new Object[]{"a", "a"}));
    assertTrue((Boolean) function.invoke(new Object[]{null, "a"}));
    assertTrue((Boolean) function.invoke(new Object[]{"a", null}));
    assertTrue((Boolean) function.invoke(new Object[]{"a", "b"}));

    // isNotDistinctFrom
    function = new PostAggregationFunction("isNotDistinctFrom",
        new ColumnDataType[]{ColumnDataType.STRING, ColumnDataType.STRING});
    assertEquals(function.getResultType(), ColumnDataType.BOOLEAN);
    assertTrue((Boolean) function.invoke(new Object[]{null, null}));
    assertTrue((Boolean) function.invoke(new Object[]{"a", "a"}));
    assertFalse((Boolean) function.invoke(new Object[]{null, "a"}));
    assertFalse((Boolean) function.invoke(new Object[]{"a", null}));
    assertFalse((Boolean) function.invoke(new Object[]{"a", "b"}));

    // Coalesce
    function = new PostAggregationFunction("coalesce", new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.STRING,
    ColumnDataType.BOOLEAN});
    assertEquals(function.getResultType(), ColumnDataType.OBJECT);
    assertNull(function.invoke(new Object[]{null, null, null}));
    assertEquals(function.invoke(new Object[]{null, "1", null}), "1");
    assertEquals(function.invoke(new Object[]{1, "2", false}), 1);
    assertEquals(function.invoke(new Object[]{null, null, true}), true);
  }
}
