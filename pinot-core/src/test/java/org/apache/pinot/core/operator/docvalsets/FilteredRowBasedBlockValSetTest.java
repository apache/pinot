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
package org.apache.pinot.core.operator.docvalsets;

import java.util.List;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.spi.utils.CommonConstants.NullValuePlaceHolder;
import org.roaringbitmap.RoaringBitmap;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;


/// Verifies string projections preserve filtered row order and null positions without shared mutable fixtures.
public class FilteredRowBasedBlockValSetTest {
  @Test
  public void testEmptyStringProjectionHasNoValuesOrNulls() {
    FilteredRowBasedBlockValSet values = new FilteredRowBasedBlockValSet(ColumnDataType.STRING,
        List.of(), 0, 0, new RoaringBitmap(), true);
    assertEquals(values.getStringValuesSV(), new String[0]);
    assertNull(values.getNullBitmap());
  }

  @Test
  public void testStringProjectionUsesSelectedRowNullPositions() {
    List<Object[]> rows = List.of(new Object[] {"excluded"}, new Object[] {null}, new Object[] {"included"});
    FilteredRowBasedBlockValSet values = new FilteredRowBasedBlockValSet(ColumnDataType.STRING,
        rows, 0, 2, RoaringBitmap.bitmapOf(1, 2), true);
    assertEquals(values.getStringValuesSV(), new String[] {NullValuePlaceHolder.STRING, "included"});
    assertEquals(values.getNullBitmap(), RoaringBitmap.bitmapOf(0));
  }
}
