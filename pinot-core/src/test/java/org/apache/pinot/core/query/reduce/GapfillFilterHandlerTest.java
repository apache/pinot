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
package org.apache.pinot.core.query.reduce;

import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class GapfillFilterHandlerTest {

  @Test
  public void testFilter() {
    QueryContext queryContext =
        QueryContextConverterUtils.getQueryContext("SELECT d1, d2 FROM testTable WHERE d2 > 5");
    DataSchema dataSchema =
        new DataSchema(new String[]{"d1", "d2"}, new ColumnDataType[]{ColumnDataType.INT, ColumnDataType.LONG});
    GapfillFilterHandler gapfillFilterHandler = new GapfillFilterHandler(queryContext.getFilter(), dataSchema);
    assertFalse(gapfillFilterHandler.isMatch(new Object[]{1, 5L}));
    assertTrue(gapfillFilterHandler.isMatch(new Object[]{2, 10L}));
    assertFalse(gapfillFilterHandler.isMatch(new Object[]{3, 3L}));
  }
}
