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
package org.apache.pinot.core.operator.blocks.results;

import java.util.List;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.VariantUtils;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.exception.QueryException;
import org.apache.pinot.spi.utils.ByteArray;
import org.testng.Assert;
import org.testng.annotations.Test;


/// Tests the final selection-result contracts for logical VARIANT values.
public class SelectionResultsBlockTest {
  private static final DataSchema VARIANT_SCHEMA =
      new DataSchema(new String[]{"payload"}, new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.VARIANT});

  @Test
  public void testRawVariantProjectionRequiresNullHandling()
      throws Exception {
    List<Object[]> rows = List.<Object[]>of(
        new Object[]{new ByteArray(VariantUtils.parseJsonToVariant("{\"answer\":42}"))});
    SelectionResultsBlock disabledBlock =
        new SelectionResultsBlock(VARIANT_SCHEMA, rows, new QueryContext.Builder().build());

    QueryException exception = Assert.expectThrows(QueryException.class, disabledBlock::getDataTable);
    Assert.assertEquals(exception.getErrorCode(), QueryErrorCode.QUERY_VALIDATION);
    Assert.assertTrue(exception.getMessage().contains("requires query null handling"));

    QueryContext nullAwareContext = new QueryContext.Builder().build();
    nullAwareContext.setNullHandlingEnabled(true);
    SelectionResultsBlock enabledBlock = new SelectionResultsBlock(VARIANT_SCHEMA, rows, nullAwareContext);
    Assert.assertEquals(enabledBlock.getDataTable().getNumberOfRows(), 1);
  }
}
