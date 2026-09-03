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
package org.apache.pinot.core.query.executor;

import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.QueryContextConverterUtils;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.exception.BadQueryRequestException;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertTrue;


public class ServerQueryExecutorV1ImplTest {
  private static final Schema VARIANT_SCHEMA = new Schema.SchemaBuilder()
      .setSchemaName("testTable")
      .addSingleValueDimension("payload", DataType.VARIANT)
      .build();

  @DataProvider(name = "rawVariantFilters")
  public Object[][] rawVariantFilters() {
    return new Object[][]{
        {"payload = '00'"},
        {"payload IN ('00', '01')"},
        {"payload BETWEEN '00' AND '01'"},
        {"REGEXP_LIKE(payload, '.*')"}
    };
  }

  @Test(dataProvider = "rawVariantFilters")
  public void testRawVariantFiltersAreRejectedBeforeSegmentPruning(String filter) {
    QueryContext queryContext =
        QueryContextConverterUtils.getQueryContext("SELECT COUNT(*) FROM testTable WHERE " + filter);
    queryContext.setSchema(VARIANT_SCHEMA);

    BadQueryRequestException exception = Assert.expectThrows(BadQueryRequestException.class,
        () -> ServerQueryExecutorV1Impl.validateVariantFilterPredicates(queryContext));
    assertTrue(exception.getMessage().contains("extract a typed path with variantGet first"));
  }
}
