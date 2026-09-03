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
package org.apache.pinot.query.runtime.operator.operands;

import java.util.List;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.testng.Assert;
import org.testng.annotations.Test;


public class FilterOperandTest {
  private static final DataSchema VARIANT_SCHEMA =
      new DataSchema(new String[]{"payload"}, new ColumnDataType[]{ColumnDataType.VARIANT});
  private static final List<RexExpression> VARIANT_OPERANDS =
      List.of(new RexExpression.InputRef(0), new RexExpression.InputRef(0));

  @Test
  public void testRawVariantInIsRejected() {
    IllegalArgumentException exception =
        Assert.expectThrows(IllegalArgumentException.class,
            () -> new FilterOperand.In(VARIANT_OPERANDS, VARIANT_SCHEMA, false));
    Assert.assertTrue(exception.getMessage().contains("Raw VARIANT values do not support IN"));
  }

  @Test
  public void testRawVariantNotInIsRejected() {
    IllegalArgumentException exception =
        Assert.expectThrows(IllegalArgumentException.class,
            () -> new FilterOperand.In(VARIANT_OPERANDS, VARIANT_SCHEMA, true));
    Assert.assertTrue(exception.getMessage().contains("Raw VARIANT values do not support IN"));
  }
}
