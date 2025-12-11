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
package org.apache.pinot.core.query.request.context.utils;

import java.util.Collections;
import org.apache.pinot.common.request.ArrayJoinOperand;
import org.apache.pinot.common.request.ArrayJoinSpec;
import org.apache.pinot.common.request.ArrayJoinType;
import org.apache.pinot.common.request.DataSource;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.core.query.request.context.ArrayJoinContext;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;


public class QueryContextConverterUtilsTest {

  @Test
  public void testArrayJoinConversion() {
    PinotQuery pinotQuery = new PinotQuery();
    DataSource dataSource = new DataSource();
    dataSource.setTableName("inputTable");
    pinotQuery.setDataSource(dataSource);
    pinotQuery.setSelectList(Collections.singletonList(RequestUtils.getIdentifierExpression("city")));

    ArrayJoinOperand operand = new ArrayJoinOperand();
    operand.setExpression(RequestUtils.getIdentifierExpression("cities"));
    operand.setAlias("city");

    ArrayJoinSpec spec = new ArrayJoinSpec();
    spec.setType(ArrayJoinType.INNER);
    spec.setOperands(Collections.singletonList(operand));
    pinotQuery.setArrayJoinList(Collections.singletonList(spec));

    QueryContext queryContext = QueryContextConverterUtils.getQueryContext(pinotQuery);
    assertFalse(queryContext.getArrayJoinContexts().isEmpty());
    ArrayJoinContext context = queryContext.getArrayJoinContexts().get(0);
    assertEquals(context.getOperands().size(), 1);
    ArrayJoinContext.Operand operandContext = context.getOperands().get(0);
    assertEquals(operandContext.getExpression().getIdentifier(), "cities");
    assertEquals(operandContext.getAlias(), "city");
  }
}
