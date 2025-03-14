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
package org.apache.pinot.query.testutils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.runtime.blocks.RowHeapDataBlock;
import org.apache.pinot.query.runtime.blocks.SuccessMseBlock;
import org.apache.pinot.query.runtime.operator.MultiStageOperator;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;


public class MockDataBlockOperatorFactory {
  protected final Map<String, List<List<Object[]>>> _rowsMap;
  protected final Map<String, DataSchema> _operatorSchemaMap;

  public MockDataBlockOperatorFactory() {
    _rowsMap = new HashMap<>();
    _operatorSchemaMap = new HashMap<>();
  }

  public MockDataBlockOperatorFactory registerOperator(String operatorName, DataSchema resultSchema) {
    _operatorSchemaMap.put(operatorName, resultSchema);
    return this;
  }

  public MockDataBlockOperatorFactory addRows(String operatorName, List<Object[]> rows) {
    List<List<Object[]>> operatorRows = _rowsMap.getOrDefault(operatorName, new ArrayList<>());
    operatorRows.add(rows);
    _rowsMap.put(operatorName, operatorRows);
    return this;
  }

  @SuppressWarnings("unchecked")
  public MultiStageOperator buildMockOperator(String operatorName) {
    MultiStageOperator operator = Mockito.mock(MultiStageOperator.class);
    Mockito.when(operator.nextBlock()).thenAnswer(new Answer<Object>() {
      private int _invocationCount = 0;
      public Object answer(InvocationOnMock invocation) {
        return _invocationCount >= _rowsMap.get(operatorName).size()
            ? SuccessMseBlock.INSTANCE
            : new RowHeapDataBlock(_rowsMap.get(operatorName).get(_invocationCount++),
                _operatorSchemaMap.get(operatorName));
      }
    });
    return operator;
  }

  public DataSchema getDataSchema(String operatorName) {
    return _operatorSchemaMap.get(operatorName);
  }
}
