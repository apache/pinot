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
package org.apache.pinot.core.operator.transform.function;

import java.util.Map;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.RequestContextUtils;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.apache.pinot.segment.spi.datasource.OpenStructDataSource;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;


/// A CASE branch that is a whole OPEN_STRUCT column is read as the column's JSON document, the way a bare projection
/// of the column reads it, so the leaf stage can fetch the CASE result.
public class CaseTransformFunctionOpenStructTest {
  private static final String DOCUMENT = "{\"latencyMs\":0}";

  @Test
  public void testOpenStructBranchIsReadAsJsonText() {
    Map<String, DataSource> columns = Map.of("flag", dataSource(DataSource.class, DataType.LONG),
        "metrics", dataSource(OpenStructDataSource.class, DataType.OPEN_STRUCT));
    ExpressionContext expression = RequestContextUtils.getExpression("CASE WHEN flag = 0 THEN metrics ELSE NULL END");
    TransformFunction function = TransformFunctionFactory.get(expression, columns);
    assertEquals(function.getResultMetadata().getDataType(), DataType.STRING);

    ValueBlock block = mock(ValueBlock.class);
    when(block.getNumDocs()).thenReturn(1);
    BlockValSet flag = mock(BlockValSet.class);
    when(flag.getLongValuesSV()).thenReturn(new long[]{0L});
    BlockValSet metrics = mock(BlockValSet.class);
    when(metrics.getStringValuesSV()).thenReturn(new String[]{DOCUMENT});
    when(block.getBlockValueSet("flag")).thenReturn(flag);
    when(block.getBlockValueSet("metrics")).thenReturn(metrics);
    assertEquals(function.transformToStringValuesSV(block)[0], DOCUMENT);
  }

  private static DataSource dataSource(Class<? extends DataSource> type, DataType dataType) {
    DataSource dataSource = mock(type);
    DataSourceMetadata metadata = mock(DataSourceMetadata.class);
    when(metadata.getDataType()).thenReturn(dataType);
    when(metadata.isSingleValue()).thenReturn(true);
    when(dataSource.getDataSourceMetadata()).thenReturn(metadata);
    return dataSource;
  }
}
