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
package org.apache.pinot.query.runtime.operator;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.routing.StageMetadata;
import org.apache.pinot.query.routing.VirtualServerAddress;
import org.apache.pinot.query.routing.WorkerMetadata;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.operator.utils.OperatorUtils;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.apache.pinot.query.testutils.MockDataBlockOperatorFactory;
import org.apache.pinot.spi.utils.CommonConstants;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class OperatorTestUtil {
  // simple key-value collision schema/data test set: "Aa" and "BB" have same hash code in java.
  private static final List<List<Object[]>> SIMPLE_KV_DATA_ROWS =
      ImmutableList.of(ImmutableList.of(new Object[]{1, "Aa"}, new Object[]{2, "BB"}, new Object[]{3, "BB"}),
          ImmutableList.of(new Object[]{1, "AA"}, new Object[]{2, "Aa"}));
  private static final MockDataBlockOperatorFactory MOCK_OPERATOR_FACTORY;

  public static final DataSchema SIMPLE_KV_DATA_SCHEMA = new DataSchema(new String[]{"foo", "bar"},
      new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING});

  public static final String OP_1 = "op1";
  public static final String OP_2 = "op2";

  public static Map<String, String> getDummyStats(long requestId, int stageId, VirtualServerAddress serverAddress) {
    OperatorStats operatorStats = new OperatorStats(requestId, stageId, serverAddress);
    String statsId = new OpChainId(requestId, serverAddress.workerId(), stageId).toString();
    return OperatorUtils.getMetadataFromOperatorStats(ImmutableMap.of(statsId, operatorStats));
  }

  static {
    MOCK_OPERATOR_FACTORY = new MockDataBlockOperatorFactory().registerOperator(OP_1, SIMPLE_KV_DATA_SCHEMA)
        .registerOperator(OP_2, SIMPLE_KV_DATA_SCHEMA).addRows(OP_1, SIMPLE_KV_DATA_ROWS.get(0))
        .addRows(OP_2, SIMPLE_KV_DATA_ROWS.get(1));
  }

  private OperatorTestUtil() {
  }

  public static MultiStageOperator getOperator(String operatorName) {
    return MOCK_OPERATOR_FACTORY.buildMockOperator(operatorName);
  }

  public static DataSchema getDataSchema(String operatorName) {
    return MOCK_OPERATOR_FACTORY.getDataSchema(operatorName);
  }

  public static TransferableBlock block(DataSchema schema, Object[]... rows) {
    return new TransferableBlock(Arrays.asList(rows), schema, DataBlock.Type.ROW);
  }

  public static OpChainExecutionContext getOpChainContext(MailboxService mailboxService, long deadlineMs,
      StageMetadata stageMetadata) {
    return new OpChainExecutionContext(mailboxService, 0, deadlineMs, ImmutableMap.of(), stageMetadata,
        stageMetadata.getWorkerMetadataList().get(0), null);
  }

  public static OpChainExecutionContext getDefaultContext() {
    return getDefaultContext(ImmutableMap.of(CommonConstants.Broker.Request.TRACE, "true"));
  }

  public static OpChainExecutionContext getDefaultContextWithTracingDisabled() {
    return getDefaultContext(ImmutableMap.of());
  }

  private static OpChainExecutionContext getDefaultContext(Map<String, String> opChainMetadata) {
    MailboxService mailboxService = mock(MailboxService.class);
    when(mailboxService.getHostname()).thenReturn("localhost");
    when(mailboxService.getPort()).thenReturn(1234);
    WorkerMetadata workerMetadata = new WorkerMetadata(0, ImmutableMap.of(), ImmutableMap.of());
    return new OpChainExecutionContext(mailboxService, 123L, Long.MAX_VALUE, opChainMetadata,
        new StageMetadata(0, ImmutableList.of(workerMetadata), ImmutableMap.of()), workerMetadata, null);
  }
}
