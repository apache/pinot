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
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.routing.StageMetadata;
import org.apache.pinot.query.routing.StagePlan;
import org.apache.pinot.query.routing.WorkerMetadata;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.plan.MultiStageQueryStats;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.apache.pinot.query.runtime.plan.server.ServerPlanRequestContext;
import org.apache.pinot.query.testutils.MockDataBlockOperatorFactory;
import org.apache.pinot.spi.utils.CommonConstants;
import org.testng.Assert;

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

  public static MultiStageQueryStats getDummyStats(int stageId) {
    return MultiStageQueryStats.createLeaf(stageId, new StatMap<>(LeafStageTransferableBlockOperator.StatKey.class));
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

  public static OpChainExecutionContext getTracingContext() {
    return getTracingContext(ImmutableMap.of(CommonConstants.Broker.Request.TRACE, "true"));
  }

  public static OpChainExecutionContext getNoTracingContext() {
    return getTracingContext(ImmutableMap.of());
  }

  private static OpChainExecutionContext getTracingContext(Map<String, String> opChainMetadata) {
    MailboxService mailboxService = mock(MailboxService.class);
    when(mailboxService.getHostname()).thenReturn("localhost");
    when(mailboxService.getPort()).thenReturn(1234);
    WorkerMetadata workerMetadata = new WorkerMetadata(0, ImmutableMap.of(), ImmutableMap.of());
    StageMetadata stageMetadata = new StageMetadata(0, ImmutableList.of(workerMetadata), ImmutableMap.of());
    OpChainExecutionContext opChainExecutionContext = new OpChainExecutionContext(mailboxService, 123L, Long.MAX_VALUE,
        opChainMetadata, stageMetadata, workerMetadata, null);

    StagePlan stagePlan = new StagePlan(null, stageMetadata);

    opChainExecutionContext.setLeafStageContext(
        new ServerPlanRequestContext(stagePlan, null, null, null));
    return opChainExecutionContext;
  }

  /**
   * Verifies that the given block is a successful end of stream block, verifies that its stats are of the same family
   * as the given keyClass and returns the {@link StatMap} cast to the that key class.
   */
  public static <K extends Enum<K> & StatMap.Key> StatMap<K> getStatMap(Class<K> keyClass, TransferableBlock block) {
    Assert.assertTrue(block.isSuccessfulEndOfStreamBlock(), "Expected EOS block but found " + block.getClass());
    MultiStageQueryStats queryStats = block.getQueryStats();
    Assert.assertNotNull(queryStats, "Stats holder should not be null");
    MultiStageQueryStats.StageStats stageStats = queryStats.getCurrentStats();
    Assert.assertEquals(stageStats.getLastOperatorStats().getKeyClass(), keyClass,
        "Key class should be " + keyClass.getName());

    @SuppressWarnings("unchecked")
    StatMap<K> lastOperatorStats = (StatMap<K>) stageStats.getLastOperatorStats();
    return lastOperatorStats;
  }
}
