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
package org.apache.pinot.query.mailbox;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.common.proto.Worker;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.blocks.SuccessMseBlock;
import org.apache.pinot.query.runtime.operator.MailboxSendOperator;
import org.apache.pinot.query.runtime.operator.OperatorTestUtil;
import org.apache.pinot.query.testutils.QueryTestUtils;
import org.apache.pinot.spi.config.instance.InstanceType;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


/// End-to-end tests for materialized mailbox storage and server-streaming transport.
public class MaterializedMailboxServiceTest {
  private static final DataSchema DATA_SCHEMA =
      new DataSchema(new String[]{"value"}, new ColumnDataType[]{ColumnDataType.STRING});
  private static final long REQUEST_ID = 41L;
  private static final int PRODUCER_STAGE_ID = 3;
  private static final int PRODUCER_WORKER_ID = 2;
  private static final int LOGICAL_PARTITION_ID = 1;

  @Test
  public void testConstructionDoesNotTouchMaterializedRoot()
      throws Exception {
    Path rootParent = Files.createTempFile("invalid-materialized-mailbox-root", null);
    Path invalidRoot = rootParent.resolve("mailboxes");
    MailboxService service = new MailboxService("localhost", QueryTestUtils.getAvailablePort(), InstanceType.SERVER,
        new PinotConfiguration(Map.of()), null, null, invalidRoot);
    service.start();
    try {
      assertFalse(Files.exists(invalidRoot));
    } finally {
      service.shutdown();
      Files.deleteIfExists(rootParent);
    }
  }

  @DataProvider(name = "readCases")
  public Object[][] readCases() {
    return new Object[][]{
        {true, 2},
        {false, 2},
        {false, 0}
    };
  }

  @Test(dataProvider = "readCases")
  public void testReadRoundTripsAndDeletesAfterExhaustion(boolean localRead, int numRows)
      throws Exception {
    MailboxService producer = createService("localhost", QueryTestUtils.getAvailablePort());
    MailboxService consumer = createService("localhost", QueryTestUtils.getAvailablePort());
    producer.start();
    consumer.start();
    try {
      StatMap<MailboxSendOperator.StatKey> stats = new StatMap<>(MailboxSendOperator.StatKey.class);
      AtomicReference<Worker.MaterializedPartitionHandle> committed = new AtomicReference<>();
      SendingMailbox sendingMailbox = producer.getMaterializedSendingMailbox(
          REQUEST_ID, PRODUCER_STAGE_ID, PRODUCER_WORKER_ID, LOGICAL_PARTITION_ID, stats, committed::set);
      if (numRows > 0) {
        sendingMailbox.send(OperatorTestUtil.block(DATA_SCHEMA, rows(numRows)));
      }
      sendingMailbox.send(SuccessMseBlock.INSTANCE, List.of());

      Worker.MaterializedPartitionHandle handle = committed.get();
      assertEquals(handle.getRowCount(), numRows);
      assertEquals(handle.getHost(), producer.getHostname());
      assertEquals(handle.getTransferPort(), producer.getPort());
      assertTrue(handle.getByteCount() >= 0);

      MailboxService reader = localRead ? producer : consumer;
      Iterator<MseBlock.Data> blocks = reader.readMaterializedPartition(
          handle.getHost(), handle.getTransferPort(), handle.getRequestId(), handle.getProducerStageId(),
          handle.getProducerWorkerId(), handle.getLogicalPartitionId(), deadlineMs(10_000L));
      List<String> actualValues = new ArrayList<>();
      while (blocks.hasNext()) {
        for (Object[] row : blocks.next().asRowHeap().getRows()) {
          actualValues.add((String) row[0]);
        }
      }
      assertEquals(actualValues, values(numRows));
      assertFalse(blocks.hasNext());
      assertEquals(stats.getInt(MailboxSendOperator.StatKey.RAW_MESSAGES), numRows > 0 ? 1 : 0);

      assertThrows(RuntimeException.class, () -> reader.readMaterializedPartition(
          handle.getHost(), handle.getTransferPort(), handle.getRequestId(), handle.getProducerStageId(),
          handle.getProducerWorkerId(), handle.getLogicalPartitionId(), deadlineMs(50L)).hasNext());
    } finally {
      consumer.shutdown();
      producer.shutdown();
    }
  }

  @DataProvider(name = "cleanupCases")
  public Object[][] cleanupCases() {
    return new Object[][]{
        {true},
        {false}
    };
  }

  @Test(dataProvider = "cleanupCases")
  public void testCleanupRejectsLocalAndRemoteReads(boolean localRead)
      throws Exception {
    MailboxService producer = createService("localhost", QueryTestUtils.getAvailablePort());
    MailboxService consumer = createService("localhost", QueryTestUtils.getAvailablePort());
    producer.start();
    consumer.start();
    try {
      producer.cleanupMaterializedMailboxRequest(REQUEST_ID);
      MailboxService reader = localRead ? producer : consumer;
      assertThrows(RuntimeException.class, () -> reader.readMaterializedPartition(
          producer.getHostname(), producer.getPort(), REQUEST_ID, PRODUCER_STAGE_ID, PRODUCER_WORKER_ID,
          LOGICAL_PARTITION_ID, deadlineMs(1_000L)).hasNext());
    } finally {
      consumer.shutdown();
      producer.shutdown();
    }
  }

  private static MailboxService createService(String hostname, int port)
      throws Exception {
    return new MailboxService(hostname, port, InstanceType.SERVER, new PinotConfiguration(Map.of()), null, null,
        Files.createTempDirectory("materialized-mailbox-service"));
  }

  private static Object[][] rows(int numRows) {
    Object[][] rows = new Object[numRows][];
    for (int i = 0; i < numRows; i++) {
      rows[i] = new Object[]{"value-" + i};
    }
    return rows;
  }

  private static List<String> values(int numRows) {
    List<String> values = new ArrayList<>(numRows);
    for (int i = 0; i < numRows; i++) {
      values.add("value-" + i);
    }
    return values;
  }

  private static long deadlineMs(long delayMs) {
    return System.currentTimeMillis() + delayMs;
  }
}
