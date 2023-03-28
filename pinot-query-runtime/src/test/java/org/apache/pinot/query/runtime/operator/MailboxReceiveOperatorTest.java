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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.pinot.common.datablock.MetadataBlock;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.mailbox.JsonMailboxIdentifier;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.mailbox.ReceivingMailbox;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.routing.VirtualServer;
import org.apache.pinot.query.routing.VirtualServerAddress;
import org.apache.pinot.query.runtime.blocks.TransferableBlock;
import org.apache.pinot.query.runtime.blocks.TransferableBlockUtils;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.apache.pinot.common.utils.DataSchema.ColumnDataType.INT;
import static org.apache.pinot.common.utils.DataSchema.ColumnDataType.STRING;


public class MailboxReceiveOperatorTest {

  private static final int DEFAULT_RECEIVER_STAGE_ID = 10;

  private AutoCloseable _mocks;

  @Mock
  private ReceivingMailbox<TransferableBlock> _mailbox;

  @Mock
  private ReceivingMailbox<TransferableBlock> _mailbox2;

  @Mock
  private MailboxService<TransferableBlock> _mailboxService;
  @Mock
  private VirtualServer _server1;
  @Mock
  private VirtualServer _server2;

  private final VirtualServerAddress _testAddr = new VirtualServerAddress("test", 123, 0);

  private final Random _random = new Random();

  @BeforeMethod
  public void setUp() {
    _mocks = MockitoAnnotations.openMocks(this);
  }

  @AfterMethod
  public void tearDown()
      throws Exception {
    _mocks.close();
  }

  @Test
  public void shouldTimeoutOnExtraLongSleep()
      throws InterruptedException {
    // Choose whether to exercise the ordering code path or not
    List<RexExpression> collationKeys = new ArrayList<>();
    List<RelFieldCollation.Direction> collationDirections = new ArrayList<>();
    boolean sortOnReceiver = false;
    if (_random.nextBoolean()) {
      collationKeys.add(new RexExpression.InputRef(0));
      collationDirections.add(RelFieldCollation.Direction.ASCENDING);
      sortOnReceiver = true;
    }

    // shorter timeoutMs should result in error.
    DataSchema inSchema = new DataSchema(new String[]{"col1", "col2"}, new DataSchema.ColumnDataType[]{INT, INT});
    OpChainExecutionContext context =
        new OpChainExecutionContext(_mailboxService, 1, DEFAULT_RECEIVER_STAGE_ID, _testAddr, 10L, 10L,
            new HashMap<>());
    MailboxReceiveOperator receiveOp =
        new MailboxReceiveOperator(context, new ArrayList<>(), RelDistribution.Type.SINGLETON, collationKeys,
            collationDirections, false, sortOnReceiver, inSchema, 456, 789, 10L);
    Thread.sleep(200L);
    TransferableBlock mailbox = receiveOp.nextBlock();
    Assert.assertTrue(mailbox.isErrorBlock());
    MetadataBlock errorBlock = (MetadataBlock) mailbox.getDataBlock();
    Assert.assertTrue(errorBlock.getExceptions().containsKey(QueryException.EXECUTION_TIMEOUT_ERROR_CODE));

    // longer timeout or default timeout (10s) doesn't result in error.
    context = new OpChainExecutionContext(_mailboxService, 1, DEFAULT_RECEIVER_STAGE_ID, _testAddr, 2000L, 2000L,
        new HashMap<>());
    receiveOp = new MailboxReceiveOperator(context, new ArrayList<>(), RelDistribution.Type.SINGLETON, collationKeys,
        collationDirections, false, sortOnReceiver, inSchema, 456, 789, 2000L);
    Thread.sleep(200L);
    mailbox = receiveOp.nextBlock();
    Assert.assertFalse(mailbox.isErrorBlock());
    context = new OpChainExecutionContext(_mailboxService, 1, DEFAULT_RECEIVER_STAGE_ID, _testAddr, Long.MAX_VALUE,
        Long.MAX_VALUE, new HashMap<>());
    receiveOp = new MailboxReceiveOperator(context, new ArrayList<>(), RelDistribution.Type.SINGLETON, collationKeys,
        collationDirections, false, sortOnReceiver, inSchema, 456, 789, null);
    Thread.sleep(200L);
    mailbox = receiveOp.nextBlock();
    Assert.assertFalse(mailbox.isErrorBlock());
  }

  @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*multiple instance "
      + "found.*")
  public void shouldThrowReceiveSingletonFromMultiMatchMailboxServer() {

    Mockito.when(_mailboxService.getHostname()).thenReturn("singleton");
    Mockito.when(_mailboxService.getMailboxPort()).thenReturn(123);

    Mockito.when(_server1.getHostname()).thenReturn("singleton");
    Mockito.when(_server1.getQueryMailboxPort()).thenReturn(123);

    Mockito.when(_server2.getHostname()).thenReturn("singleton");
    Mockito.when(_server2.getQueryMailboxPort()).thenReturn(123);

    DataSchema inSchema = new DataSchema(new String[]{"col1", "col2"}, new DataSchema.ColumnDataType[]{INT, INT});

    // Choose whether to exercise the ordering code path or not
    List<RexExpression> collationKeys = new ArrayList<>();
    List<RelFieldCollation.Direction> collationDirections = new ArrayList<>();
    boolean sortOnReceiver = false;
    if (_random.nextBoolean()) {
      collationKeys.add(new RexExpression.InputRef(0));
      collationDirections.add(RelFieldCollation.Direction.ASCENDING);
      sortOnReceiver = true;
    }

    OpChainExecutionContext context =
        new OpChainExecutionContext(_mailboxService, 1, DEFAULT_RECEIVER_STAGE_ID, _testAddr, Long.MAX_VALUE,
            Long.MAX_VALUE, new HashMap<>());
    MailboxReceiveOperator receiveOp =
        new MailboxReceiveOperator(context, ImmutableList.of(_server1, _server2), RelDistribution.Type.SINGLETON,
            collationKeys, collationDirections, false, sortOnReceiver, inSchema, 456, 789, null);
  }

  @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = ".*RANGE_DISTRIBUTED.*")
  public void shouldThrowRangeDistributionNotSupported() {
    Mockito.when(_mailboxService.getHostname()).thenReturn("singleton");
    Mockito.when(_mailboxService.getMailboxPort()).thenReturn(123);

    Mockito.when(_server1.getHostname()).thenReturn("singleton");
    Mockito.when(_server1.getQueryMailboxPort()).thenReturn(123);

    Mockito.when(_server2.getHostname()).thenReturn("singleton");
    Mockito.when(_server2.getQueryMailboxPort()).thenReturn(123);

    DataSchema inSchema = new DataSchema(new String[]{"col1", "col2"}, new DataSchema.ColumnDataType[]{INT, INT});

    // Choose whether to exercise the ordering code path or not
    List<RexExpression> collationKeys = new ArrayList<>();
    List<RelFieldCollation.Direction> collationDirections = new ArrayList<>();
    boolean sortOnReceiver = false;
    if (_random.nextBoolean()) {
      collationKeys.add(new RexExpression.InputRef(0));
      collationDirections.add(RelFieldCollation.Direction.ASCENDING);
      sortOnReceiver = true;
    }

    OpChainExecutionContext context =
        new OpChainExecutionContext(_mailboxService, 1, DEFAULT_RECEIVER_STAGE_ID, _testAddr, Long.MAX_VALUE,
            Long.MAX_VALUE, new HashMap<>());
    MailboxReceiveOperator receiveOp = new MailboxReceiveOperator(context, ImmutableList.of(_server1, _server2),
        RelDistribution.Type.RANGE_DISTRIBUTED, collationKeys, collationDirections, false, sortOnReceiver, inSchema,
        456, 789, null);
  }

  @Test
  public void shouldReceiveSingletonNoMatchMailboxServer() {
    String serverHost = "singleton";
    int server1Port = 123;
    Mockito.when(_server1.getHostname()).thenReturn(serverHost);
    Mockito.when(_server1.getQueryMailboxPort()).thenReturn(server1Port);
    Mockito.when(_server1.getPartitionIds()).thenReturn(Collections.singletonList(0));

    int server2Port = 456;
    Mockito.when(_server2.getHostname()).thenReturn(serverHost);
    Mockito.when(_server2.getQueryMailboxPort()).thenReturn(server2Port);
    Mockito.when(_server2.getPartitionIds()).thenReturn(Collections.singletonList(0));

    int mailboxPort = 789;
    Mockito.when(_mailboxService.getHostname()).thenReturn(serverHost);
    Mockito.when(_mailboxService.getMailboxPort()).thenReturn(mailboxPort);

    int jobId = 456;
    int stageId = 0;
    int toPort = 8888;
    String toHost = "toHost";
    VirtualServerAddress toAddress = new VirtualServerAddress(toHost, toPort, 0);

    DataSchema inSchema = new DataSchema(new String[]{"col1", "col2"}, new DataSchema.ColumnDataType[]{INT, INT});

    // Choose whether to exercise the ordering code path or not
    List<RexExpression> collationKeys = new ArrayList<>();
    List<RelFieldCollation.Direction> collationDirections = new ArrayList<>();
    boolean sortOnReceiver = false;
    if (_random.nextBoolean()) {
      collationKeys.add(new RexExpression.InputRef(0));
      collationDirections.add(RelFieldCollation.Direction.ASCENDING);
      sortOnReceiver = true;
    }

    OpChainExecutionContext context =
        new OpChainExecutionContext(_mailboxService, jobId, DEFAULT_RECEIVER_STAGE_ID, toAddress, Long.MAX_VALUE,
            Long.MAX_VALUE, new HashMap<>());

    MailboxReceiveOperator receiveOp =
        new MailboxReceiveOperator(context, ImmutableList.of(_server1, _server2), RelDistribution.Type.SINGLETON,
            collationKeys, collationDirections, false, sortOnReceiver, inSchema, stageId, DEFAULT_RECEIVER_STAGE_ID,
            null);

    // Receive end of stream block directly when there is no match.
    Assert.assertTrue(receiveOp.nextBlock().isEndOfStreamBlock());
  }

  @Test
  public void shouldReceiveSingletonCloseMailbox() {
    String serverHost = "singleton";
    int server1Port = 123;
    Mockito.when(_server1.getHostname()).thenReturn(serverHost);
    Mockito.when(_server1.getQueryMailboxPort()).thenReturn(server1Port);
    Mockito.when(_server1.getPartitionIds()).thenReturn(Collections.singletonList(0));

    int server2Port = 456;
    Mockito.when(_server2.getHostname()).thenReturn(serverHost);
    Mockito.when(_server2.getQueryMailboxPort()).thenReturn(server2Port);
    Mockito.when(_server2.getPartitionIds()).thenReturn(Collections.singletonList(0));

    int mailboxPort = server2Port;
    Mockito.when(_mailboxService.getHostname()).thenReturn(serverHost);
    Mockito.when(_mailboxService.getMailboxPort()).thenReturn(mailboxPort);

    int jobId = 456;
    int stageId = 0;
    int toPort = 8888;
    String toHost = "toHost";
    VirtualServerAddress toAddress = new VirtualServerAddress(toHost, toPort, 0);

    JsonMailboxIdentifier expectedMailboxId = new JsonMailboxIdentifier(String.format("%s_%s", jobId, stageId),
        new VirtualServerAddress(serverHost, server2Port, 0), toAddress, stageId, DEFAULT_RECEIVER_STAGE_ID);
    Mockito.when(_mailboxService.getReceivingMailbox(expectedMailboxId)).thenReturn(_mailbox);
    Mockito.when(_mailbox.isClosed()).thenReturn(true);

    DataSchema inSchema = new DataSchema(new String[]{"col1", "col2"}, new DataSchema.ColumnDataType[]{INT, INT});

    // Choose whether to exercise the ordering code path or not
    List<RexExpression> collationKeys = new ArrayList<>();
    List<RelFieldCollation.Direction> collationDirections = new ArrayList<>();
    boolean sortOnReceiver = false;
    if (_random.nextBoolean()) {
      collationKeys.add(new RexExpression.InputRef(0));
      collationDirections.add(RelFieldCollation.Direction.ASCENDING);
      sortOnReceiver = true;
    }

    OpChainExecutionContext context =
        new OpChainExecutionContext(_mailboxService, jobId, DEFAULT_RECEIVER_STAGE_ID, toAddress, Long.MAX_VALUE,
            Long.MAX_VALUE, new HashMap<>());

    MailboxReceiveOperator receiveOp =
        new MailboxReceiveOperator(context, ImmutableList.of(_server1, _server2), RelDistribution.Type.SINGLETON,
            collationKeys, collationDirections, false, sortOnReceiver, inSchema, stageId, DEFAULT_RECEIVER_STAGE_ID,
            null);

    // Receive end of stream block directly when mailbox is close.
    Assert.assertTrue(receiveOp.nextBlock().isEndOfStreamBlock());
  }

  @Test
  public void shouldReceiveSingletonNullMailbox()
      throws Exception {
    String serverHost = "singleton";
    int server1Port = 123;
    Mockito.when(_server1.getHostname()).thenReturn(serverHost);
    Mockito.when(_server1.getQueryMailboxPort()).thenReturn(server1Port);
    Mockito.when(_server1.getPartitionIds()).thenReturn(Collections.singletonList(0));

    int server2Port = 456;
    Mockito.when(_server2.getHostname()).thenReturn(serverHost);
    Mockito.when(_server2.getQueryMailboxPort()).thenReturn(server2Port);
    Mockito.when(_server2.getPartitionIds()).thenReturn(Collections.singletonList(0));

    int mailboxPort = server2Port;
    Mockito.when(_mailboxService.getHostname()).thenReturn(serverHost);
    Mockito.when(_mailboxService.getMailboxPort()).thenReturn(mailboxPort);

    int jobId = 456;
    int stageId = 0;
    int toPort = 8888;
    String toHost = "toHost";
    VirtualServerAddress toAddress = new VirtualServerAddress(toHost, toPort, 0);

    JsonMailboxIdentifier expectedMailboxId = new JsonMailboxIdentifier(String.format("%s_%s", jobId, stageId),
        new VirtualServerAddress(serverHost, server2Port, 0), toAddress, stageId, DEFAULT_RECEIVER_STAGE_ID);
    Mockito.when(_mailboxService.getReceivingMailbox(expectedMailboxId)).thenReturn(_mailbox);
    Mockito.when(_mailbox.isClosed()).thenReturn(false);
    // Receive null mailbox during timeout.
    Mockito.when(_mailbox.receive()).thenReturn(null);

    DataSchema inSchema = new DataSchema(new String[]{"col1", "col2"}, new DataSchema.ColumnDataType[]{INT, INT});

    // Choose whether to exercise the ordering code path or not
    List<RexExpression> collationKeys = new ArrayList<>();
    List<RelFieldCollation.Direction> collationDirections = new ArrayList<>();
    boolean sortOnReceiver = false;
    if (_random.nextBoolean()) {
      collationKeys.add(new RexExpression.InputRef(0));
      collationDirections.add(RelFieldCollation.Direction.ASCENDING);
      sortOnReceiver = true;
    }

    OpChainExecutionContext context =
        new OpChainExecutionContext(_mailboxService, jobId, DEFAULT_RECEIVER_STAGE_ID, toAddress, Long.MAX_VALUE,
            Long.MAX_VALUE, new HashMap<>());

    MailboxReceiveOperator receiveOp =
        new MailboxReceiveOperator(context, ImmutableList.of(_server1, _server2), RelDistribution.Type.SINGLETON,
            collationKeys, collationDirections, false, sortOnReceiver, inSchema, stageId, DEFAULT_RECEIVER_STAGE_ID,
            null);
    // Receive NoOpBlock.
    Assert.assertTrue(receiveOp.nextBlock().isNoOpBlock());
  }

  @Test
  public void shouldReceiveEosDirectlyFromSender()
      throws Exception {
    String serverHost = "singleton";
    int server1Port = 123;
    Mockito.when(_server1.getHostname()).thenReturn(serverHost);
    Mockito.when(_server1.getQueryMailboxPort()).thenReturn(server1Port);
    Mockito.when(_server1.getPartitionIds()).thenReturn(Collections.singletonList(0));

    int server2Port = 456;
    Mockito.when(_server2.getHostname()).thenReturn(serverHost);
    Mockito.when(_server2.getQueryMailboxPort()).thenReturn(server2Port);
    Mockito.when(_server2.getPartitionIds()).thenReturn(Collections.singletonList(0));

    int mailboxPort = server2Port;
    Mockito.when(_mailboxService.getHostname()).thenReturn(serverHost);
    Mockito.when(_mailboxService.getMailboxPort()).thenReturn(mailboxPort);

    int jobId = 456;
    int stageId = 0;
    int toPort = 8888;
    String toHost = "toHost";
    VirtualServerAddress toAddress = new VirtualServerAddress(toHost, toPort, 0);

    JsonMailboxIdentifier expectedMailboxId = new JsonMailboxIdentifier(String.format("%s_%s", jobId, stageId),
        new VirtualServerAddress(serverHost, server2Port, 0), toAddress, stageId, DEFAULT_RECEIVER_STAGE_ID);
    Mockito.when(_mailboxService.getReceivingMailbox(expectedMailboxId)).thenReturn(_mailbox);
    Mockito.when(_mailbox.isClosed()).thenReturn(false);
    Mockito.when(_mailbox.receive()).thenReturn(TransferableBlockUtils.getEndOfStreamTransferableBlock());

    DataSchema inSchema = new DataSchema(new String[]{"col1", "col2"}, new DataSchema.ColumnDataType[]{INT, INT});

    // Choose whether to exercise the ordering code path or not
    List<RexExpression> collationKeys = new ArrayList<>();
    List<RelFieldCollation.Direction> collationDirections = new ArrayList<>();
    boolean sortOnReceiver = false;
    if (_random.nextBoolean()) {
      collationKeys.add(new RexExpression.InputRef(0));
      collationDirections.add(RelFieldCollation.Direction.ASCENDING);
      sortOnReceiver = true;
    }

    OpChainExecutionContext context =
        new OpChainExecutionContext(_mailboxService, jobId, DEFAULT_RECEIVER_STAGE_ID, toAddress, Long.MAX_VALUE,
            Long.MAX_VALUE, new HashMap<>());

    MailboxReceiveOperator receiveOp =
        new MailboxReceiveOperator(context, ImmutableList.of(_server1, _server2), RelDistribution.Type.SINGLETON,
            collationKeys, collationDirections, false, sortOnReceiver, inSchema, stageId, DEFAULT_RECEIVER_STAGE_ID,
            null);
    // Receive EosBloc.
    Assert.assertTrue(receiveOp.nextBlock().isEndOfStreamBlock());
  }

  @Test
  public void shouldReceiveSingletonMailbox()
      throws Exception {
    String serverHost = "singleton";
    int server1Port = 123;
    Mockito.when(_server1.getHostname()).thenReturn(serverHost);
    Mockito.when(_server1.getQueryMailboxPort()).thenReturn(server1Port);
    Mockito.when(_server1.getPartitionIds()).thenReturn(Collections.singletonList(0));

    int server2Port = 456;
    Mockito.when(_server2.getHostname()).thenReturn(serverHost);
    Mockito.when(_server2.getQueryMailboxPort()).thenReturn(server2Port);
    Mockito.when(_server2.getPartitionIds()).thenReturn(Collections.singletonList(0));

    int mailboxPort = server2Port;
    Mockito.when(_mailboxService.getHostname()).thenReturn(serverHost);
    Mockito.when(_mailboxService.getMailboxPort()).thenReturn(mailboxPort);

    int jobId = 456;
    int stageId = 0;
    int toPort = 8888;
    String toHost = "toHost";
    VirtualServerAddress toAddress = new VirtualServerAddress(toHost, toPort, 0);

    JsonMailboxIdentifier expectedMailboxId = new JsonMailboxIdentifier(String.format("%s_%s", jobId, stageId),
        new VirtualServerAddress(serverHost, server2Port, 0), toAddress, stageId, DEFAULT_RECEIVER_STAGE_ID);
    Mockito.when(_mailboxService.getReceivingMailbox(expectedMailboxId)).thenReturn(_mailbox);
    Mockito.when(_mailbox.isClosed()).thenReturn(false);
    Object[] expRow = new Object[]{1, 1};
    DataSchema inSchema = new DataSchema(new String[]{"col1", "col2"}, new DataSchema.ColumnDataType[]{INT, INT});
    Mockito.when(_mailbox.receive()).thenReturn(OperatorTestUtil.block(inSchema, expRow),
        TransferableBlockUtils.getEndOfStreamTransferableBlock());


    // Choose whether to exercise the ordering code path or not
    List<RexExpression> collationKeys = new ArrayList<>();
    List<RelFieldCollation.Direction> collationDirections = new ArrayList<>();
    boolean sortOnReceiver = false;
    if (_random.nextBoolean()) {
      collationKeys.add(new RexExpression.InputRef(0));
      collationDirections.add(RelFieldCollation.Direction.ASCENDING);
      sortOnReceiver = true;
    }

    OpChainExecutionContext context =
        new OpChainExecutionContext(_mailboxService, jobId, DEFAULT_RECEIVER_STAGE_ID, toAddress, Long.MAX_VALUE,
            Long.MAX_VALUE, new HashMap<>());

    MailboxReceiveOperator receiveOp =
        new MailboxReceiveOperator(context, ImmutableList.of(_server1, _server2), RelDistribution.Type.SINGLETON,
            collationKeys, collationDirections, false, sortOnReceiver, inSchema, stageId, DEFAULT_RECEIVER_STAGE_ID,
            null);
    TransferableBlock receivedBlock = receiveOp.nextBlock();
    while (receivedBlock.isNoOpBlock()) {
      receivedBlock = receiveOp.nextBlock();
    }
    List<Object[]> resultRows = receivedBlock.getContainer();
    Assert.assertEquals(resultRows.size(), 1);
    Assert.assertEquals(resultRows.get(0), expRow);
  }

  @Test
  public void shouldReceiveSingletonErrorMailbox()
      throws Exception {
    String serverHost = "singleton";
    int server1Port = 123;
    Mockito.when(_server1.getHostname()).thenReturn(serverHost);
    Mockito.when(_server1.getQueryMailboxPort()).thenReturn(server1Port);
    Mockito.when(_server1.getPartitionIds()).thenReturn(Collections.singletonList(0));

    int server2Port = 456;
    Mockito.when(_server2.getHostname()).thenReturn(serverHost);
    Mockito.when(_server2.getQueryMailboxPort()).thenReturn(server2Port);
    Mockito.when(_server2.getPartitionIds()).thenReturn(Collections.singletonList(0));

    int mailboxPort = server2Port;
    Mockito.when(_mailboxService.getHostname()).thenReturn(serverHost);
    Mockito.when(_mailboxService.getMailboxPort()).thenReturn(mailboxPort);

    int jobId = 456;
    int stageId = 0;
    int toPort = 8888;
    String toHost = "toHost";
    VirtualServerAddress toAddress = new VirtualServerAddress(toHost, toPort, 0);

    JsonMailboxIdentifier expectedMailboxId = new JsonMailboxIdentifier(String.format("%s_%s", jobId, stageId),
        new VirtualServerAddress(serverHost, server2Port, 0), toAddress, stageId, DEFAULT_RECEIVER_STAGE_ID);
    Mockito.when(_mailboxService.getReceivingMailbox(expectedMailboxId)).thenReturn(_mailbox);
    Mockito.when(_mailbox.isClosed()).thenReturn(false);
    Exception e = new Exception("errorBlock");
    DataSchema inSchema = new DataSchema(new String[]{"col1", "col2"}, new DataSchema.ColumnDataType[]{INT, INT});
    Mockito.when(_mailbox.receive()).thenReturn(TransferableBlockUtils.getErrorTransferableBlock(e));

    // Choose whether to exercise the ordering code path or not
    List<RexExpression> collationKeys = new ArrayList<>();
    List<RelFieldCollation.Direction> collationDirections = new ArrayList<>();
    boolean sortOnReceiver = false;
    if (_random.nextBoolean()) {
      collationKeys.add(new RexExpression.InputRef(0));
      collationDirections.add(RelFieldCollation.Direction.ASCENDING);
      sortOnReceiver = true;
    }

    OpChainExecutionContext context =
        new OpChainExecutionContext(_mailboxService, jobId, DEFAULT_RECEIVER_STAGE_ID, toAddress, Long.MAX_VALUE,
            Long.MAX_VALUE, new HashMap<>());

    MailboxReceiveOperator receiveOp =
        new MailboxReceiveOperator(context, ImmutableList.of(_server1, _server2), RelDistribution.Type.SINGLETON,
            collationKeys, collationDirections, false, sortOnReceiver, inSchema, stageId, DEFAULT_RECEIVER_STAGE_ID,
            null);
    TransferableBlock receivedBlock = receiveOp.nextBlock();
    Assert.assertTrue(receivedBlock.isErrorBlock());
    MetadataBlock error = (MetadataBlock) receivedBlock.getDataBlock();
    Assert.assertTrue(error.getExceptions().get(QueryException.UNKNOWN_ERROR_CODE).contains("errorBlock"));
  }

  @Test
  public void shouldReceiveMailboxFromTwoServersOneClose()
      throws Exception {
    String server1Host = "hash1";
    int server1Port = 123;
    Mockito.when(_server1.getHostname()).thenReturn(server1Host);
    Mockito.when(_server1.getQueryMailboxPort()).thenReturn(server1Port);
    Mockito.when(_server1.getPartitionIds()).thenReturn(Collections.singletonList(0));

    String server2Host = "hash2";
    int server2Port = 456;
    Mockito.when(_server2.getHostname()).thenReturn(server2Host);
    Mockito.when(_server2.getQueryMailboxPort()).thenReturn(server2Port);
    Mockito.when(_server2.getPartitionIds()).thenReturn(Collections.singletonList(0));

    int jobId = 456;
    int stageId = 0;
    int toPort = 8888;
    String toHost = "toHost";
    VirtualServerAddress toAddress = new VirtualServerAddress(toHost, toPort, 0);

    JsonMailboxIdentifier expectedMailboxId1 = new JsonMailboxIdentifier(String.format("%s_%s", jobId, stageId),
        new VirtualServerAddress(server1Host, server1Port, 0), toAddress, stageId, DEFAULT_RECEIVER_STAGE_ID);
    Mockito.when(_mailboxService.getReceivingMailbox(expectedMailboxId1)).thenReturn(_mailbox);
    Mockito.when(_mailbox.isClosed()).thenReturn(true);

    JsonMailboxIdentifier expectedMailboxId2 = new JsonMailboxIdentifier(String.format("%s_%s", jobId, stageId),
        new VirtualServerAddress(server2Host, server2Port, 0), toAddress, stageId, DEFAULT_RECEIVER_STAGE_ID);
    Mockito.when(_mailboxService.getReceivingMailbox(expectedMailboxId2)).thenReturn(_mailbox2);
    Mockito.when(_mailbox2.isClosed()).thenReturn(false);
    Object[] expRow = new Object[]{1, 1};
    DataSchema inSchema = new DataSchema(new String[]{"col1", "col2"}, new DataSchema.ColumnDataType[]{INT, INT});
    Mockito.when(_mailbox2.receive()).thenReturn(OperatorTestUtil.block(inSchema, expRow),
        TransferableBlockUtils.getEndOfStreamTransferableBlock());

    // Choose whether to exercise the ordering code path or not
    List<RexExpression> collationKeys = new ArrayList<>();
    List<RelFieldCollation.Direction> collationDirections = new ArrayList<>();
    boolean sortOnReceiver = false;
    if (_random.nextBoolean()) {
      collationKeys.add(new RexExpression.InputRef(0));
      collationDirections.add(RelFieldCollation.Direction.ASCENDING);
      sortOnReceiver = true;
    }

    OpChainExecutionContext context =
        new OpChainExecutionContext(_mailboxService, jobId, DEFAULT_RECEIVER_STAGE_ID, toAddress, Long.MAX_VALUE,
            Long.MAX_VALUE, new HashMap<>());

    MailboxReceiveOperator receiveOp =
        new MailboxReceiveOperator(context, ImmutableList.of(_server1, _server2), RelDistribution.Type.HASH_DISTRIBUTED,
            collationKeys, collationDirections, false, sortOnReceiver, inSchema, stageId, DEFAULT_RECEIVER_STAGE_ID,
            null);
    TransferableBlock receivedBlock = receiveOp.nextBlock();
    while (receivedBlock.isNoOpBlock()) {
      receivedBlock = receiveOp.nextBlock();
    }
    List<Object[]> resultRows = receivedBlock.getContainer();
    Assert.assertEquals(resultRows.size(), 1);
    Assert.assertEquals(resultRows.get(0), expRow);
  }

  @Test
  public void shouldReceiveMailboxFromTwoServersOneNull()
      throws Exception {
    String server1Host = "hash1";
    int server1Port = 123;
    Mockito.when(_server1.getHostname()).thenReturn(server1Host);
    Mockito.when(_server1.getQueryMailboxPort()).thenReturn(server1Port);
    Mockito.when(_server1.getPartitionIds()).thenReturn(Collections.singletonList(0));

    String server2Host = "hash2";
    int server2Port = 456;
    Mockito.when(_server2.getHostname()).thenReturn(server2Host);
    Mockito.when(_server2.getQueryMailboxPort()).thenReturn(server2Port);
    Mockito.when(_server2.getPartitionIds()).thenReturn(Collections.singletonList(0));

    int jobId = 456;
    int stageId = 0;
    int toPort = 8888;
    String toHost = "toHost";
    VirtualServerAddress toAddress = new VirtualServerAddress(toHost, toPort, 0);

    JsonMailboxIdentifier expectedMailboxId1 = new JsonMailboxIdentifier(String.format("%s_%s", jobId, stageId),
        new VirtualServerAddress(server1Host, server1Port, 0), toAddress, stageId, DEFAULT_RECEIVER_STAGE_ID);
    Mockito.when(_mailboxService.getReceivingMailbox(expectedMailboxId1)).thenReturn(_mailbox);
    Mockito.when(_mailbox.isClosed()).thenReturn(false);
    Mockito.when(_mailbox.receive()).thenReturn(null, TransferableBlockUtils.getEndOfStreamTransferableBlock());

    JsonMailboxIdentifier expectedMailboxId2 = new JsonMailboxIdentifier(String.format("%s_%s", jobId, stageId),
        new VirtualServerAddress(server2Host, server2Port, 0), toAddress, stageId, DEFAULT_RECEIVER_STAGE_ID);
    Mockito.when(_mailboxService.getReceivingMailbox(expectedMailboxId2)).thenReturn(_mailbox2);
    Mockito.when(_mailbox2.isClosed()).thenReturn(false);
    Object[] expRow = new Object[]{1, 1};
    DataSchema inSchema = new DataSchema(new String[]{"col1", "col2"}, new DataSchema.ColumnDataType[]{INT, INT});
    Mockito.when(_mailbox2.receive()).thenReturn(OperatorTestUtil.block(inSchema, expRow),
        TransferableBlockUtils.getEndOfStreamTransferableBlock());

    // Choose whether to exercise the ordering code path or not
    List<RexExpression> collationKeys = new ArrayList<>();
    List<RelFieldCollation.Direction> collationDirections = new ArrayList<>();
    boolean sortOnReceiver = false;
    if (_random.nextBoolean()) {
      collationKeys.add(new RexExpression.InputRef(0));
      collationDirections.add(RelFieldCollation.Direction.ASCENDING);
      sortOnReceiver = true;
    }

    OpChainExecutionContext context =
        new OpChainExecutionContext(_mailboxService, jobId, DEFAULT_RECEIVER_STAGE_ID, toAddress, Long.MAX_VALUE,
            Long.MAX_VALUE, new HashMap<>());

    MailboxReceiveOperator receiveOp =
        new MailboxReceiveOperator(context, ImmutableList.of(_server1, _server2), RelDistribution.Type.HASH_DISTRIBUTED,
            collationKeys, collationDirections, false, sortOnReceiver, inSchema, stageId, DEFAULT_RECEIVER_STAGE_ID,
            null);
    TransferableBlock receivedBlock = receiveOp.nextBlock();
    while (receivedBlock.isNoOpBlock()) {
      receivedBlock = receiveOp.nextBlock();
    }
    List<Object[]> resultRows = receivedBlock.getContainer();
    Assert.assertEquals(resultRows.size(), 1);
    Assert.assertEquals(resultRows.get(0), expRow);
  }

  @Test
  public void shouldReceiveMailboxFromTwoServers()
      throws Exception {
    String server1Host = "hash1";
    int server1Port = 123;
    Mockito.when(_server1.getHostname()).thenReturn(server1Host);
    Mockito.when(_server1.getQueryMailboxPort()).thenReturn(server1Port);
    Mockito.when(_server1.getPartitionIds()).thenReturn(Collections.singletonList(0));

    String server2Host = "hash2";
    int server2Port = 456;
    Mockito.when(_server2.getHostname()).thenReturn(server2Host);
    Mockito.when(_server2.getQueryMailboxPort()).thenReturn(server2Port);
    Mockito.when(_server2.getPartitionIds()).thenReturn(Collections.singletonList(0));

    int jobId = 456;
    int stageId = 0;
    int toPort = 8888;
    String toHost = "toHost";
    VirtualServerAddress toAddress = new VirtualServerAddress(toHost, toPort, 0);

    DataSchema inSchema = new DataSchema(new String[]{"col1", "col2"}, new DataSchema.ColumnDataType[]{INT, INT});
    JsonMailboxIdentifier expectedMailboxId1 = new JsonMailboxIdentifier(String.format("%s_%s", jobId, stageId),
        new VirtualServerAddress(server1Host, server1Port, 0), toAddress, stageId, DEFAULT_RECEIVER_STAGE_ID);
    Mockito.when(_mailboxService.getReceivingMailbox(expectedMailboxId1)).thenReturn(_mailbox);
    Mockito.when(_mailbox.isClosed()).thenReturn(false);
    Object[] expRow1 = new Object[]{1, 1};
    Object[] expRow2 = new Object[]{2, 2};
    Mockito.when(_mailbox.receive())
        .thenReturn(OperatorTestUtil.block(inSchema, expRow1), OperatorTestUtil.block(inSchema, expRow2),
            TransferableBlockUtils.getEndOfStreamTransferableBlock());

    Object[] expRow3 = new Object[]{3, 3};
    JsonMailboxIdentifier expectedMailboxId2 = new JsonMailboxIdentifier(String.format("%s_%s", jobId, stageId),
        new VirtualServerAddress(server2Host, server2Port, 0), toAddress, stageId, DEFAULT_RECEIVER_STAGE_ID);
    Mockito.when(_mailboxService.getReceivingMailbox(expectedMailboxId2)).thenReturn(_mailbox2);
    Mockito.when(_mailbox2.isClosed()).thenReturn(false);
    Mockito.when(_mailbox2.receive()).thenReturn(OperatorTestUtil.block(inSchema, expRow3));
    OpChainExecutionContext context =
        new OpChainExecutionContext(_mailboxService, jobId, DEFAULT_RECEIVER_STAGE_ID, toAddress, Long.MAX_VALUE,
            Long.MAX_VALUE, new HashMap<>());

    MailboxReceiveOperator receiveOp =
        new MailboxReceiveOperator(context, ImmutableList.of(_server1, _server2), RelDistribution.Type.HASH_DISTRIBUTED,
            Collections.emptyList(), Collections.emptyList(), false, false, inSchema, stageId,
            DEFAULT_RECEIVER_STAGE_ID, null);
    // Receive first block from first server.
    TransferableBlock receivedBlock = receiveOp.nextBlock();
    List<Object[]> resultRows = receivedBlock.getContainer();
    Assert.assertEquals(resultRows.size(), 1);
    Assert.assertEquals(resultRows.get(0), expRow1);
    // Receive second block from first server.
    receivedBlock = receiveOp.nextBlock();
    resultRows = receivedBlock.getContainer();
    Assert.assertEquals(resultRows.size(), 1);
    Assert.assertEquals(resultRows.get(0), expRow2);

    // Receive from second server.
    receivedBlock = receiveOp.nextBlock();
    resultRows = receivedBlock.getContainer();
    Assert.assertEquals(resultRows.size(), 1);
    Assert.assertEquals(resultRows.get(0), expRow3);
  }

  @Test
  public void shouldGetReceptionReceiveErrorMailbox()
      throws Exception {
    String server1Host = "hash1";
    int server1Port = 123;
    Mockito.when(_server1.getHostname()).thenReturn(server1Host);
    Mockito.when(_server1.getQueryMailboxPort()).thenReturn(server1Port);
    Mockito.when(_server1.getPartitionIds()).thenReturn(Collections.singletonList(0));

    String server2Host = "hash2";
    int server2Port = 456;
    Mockito.when(_server2.getHostname()).thenReturn(server2Host);
    Mockito.when(_server2.getQueryMailboxPort()).thenReturn(server2Port);
    Mockito.when(_server2.getPartitionIds()).thenReturn(Collections.singletonList(0));

    int jobId = 456;
    int stageId = 0;
    int toPort = 8888;
    String toHost = "toHost";
    VirtualServerAddress toAddress = new VirtualServerAddress(toHost, toPort, 0);

    DataSchema inSchema = new DataSchema(new String[]{"col1", "col2"}, new DataSchema.ColumnDataType[]{INT, INT});
    JsonMailboxIdentifier expectedMailboxId1 = new JsonMailboxIdentifier(String.format("%s_%s", jobId, stageId),
        new VirtualServerAddress(server1Host, server1Port, 0), toAddress, stageId, DEFAULT_RECEIVER_STAGE_ID);
    Mockito.when(_mailboxService.getReceivingMailbox(expectedMailboxId1)).thenReturn(_mailbox);
    Mockito.when(_mailbox.isClosed()).thenReturn(false);
    Mockito.when(_mailbox.receive())
        .thenReturn(TransferableBlockUtils.getErrorTransferableBlock(new Exception("mailboxError")));

    Object[] expRow3 = new Object[]{3, 3};
    JsonMailboxIdentifier expectedMailboxId2 = new JsonMailboxIdentifier(String.format("%s_%s", jobId, stageId),
        new VirtualServerAddress(server2Host, server2Port, 0), toAddress, stageId, DEFAULT_RECEIVER_STAGE_ID);
    Mockito.when(_mailboxService.getReceivingMailbox(expectedMailboxId2)).thenReturn(_mailbox2);
    Mockito.when(_mailbox2.isClosed()).thenReturn(false);
    Mockito.when(_mailbox2.receive()).thenReturn(OperatorTestUtil.block(inSchema, expRow3));

    // Choose whether to exercise the ordering code path or not
    List<RexExpression> collationKeys = new ArrayList<>();
    List<RelFieldCollation.Direction> collationDirections = new ArrayList<>();
    boolean sortOnReceiver = false;
    if (_random.nextBoolean()) {
      collationKeys.add(new RexExpression.InputRef(0));
      collationDirections.add(RelFieldCollation.Direction.ASCENDING);
      sortOnReceiver = true;
    }

    OpChainExecutionContext context =
        new OpChainExecutionContext(_mailboxService, jobId, DEFAULT_RECEIVER_STAGE_ID, toAddress, Long.MAX_VALUE,
            Long.MAX_VALUE, new HashMap<>());

    MailboxReceiveOperator receiveOp =
        new MailboxReceiveOperator(context, ImmutableList.of(_server1, _server2), RelDistribution.Type.HASH_DISTRIBUTED,
            collationKeys, collationDirections, false, sortOnReceiver, inSchema, stageId, DEFAULT_RECEIVER_STAGE_ID,
            null);
    // Receive error block from first server.
    TransferableBlock receivedBlock = receiveOp.nextBlock();
    Assert.assertTrue(receivedBlock.isErrorBlock());
    MetadataBlock error = (MetadataBlock) receivedBlock.getDataBlock();
    Assert.assertTrue(error.getExceptions().get(QueryException.UNKNOWN_ERROR_CODE).contains("mailboxError"));
  }

  @Test
  public void shouldThrowReceiveWhenOneServerReceiveThrowException()
      throws Exception {
    String server1Host = "hash1";
    int server1Port = 123;
    Mockito.when(_server1.getHostname()).thenReturn(server1Host);
    Mockito.when(_server1.getQueryMailboxPort()).thenReturn(server1Port);
    Mockito.when(_server1.getPartitionIds()).thenReturn(Collections.singletonList(0));

    String server2Host = "hash2";
    int server2Port = 456;
    Mockito.when(_server2.getHostname()).thenReturn(server2Host);
    Mockito.when(_server2.getQueryMailboxPort()).thenReturn(server2Port);
    Mockito.when(_server2.getPartitionIds()).thenReturn(Collections.singletonList(0));

    int jobId = 456;
    int stageId = 0;
    int toPort = 8888;
    String toHost = "toHost";
    VirtualServerAddress toAddress = new VirtualServerAddress(toHost, toPort, 0);

    DataSchema inSchema = new DataSchema(new String[]{"col1", "col2"}, new DataSchema.ColumnDataType[]{INT, INT});
    JsonMailboxIdentifier expectedMailboxId1 = new JsonMailboxIdentifier(String.format("%s_%s", jobId, stageId),
        new VirtualServerAddress(server1Host, server1Port, 0), toAddress, stageId, DEFAULT_RECEIVER_STAGE_ID);
    Mockito.when(_mailboxService.getReceivingMailbox(expectedMailboxId1)).thenReturn(_mailbox);
    Mockito.when(_mailbox.isClosed()).thenReturn(false);
    Mockito.when(_mailbox.receive()).thenThrow(new Exception("mailboxError"));

    Object[] expRow3 = new Object[]{3, 3};
    JsonMailboxIdentifier expectedMailboxId2 = new JsonMailboxIdentifier(String.format("%s_%s", jobId, stageId),
        new VirtualServerAddress(server2Host, server2Port, 0), toAddress, stageId, DEFAULT_RECEIVER_STAGE_ID);
    Mockito.when(_mailboxService.getReceivingMailbox(expectedMailboxId2)).thenReturn(_mailbox2);
    Mockito.when(_mailbox2.isClosed()).thenReturn(false);
    Mockito.when(_mailbox2.receive()).thenReturn(OperatorTestUtil.block(inSchema, expRow3));

    // Choose whether to exercise the ordering code path or not
    List<RexExpression> collationKeys = new ArrayList<>();
    List<RelFieldCollation.Direction> collationDirections = new ArrayList<>();
    boolean sortOnReceiver = false;
    if (_random.nextBoolean()) {
      collationKeys.add(new RexExpression.InputRef(0));
      collationDirections.add(RelFieldCollation.Direction.ASCENDING);
      sortOnReceiver = true;
    }

    OpChainExecutionContext context =
        new OpChainExecutionContext(_mailboxService, jobId, DEFAULT_RECEIVER_STAGE_ID, toAddress, Long.MAX_VALUE,
            Long.MAX_VALUE, new HashMap<>());

    MailboxReceiveOperator receiveOp =
        new MailboxReceiveOperator(context, ImmutableList.of(_server1, _server2), RelDistribution.Type.HASH_DISTRIBUTED,
            collationKeys, collationDirections, false, sortOnReceiver, inSchema, stageId, DEFAULT_RECEIVER_STAGE_ID,
            null);
    TransferableBlock receivedBlock = receiveOp.nextBlock();
    Assert.assertTrue(receivedBlock.isErrorBlock(), "server-1 should have returned an error-block");
  }

  @Test
  public void shouldReceiveMailboxFromTwoServersWithCollationKey()
      throws Exception {
    String server1Host = "hash1";
    int server1Port = 123;
    Mockito.when(_server1.getHostname()).thenReturn(server1Host);
    Mockito.when(_server1.getQueryMailboxPort()).thenReturn(server1Port);
    Mockito.when(_server1.getPartitionIds()).thenReturn(Collections.singletonList(0));

    String server2Host = "hash2";
    int server2Port = 456;
    Mockito.when(_server2.getHostname()).thenReturn(server2Host);
    Mockito.when(_server2.getQueryMailboxPort()).thenReturn(server2Port);
    Mockito.when(_server2.getPartitionIds()).thenReturn(Collections.singletonList(0));

    int jobId = 456;
    int stageId = 0;
    int toPort = 8888;
    String toHost = "toHost";
    VirtualServerAddress toAddress = new VirtualServerAddress(toHost, toPort, 0);

    DataSchema inSchema = new DataSchema(new String[]{"col1", "col2"}, new DataSchema.ColumnDataType[]{INT, INT});
    JsonMailboxIdentifier expectedMailboxId1 = new JsonMailboxIdentifier(String.format("%s_%s", jobId, stageId),
        new VirtualServerAddress(server1Host, server1Port, 0), toAddress, stageId, DEFAULT_RECEIVER_STAGE_ID);
    Mockito.when(_mailboxService.getReceivingMailbox(expectedMailboxId1)).thenReturn(_mailbox);
    Mockito.when(_mailbox.isClosed()).thenReturn(false);
    Object[] expRow1 = new Object[]{3, 3};
    Object[] expRow2 = new Object[]{1, 1};
    Mockito.when(_mailbox.receive())
        .thenReturn(OperatorTestUtil.block(inSchema, expRow1), OperatorTestUtil.block(inSchema, expRow2),
            TransferableBlockUtils.getEndOfStreamTransferableBlock());

    Object[] expRow3 = new Object[]{4, 2};
    Object[] expRow4 = new Object[]{2, 4};
    Object[] expRow5 = new Object[]{-1, 95};
    JsonMailboxIdentifier expectedMailboxId2 = new JsonMailboxIdentifier(String.format("%s_%s", jobId, stageId),
        new VirtualServerAddress(server2Host, server2Port, 0), toAddress, stageId, DEFAULT_RECEIVER_STAGE_ID);
    Mockito.when(_mailboxService.getReceivingMailbox(expectedMailboxId2)).thenReturn(_mailbox2);
    Mockito.when(_mailbox2.isClosed()).thenReturn(false);
    Mockito.when(_mailbox2.receive()).thenReturn(OperatorTestUtil.block(inSchema, expRow3),
        OperatorTestUtil.block(inSchema, expRow4), OperatorTestUtil.block(inSchema, expRow5),
        TransferableBlockUtils.getEndOfStreamTransferableBlock());

    // Setup the collation key and direction
    List<RexExpression> collationKeys = new ArrayList<>(Collections.singletonList(new RexExpression.InputRef(0)));
    RelFieldCollation.Direction direction = _random.nextBoolean() ? RelFieldCollation.Direction.ASCENDING
        : RelFieldCollation.Direction.DESCENDING;
    List<RelFieldCollation.Direction> collationDirection = new ArrayList<>(Collections.singletonList(direction));

    OpChainExecutionContext context =
        new OpChainExecutionContext(_mailboxService, jobId, DEFAULT_RECEIVER_STAGE_ID, toAddress, Long.MAX_VALUE,
            Long.MAX_VALUE, new HashMap<>());

    MailboxReceiveOperator receiveOp = new MailboxReceiveOperator(context, ImmutableList.of(_server1, _server2),
        RelDistribution.Type.HASH_DISTRIBUTED, collationKeys, collationDirection, false, true, inSchema, stageId,
        DEFAULT_RECEIVER_STAGE_ID, null);

    // Receive a set of no-op blocks and skip over them
    TransferableBlock receivedBlock = receiveOp.nextBlock();
    while (receivedBlock.isNoOpBlock()) {
      receivedBlock = receiveOp.nextBlock();
    }
    List<Object[]> resultRows = receivedBlock.getContainer();
    // All blocks should be returned together since ordering was required
    Assert.assertEquals(resultRows.size(), 5);
    if (direction == RelFieldCollation.Direction.ASCENDING) {
      Assert.assertEquals(resultRows.get(0), expRow5);
      Assert.assertEquals(resultRows.get(1), expRow2);
      Assert.assertEquals(resultRows.get(2), expRow4);
      Assert.assertEquals(resultRows.get(3), expRow1);
      Assert.assertEquals(resultRows.get(4), expRow3);
    } else {
      Assert.assertEquals(resultRows.get(0), expRow3);
      Assert.assertEquals(resultRows.get(1), expRow1);
      Assert.assertEquals(resultRows.get(2), expRow4);
      Assert.assertEquals(resultRows.get(3), expRow2);
      Assert.assertEquals(resultRows.get(4), expRow5);
    }

    receivedBlock = receiveOp.nextBlock();
    Assert.assertTrue(receivedBlock.isEndOfStreamBlock());
  }

  @Test
  public void shouldReceiveMailboxFromTwoServersWithCollationKeyTwoColumns()
      throws Exception {
    String server1Host = "hash1";
    int server1Port = 123;
    Mockito.when(_server1.getHostname()).thenReturn(server1Host);
    Mockito.when(_server1.getQueryMailboxPort()).thenReturn(server1Port);
    Mockito.when(_server1.getPartitionIds()).thenReturn(Collections.singletonList(0));

    String server2Host = "hash2";
    int server2Port = 456;
    Mockito.when(_server2.getHostname()).thenReturn(server2Host);
    Mockito.when(_server2.getQueryMailboxPort()).thenReturn(server2Port);
    Mockito.when(_server2.getPartitionIds()).thenReturn(Collections.singletonList(0));

    int jobId = 456;
    int stageId = 0;
    int toPort = 8888;
    String toHost = "toHost";
    VirtualServerAddress toAddress = new VirtualServerAddress(toHost, toPort, 0);

    DataSchema inSchema = new DataSchema(new String[]{"col1", "col2", "col3"},
        new DataSchema.ColumnDataType[]{INT, INT, STRING});
    JsonMailboxIdentifier expectedMailboxId1 = new JsonMailboxIdentifier(String.format("%s_%s", jobId, stageId),
        new VirtualServerAddress(server1Host, server1Port, 0), toAddress, stageId, DEFAULT_RECEIVER_STAGE_ID);
    Mockito.when(_mailboxService.getReceivingMailbox(expectedMailboxId1)).thenReturn(_mailbox);
    Mockito.when(_mailbox.isClosed()).thenReturn(false);
    Object[] expRow1 = new Object[]{3, 3, "queen"};
    Object[] expRow2 = new Object[]{1, 1, "pink floyd"};
    Mockito.when(_mailbox.receive())
        .thenReturn(OperatorTestUtil.block(inSchema, expRow1), OperatorTestUtil.block(inSchema, expRow2),
            TransferableBlockUtils.getEndOfStreamTransferableBlock());

    Object[] expRow3 = new Object[]{42, 2, "pink floyd"};
    Object[] expRow4 = new Object[]{2, 4, "aerosmith"};
    Object[] expRow5 = new Object[]{-1, 95, "foo fighters"};
    JsonMailboxIdentifier expectedMailboxId2 = new JsonMailboxIdentifier(String.format("%s_%s", jobId, stageId),
        new VirtualServerAddress(server2Host, server2Port, 0), toAddress, stageId, DEFAULT_RECEIVER_STAGE_ID);
    Mockito.when(_mailboxService.getReceivingMailbox(expectedMailboxId2)).thenReturn(_mailbox2);
    Mockito.when(_mailbox2.isClosed()).thenReturn(false);
    Mockito.when(_mailbox2.receive()).thenReturn(OperatorTestUtil.block(inSchema, expRow3),
        OperatorTestUtil.block(inSchema, expRow4), OperatorTestUtil.block(inSchema, expRow5),
        TransferableBlockUtils.getEndOfStreamTransferableBlock());

    // Setup the collation key and direction
    List<RexExpression> collationKeys = new ArrayList<>(Arrays.asList(new RexExpression.InputRef(2),
        new RexExpression.InputRef(0)));
    RelFieldCollation.Direction direction1 = _random.nextBoolean() ? RelFieldCollation.Direction.ASCENDING
        : RelFieldCollation.Direction.DESCENDING;
    RelFieldCollation.Direction direction2 = RelFieldCollation.Direction.ASCENDING;
    List<RelFieldCollation.Direction> collationDirection = new ArrayList<>(Arrays.asList(direction1, direction2));

    OpChainExecutionContext context =
        new OpChainExecutionContext(_mailboxService, jobId, DEFAULT_RECEIVER_STAGE_ID, toAddress, Long.MAX_VALUE,
            Long.MAX_VALUE, new HashMap<>());

    MailboxReceiveOperator receiveOp = new MailboxReceiveOperator(context, ImmutableList.of(_server1, _server2),
        RelDistribution.Type.HASH_DISTRIBUTED, collationKeys, collationDirection, false, true, inSchema, stageId,
        DEFAULT_RECEIVER_STAGE_ID, null);

    // Receive a set of no-op blocks and skip over them
    TransferableBlock receivedBlock = receiveOp.nextBlock();
    while (receivedBlock.isNoOpBlock()) {
      receivedBlock = receiveOp.nextBlock();
    }
    List<Object[]> resultRows = receivedBlock.getContainer();
    // All blocks should be returned together since ordering was required
    Assert.assertEquals(resultRows.size(), 5);
    if (direction1 == RelFieldCollation.Direction.ASCENDING) {
      Assert.assertEquals(resultRows.get(0), expRow4);
      Assert.assertEquals(resultRows.get(1), expRow5);
      Assert.assertEquals(resultRows.get(2), expRow2);
      Assert.assertEquals(resultRows.get(3), expRow3);
      Assert.assertEquals(resultRows.get(4), expRow1);
    } else {
      Assert.assertEquals(resultRows.get(0), expRow1);
      Assert.assertEquals(resultRows.get(1), expRow2);
      Assert.assertEquals(resultRows.get(2), expRow3);
      Assert.assertEquals(resultRows.get(3), expRow5);
      Assert.assertEquals(resultRows.get(4), expRow4);
    }

    receivedBlock = receiveOp.nextBlock();
    Assert.assertTrue(receivedBlock.isEndOfStreamBlock());
  }
}
