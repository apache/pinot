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

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.calcite.rel.RelDistribution;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.mailbox.ReceivingMailbox;
import org.apache.pinot.query.planner.physical.MailboxIdUtils;
import org.apache.pinot.query.planner.plannode.MailboxReceiveNode;
import org.apache.pinot.query.routing.MailboxInfos;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.operator.utils.AsyncStream;
import org.apache.pinot.query.runtime.operator.utils.BlockingMultiStreamConsumer;
import org.apache.pinot.query.runtime.plan.MultiStageQueryStats;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;


/**
 * Base class to be used by the various MailboxReceiveOperators such as the sorted and non-sorted versions. This
 * class contains the common logic needed for MailboxReceive
 *
 * BaseMailboxReceiveOperator receives mailbox from mailboxService from sendingStageInstances.
 * We use sendingStageInstance to deduce mailboxId and fetch the content from mailboxService.
 * When exchangeType is Singleton, we find the mapping mailbox for the mailboxService. If not found, use empty list.
 * When exchangeType is non-Singleton, we pull from each instance in round-robin way to get matched mailbox content.
 */
public abstract class BaseMailboxReceiveOperator extends MultiStageOperator {
  protected final MailboxService _mailboxService;
  protected final RelDistribution.Type _distributionType;
  protected final List<String> _mailboxIds;
  protected final BlockingMultiStreamConsumer.OfMseBlock _multiConsumer;
  protected final List<StatMap<ReceivingMailbox.StatKey>> _receivingStats;
  protected final StatMap<StatKey> _statMap = new StatMap<>(StatKey.class);

  public BaseMailboxReceiveOperator(OpChainExecutionContext context, MailboxReceiveNode node) {
    super(context);
    _mailboxService = context.getMailboxService();
    RelDistribution.Type distributionType = node.getDistributionType();
    Preconditions.checkState(MailboxSendOperator.SUPPORTED_EXCHANGE_TYPES.contains(distributionType),
        "Unsupported exchange type: %s", distributionType);
    _distributionType = distributionType;

    long requestId = context.getRequestId();
    int senderStageId = node.getSenderStageId();
    MailboxInfos mailboxInfos = context.getWorkerMetadata().getMailboxInfosMap().get(senderStageId);
    if (mailboxInfos != null) {
      _mailboxIds =
          MailboxIdUtils.toMailboxIds(requestId, senderStageId, mailboxInfos.getMailboxInfos(), context.getStageId(),
              context.getWorkerId());
      int numMailboxes = _mailboxIds.size();
      List<ReadMailboxAsyncStream> asyncStreams = new ArrayList<>(numMailboxes);
      _receivingStats = new ArrayList<>(numMailboxes);
      for (String mailboxId : _mailboxIds) {
        ReadMailboxAsyncStream asyncStream =
            new ReadMailboxAsyncStream(_mailboxService.getReceivingMailbox(mailboxId), this);
        asyncStreams.add(asyncStream);
        _receivingStats.add(asyncStream._mailbox.getStatMap());
      }
      _multiConsumer = new BlockingMultiStreamConsumer.OfMseBlock(context, asyncStreams);
    } else {
      // TODO: Revisit if we should throw exception here.
      _mailboxIds = List.of();
      _receivingStats = List.of();
      _multiConsumer = new BlockingMultiStreamConsumer.OfMseBlock(context, List.of());
    }
    _statMap.merge(StatKey.FAN_IN, _mailboxIds.size());
  }

  @Override
  protected void earlyTerminate() {
    _isEarlyTerminated = true;
    _multiConsumer.earlyTerminate();
  }

  @Override
  public Type getOperatorType() {
    return Type.MAILBOX_RECEIVE;
  }

  @Override
  public List<MultiStageOperator> getChildOperators() {
    return List.of();
  }

  @Override
  public void close() {
    super.close();
    _multiConsumer.close();
  }

  @Override
  public void cancel(Throwable t) {
    super.cancel(t);
    _multiConsumer.cancel(t);
  }

  @Override
  public MultiStageQueryStats calculateUpstreamStats() {
    return _multiConsumer.calculateStats();
  }

  @Override
  protected StatMap<?> copyStatMaps() {
    return new StatMap<>(_statMap);
  }

  protected void onEos() {
    for (StatMap<ReceivingMailbox.StatKey> receivingStats : _receivingStats) {
      addReceivingStats(receivingStats);
    }
  }

  @Override
  public void registerExecution(long time, int numRows) {
    _statMap.merge(StatKey.EXECUTION_TIME_MS, time);
    _statMap.merge(StatKey.EMITTED_ROWS, numRows);
  }

  private void addReceivingStats(StatMap<ReceivingMailbox.StatKey> from) {
    _statMap.merge(StatKey.RAW_MESSAGES, from.getInt(ReceivingMailbox.StatKey.DESERIALIZED_MESSAGES));
    _statMap.merge(StatKey.DESERIALIZED_BYTES, from.getLong(ReceivingMailbox.StatKey.DESERIALIZED_BYTES));
    _statMap.merge(StatKey.DESERIALIZATION_TIME_MS, from.getLong(ReceivingMailbox.StatKey.DESERIALIZATION_TIME_MS));
    _statMap.merge(StatKey.IN_MEMORY_MESSAGES, from.getInt(ReceivingMailbox.StatKey.IN_MEMORY_MESSAGES));
    _statMap.merge(StatKey.DOWNSTREAM_WAIT_MS, from.getLong(ReceivingMailbox.StatKey.OFFER_CPU_TIME_MS));
    _statMap.merge(StatKey.UPSTREAM_WAIT_MS, from.getLong(ReceivingMailbox.StatKey.WAIT_CPU_TIME_MS));
  }

  private static class ReadMailboxAsyncStream implements AsyncStream<ReceivingMailbox.MseBlockWithStats> {
    final ReceivingMailbox _mailbox;
    final BaseMailboxReceiveOperator _operator;

    ReadMailboxAsyncStream(ReceivingMailbox mailbox, BaseMailboxReceiveOperator operator) {
      _mailbox = mailbox;
      _operator = operator;
    }

    @Override
    public Object getId() {
      return _mailbox.getId();
    }

    @Nullable
    @Override
    public ReceivingMailbox.MseBlockWithStats poll() {
      ReceivingMailbox.MseBlockWithStats blockWithStats = _mailbox.poll();

      if (blockWithStats != null) {
        MseBlock block = blockWithStats.getBlock();

        // TODO: Check if we should also release mailbox on not successful EOS.
        if (block.isSuccess()) {
          _operator._mailboxService.releaseReceivingMailbox(_mailbox);
        }
      }
      return blockWithStats;
    }

    @Override
    public void addOnNewDataListener(OnNewData onNewData) {
      _mailbox.registeredReader(onNewData::newDataAvailable);
    }

    @Override
    public void earlyTerminate() {
      _mailbox.earlyTerminate();
    }

    @Override
    public void cancel() {
      _mailbox.cancel();
    }
  }

  public enum StatKey implements StatMap.Key {
    //@formatter:off
    EXECUTION_TIME_MS(StatMap.Type.LONG) {
      @Override
      public boolean includeDefaultInJson() {
        return true;
      }
    },
    EMITTED_ROWS(StatMap.Type.LONG) {
      @Override
      public boolean includeDefaultInJson() {
        return true;
      }
    },
    /**
     * How many send mailboxes are being read by this receive operator.
     * <p>
     * Clock time will be proportional to this number and the parallelism of the stage.
     */
    FAN_IN(StatMap.Type.INT) {
      @Override
      public int merge(int value1, int value2) {
        return Math.max(value1, value2);
      }
    },
    /**
     * How many messages have been received in heap format by this mailbox.
     * <p>
     * The lower the relation between RAW_MESSAGES and IN_MEMORY_MESSAGES, the more efficient the exchange is.
     */
    IN_MEMORY_MESSAGES(StatMap.Type.INT),
    /**
     * How many messages have been received in raw format and therefore deserialized by this mailbox.
     * <p>
     * The higher the relation between RAW_MESSAGES and IN_MEMORY_MESSAGES, the less efficient the exchange is.
     */
    RAW_MESSAGES(StatMap.Type.INT),
    /**
     * How many bytes have been deserialized by this mailbox.
     * <p>
     * A high number here indicates that the mailbox is receiving a lot of data from other servers.
     */
    DESERIALIZED_BYTES(StatMap.Type.LONG),
    /**
     * How long (in CPU time) it took to deserialize the raw messages received by this mailbox.
     */
    DESERIALIZATION_TIME_MS(StatMap.Type.LONG),
    /**
     * How long (in CPU time) it took to offer the messages to downstream operator.
     */
    DOWNSTREAM_WAIT_MS(StatMap.Type.LONG),
    /**
     * How long (in CPU time) it took to wait for the messages to be offered to downstream operator.
     */
    UPSTREAM_WAIT_MS(StatMap.Type.LONG);
    //@formatter:on

    private final StatMap.Type _type;

    StatKey(StatMap.Type type) {
      _type = type;
    }

    @Override
    public StatMap.Type getType() {
      return _type;
    }
  }
}
