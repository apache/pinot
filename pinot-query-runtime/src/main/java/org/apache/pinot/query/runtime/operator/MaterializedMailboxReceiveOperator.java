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
import java.util.Iterator;
import java.util.List;
import org.apache.calcite.rel.RelDistribution;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.query.mailbox.MailboxService;
import org.apache.pinot.query.planner.plannode.MailboxReceiveNode;
import org.apache.pinot.query.routing.MailboxInfo;
import org.apache.pinot.query.routing.MailboxInfos;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.blocks.SuccessMseBlock;
import org.apache.pinot.query.runtime.plan.MultiStageQueryStats;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// Reads one logical materialized HASH partition from every worker of the completed producer stage.
///
/// The static materialized-exchange phase keeps the existing worker assignment, so consumer worker `p` owns logical
/// partition `p`. Producer identity remains independent of the consumer, which lets later phases replace this
/// implicit binding with broker-provided handles. This class is not thread-safe.
public final class MaterializedMailboxReceiveOperator extends MultiStageOperator {
  private static final Logger LOGGER = LoggerFactory.getLogger(MaterializedMailboxReceiveOperator.class);
  private static final String EXPLAIN_NAME = "MATERIALIZED_MAILBOX_RECEIVE";

  private final MailboxService _mailboxService;
  private final int _senderStageId;
  private final List<PartitionSource> _sources;
  private final StatMap<BaseMailboxReceiveOperator.StatKey> _statMap =
      new StatMap<>(BaseMailboxReceiveOperator.StatKey.class);

  private int _sourceIndex;
  private Iterator<MseBlock.Data> _current = List.<MseBlock.Data>of().iterator();

  public MaterializedMailboxReceiveOperator(OpChainExecutionContext context, MailboxReceiveNode node) {
    super(context);
    Preconditions.checkState(node.getDistributionType() == RelDistribution.Type.HASH_DISTRIBUTED,
        "Materialized exchange only supports HASH_DISTRIBUTED, got: %s", node.getDistributionType());
    _mailboxService = context.getMailboxService();
    _senderStageId = node.getSenderStageId();
    _sources = getSources(context, _senderStageId);
    _statMap.merge(BaseMailboxReceiveOperator.StatKey.FAN_IN, _sources.size());
  }

  @Override
  public Type getOperatorType() {
    return Type.MAILBOX_RECEIVE;
  }

  @Override
  public void registerExecution(long time, int numRows, long memoryUsedBytes, long gcTimeMs) {
    _statMap.merge(BaseMailboxReceiveOperator.StatKey.EXECUTION_TIME_MS, time);
    _statMap.merge(BaseMailboxReceiveOperator.StatKey.EMITTED_ROWS, numRows);
    _statMap.merge(BaseMailboxReceiveOperator.StatKey.ALLOCATED_MEMORY_BYTES, memoryUsedBytes);
    _statMap.merge(BaseMailboxReceiveOperator.StatKey.GC_TIME_MS, gcTimeMs);
  }

  @Override
  public List<MultiStageOperator> getChildOperators() {
    return List.of();
  }

  @Override
  public StatMap<BaseMailboxReceiveOperator.StatKey> copyStatMaps() {
    StatMap<BaseMailboxReceiveOperator.StatKey> statMap = new StatMap<>(_statMap);
    if (statMap.getLong(BaseMailboxReceiveOperator.StatKey.EMITTED_ROWS) == 0) {
      statMap.merge(BaseMailboxReceiveOperator.StatKey.NON_ACTIVE_WORKERS, 1);
    }
    return statMap;
  }

  @Override
  public MultiStageQueryStats calculateUpstreamStats() {
    return MultiStageQueryStats.emptyStats(_context.getStageId());
  }

  @Override
  public String toExplainString() {
    return EXPLAIN_NAME;
  }

  @Override
  protected Logger logger() {
    return LOGGER;
  }

  @Override
  protected long getDeadlineMs() {
    return _context.getPassiveDeadlineMs();
  }

  @Override
  protected MseBlock getNextBlock() {
    while (true) {
      if (_current.hasNext()) {
        MseBlock.Data block = _current.next();
        if (!_isEarlyTerminated) {
          checkTerminationAndSampleUsage();
          return block;
        }
        continue;
      }
      if (_sourceIndex == _sources.size()) {
        return SuccessMseBlock.INSTANCE;
      }
      PartitionSource source = _sources.get(_sourceIndex++);
      _current = _mailboxService.readMaterializedPartition(source._hostname, source._port, _context.getRequestId(),
          _senderStageId, source._producerWorkerId, _context.getWorkerId(), _context.getPassiveDeadlineMs());
    }
  }

  private static List<PartitionSource> getSources(OpChainExecutionContext context, int senderStageId) {
    MailboxInfos mailboxInfos = context.getWorkerMetadata().getMailboxInfosMap().get(senderStageId);
    if (mailboxInfos == null) {
      return List.of();
    }
    List<PartitionSource> sources = new ArrayList<>();
    for (MailboxInfo mailboxInfo : mailboxInfos.getMailboxInfos()) {
      for (int producerWorkerId : mailboxInfo.getWorkerIds()) {
        sources.add(new PartitionSource(mailboxInfo.getHostname(), mailboxInfo.getPort(), producerWorkerId));
      }
    }
    return sources;
  }

  private static final class PartitionSource {
    private final String _hostname;
    private final int _port;
    private final int _producerWorkerId;

    private PartitionSource(String hostname, int port, int producerWorkerId) {
      _hostname = hostname;
      _port = port;
      _producerWorkerId = producerWorkerId;
    }
  }
}
