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

import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.core.util.DataBlockExtractUtils;
import org.apache.pinot.query.mailbox.ReceivingMailbox;
import org.apache.pinot.query.planner.plannode.MailboxReceiveNode;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.apache.pinot.query.runtime.blocks.SerializedDataBlock;
import org.apache.pinot.query.runtime.operator.utils.AsyncStream;
import org.apache.pinot.query.runtime.operator.utils.SortUtils;
import org.apache.pinot.query.runtime.plan.OpChainExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/// This `MailboxReceiveOperator` receives data from a [org.apache.pinot.query.mailbox.ReceivingMailbox] and
/// serve it out from the [#nextBlock()] API.
public class MailboxReceiveOperator extends BaseMailboxReceiveOperator {
  private static final Logger LOGGER = LoggerFactory.getLogger(MailboxReceiveOperator.class);
  private static final String EXPLAIN_NAME = "MAILBOX_RECEIVE";
  private static final int AUTO_SAMPLE_COMPARISONS = 64;
  private static final int AUTO_DISORDERED_INVERSIONS = 16;
  private static final int AUTO_MAX_DISORDERED_INVERSIONS = 48;
  private static final int AUTO_MAX_EQUAL_PAIRS = 4;

  @Nullable
  private final SortUtils.SortComparator _autoComparator;
  private Map<AsyncStream<ReceivingMailbox.MseBlockWithStats>, StreamSample> _autoSamples;
  private boolean _autoProfilingDisabled;

  public MailboxReceiveOperator(OpChainExecutionContext context, MailboxReceiveNode node) {
    super(context, node);
    _autoComparator = node.isAutoProfile() ? new SortUtils.SortComparator(node.getCollations(), false) : null;
    _autoSamples = _autoComparator != null ? new IdentityHashMap<>() : Map.of();
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
  protected MseBlock getNextBlock() {
    try {
      return readNextBlock();
    } catch (RuntimeException e) {
      releaseBuffers();
      throw e;
    }
  }

  private MseBlock readNextBlock() {
    MseBlock block = _multiConsumer.readMseBlockBlocking();
    // When early termination flag is set, caller is expecting an EOS block to be returned, however since the 2 stages
    // between sending/receiving mailbox are setting early termination flag asynchronously, there's chances that the
    // next block pulled out of the ReceivingMailbox to be an already buffered normal data block. This requires the
    // MailboxReceiveOperator to continue pulling and dropping data block until an EOS block is observed.
    while (_isEarlyTerminated && block.isData()) {
      block = _multiConsumer.readMseBlockBlocking();
    }
    if (block.isData()) {
      if (_autoComparator != null && !_autoProfilingDisabled) {
        try {
          sampleSenderOrder((MseBlock.Data) block);
        } catch (RuntimeException e) {
          // AUTO evidence must never turn a successful receiver-sort query into a failed one.
          releaseBuffers();
          LOGGER.debug("Disabling window AUTO order profiling for this receiver", e);
        }
      }
      checkTerminationAndSampleUsage();
    } else {
      releaseBuffers();
      onEos();
    }
    return block;
  }

  /// Profiles the first 64 adjacent pairs from each sender independently. This is diagnostic only: the original
  /// block and its ordering are passed through unchanged. Serialized blocks decode no more than the sampled prefix.
  private void sampleSenderOrder(MseBlock.Data block) {
    AsyncStream<ReceivingMailbox.MseBlockWithStats> stream = _multiConsumer.getLastReadStream();
    if (stream == null) {
      return;
    }
    StreamSample sample = _autoSamples.computeIfAbsent(stream, ignored -> new StreamSample());
    int remainingRows = AUTO_SAMPLE_COMPARISONS + 1 - sample._sampledRows;
    if (remainingRows == 0) {
      return;
    }
    List<Object[]> rows;
    if (block.isRowHeap()) {
      rows = block.asRowHeap().getRows();
    } else if (block instanceof SerializedDataBlock) {
      rows = DataBlockExtractUtils.extractRows(((SerializedDataBlock) block).getDataBlock(), remainingRows);
    } else {
      return;
    }
    int numSampled = Math.min(rows.size(), remainingRows);
    for (int i = 0; i < numSampled; i++) {
      Object[] row = rows.get(i);
      if (sample._previousRow != null) {
        int comparison = _autoComparator.compare(sample._previousRow, row);
        if (comparison > 0) {
          sample._inversions++;
        } else if (comparison == 0) {
          sample._equalPairs++;
        }
      }
      sample._previousRow = row;
      sample._sampledRows++;
    }
    _statMap.merge(StatKey.AUTO_SAMPLED_ROWS, numSampled);
    if (sample._sampledRows == AUTO_SAMPLE_COMPARISONS + 1) {
      _statMap.merge(StatKey.AUTO_SAMPLE_STREAMS, 1);
      if (sample._inversions >= AUTO_DISORDERED_INVERSIONS
          && sample._inversions <= AUTO_MAX_DISORDERED_INVERSIONS
          && sample._equalPairs <= AUTO_MAX_EQUAL_PAIRS) {
        _statMap.merge(StatKey.AUTO_CANDIDATE_STREAMS, 1);
      }
      sample._previousRow = null;
    }
  }

  @Override
  protected void releaseBuffers() {
    _autoProfilingDisabled = true;
    _autoSamples = Map.of();
  }

  @Override
  protected boolean hasBufferedState() {
    return !_autoSamples.isEmpty();
  }

  private static final class StreamSample {
    private int _sampledRows;
    private int _inversions;
    private int _equalPairs;
    @Nullable
    private Object[] _previousRow;
  }
}
