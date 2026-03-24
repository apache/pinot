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
package org.apache.pinot.query.runtime.plan;

import java.io.IOException;
import java.util.List;
import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.query.runtime.operator.AggregateOperator;
import org.apache.pinot.query.runtime.operator.BaseMailboxReceiveOperator;
import org.apache.pinot.query.runtime.operator.HashJoinOperator;
import org.apache.pinot.query.runtime.operator.LeafOperator;
import org.apache.pinot.query.runtime.operator.MailboxSendOperator;
import org.apache.pinot.query.runtime.operator.MultiStageOperator;
import org.apache.pinot.query.runtime.operator.SortOperator;
import org.apache.pinot.query.runtime.operator.WindowAggregateOperator;
import org.apache.pinot.segment.spi.memory.DataBuffer;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class MultiStageQueryStatsTest {

  /**
   * A test that verifies calling {@link MultiStageQueryStats#mergeUpstream(MultiStageQueryStats)} is similar to call
   * {@link MultiStageQueryStats#serialize()} and then {@link MultiStageQueryStats#mergeUpstream(List)}.
   */
  @Test(dataProvider = "stats")
  public void testMergeEquivalence(MultiStageQueryStats stats)
      throws IOException {

    assert stats.getCurrentStageId() > 0 : "Stage id should be greater than 0 in order to run this test";

    MultiStageQueryStats mergingHeap = MultiStageQueryStats.emptyStats(0);
    mergingHeap.mergeUpstream(stats);

    List<DataBuffer> buffers = stats.serialize();
    MultiStageQueryStats rootStats = MultiStageQueryStats.emptyStats(0);
    rootStats.mergeUpstream(buffers);

    Assert.assertEquals(mergingHeap, rootStats, "Merging objects should be equal to merging serialized buffers");
  }

  @DataProvider(name = "stats")
  public static MultiStageQueryStats[] stats() {
    return new MultiStageQueryStats[] {
        stats1(),
        stats2()
    };
  }

  public static MultiStageQueryStats stats1() {
    return new MultiStageQueryStats.Builder(1)
        .customizeOpen(open ->
          open.addLastOperator(MultiStageOperator.Type.MAILBOX_RECEIVE,
                  new StatMap<>(BaseMailboxReceiveOperator.StatKey.class)
                      .merge(BaseMailboxReceiveOperator.StatKey.EXECUTION_TIME_MS, 100)
                      .merge(BaseMailboxReceiveOperator.StatKey.EMITTED_ROWS, 10))
              .addLastOperator(MultiStageOperator.Type.SORT_OR_LIMIT,
                  new StatMap<>(SortOperator.StatKey.class)
                      .merge(SortOperator.StatKey.EXECUTION_TIME_MS, 10)
                      .merge(SortOperator.StatKey.EMITTED_ROWS, 10))
              .addLastOperator(MultiStageOperator.Type.MAILBOX_SEND,
                  new StatMap<>(MailboxSendOperator.StatKey.class)
                      .merge(MailboxSendOperator.StatKey.STAGE, 1)
                      .merge(MailboxSendOperator.StatKey.EXECUTION_TIME_MS, 100)
                      .merge(MailboxSendOperator.StatKey.EMITTED_ROWS, 10))
        )
        .addLast(stageStats ->
            stageStats.addLastOperator(MultiStageOperator.Type.LEAF,
                    new StatMap<>(LeafOperator.StatKey.class)
                        .merge(LeafOperator.StatKey.NUM_SEGMENTS_QUERIED, 1)
                        .merge(LeafOperator.StatKey.NUM_SEGMENTS_PROCESSED, 1)
                        .merge(LeafOperator.StatKey.NUM_SEGMENTS_MATCHED, 1)
                        .merge(LeafOperator.StatKey.NUM_DOCS_SCANNED, 10)
                        .merge(LeafOperator.StatKey.NUM_ENTRIES_SCANNED_POST_FILTER, 5)
                        .merge(LeafOperator.StatKey.TOTAL_DOCS, 5)
                        .merge(LeafOperator.StatKey.EXECUTION_TIME_MS, 95)
                        .merge(LeafOperator.StatKey.TABLE, "a"))
                .addLastOperator(MultiStageOperator.Type.MAILBOX_SEND,
                    new StatMap<>(MailboxSendOperator.StatKey.class)
                        .merge(MailboxSendOperator.StatKey.STAGE, 2)
                        .merge(MailboxSendOperator.StatKey.EXECUTION_TIME_MS, 135)
                        .merge(MailboxSendOperator.StatKey.EMITTED_ROWS, 5))
                .close())
        .build();
  }

  public static MultiStageQueryStats stats2() {
    return new MultiStageQueryStats.Builder(1)
        .customizeOpen(open ->
            open.addLastOperator(MultiStageOperator.Type.MAILBOX_RECEIVE,
                    new StatMap<>(BaseMailboxReceiveOperator.StatKey.class)
                        .merge(BaseMailboxReceiveOperator.StatKey.EXECUTION_TIME_MS, 50)
                        .merge(BaseMailboxReceiveOperator.StatKey.EMITTED_ROWS, 20))
                .addLastOperator(MultiStageOperator.Type.HASH_JOIN,
                    new StatMap<>(HashJoinOperator.StatKey.class)
                        .merge(HashJoinOperator.StatKey.EXECUTION_TIME_MS, 30)
                        .merge(HashJoinOperator.StatKey.EMITTED_ROWS, 15)
                        .merge(HashJoinOperator.StatKey.MAX_ROWS_IN_JOIN, 100L))
                .addLastOperator(MultiStageOperator.Type.WINDOW,
                    new StatMap<>(WindowAggregateOperator.StatKey.class)
                        .merge(WindowAggregateOperator.StatKey.EXECUTION_TIME_MS, 20)
                        .merge(WindowAggregateOperator.StatKey.EMITTED_ROWS, 15)
                        .merge(WindowAggregateOperator.StatKey.MAX_ROWS_IN_WINDOW, 50L))
                .addLastOperator(MultiStageOperator.Type.MAILBOX_SEND,
                    new StatMap<>(MailboxSendOperator.StatKey.class)
                        .merge(MailboxSendOperator.StatKey.STAGE, 1)
                        .merge(MailboxSendOperator.StatKey.EXECUTION_TIME_MS, 40)
                        .merge(MailboxSendOperator.StatKey.EMITTED_ROWS, 15))
        )
        .addLast(stageStats ->
            stageStats.addLastOperator(MultiStageOperator.Type.LEAF,
                    new StatMap<>(LeafOperator.StatKey.class)
                        .merge(LeafOperator.StatKey.EXECUTION_TIME_MS, 80)
                        .merge(LeafOperator.StatKey.EMITTED_ROWS, 30))
                .addLastOperator(MultiStageOperator.Type.AGGREGATE,
                    new StatMap<>(AggregateOperator.StatKey.class)
                        .merge(AggregateOperator.StatKey.EXECUTION_TIME_MS, 25)
                        .merge(AggregateOperator.StatKey.EMITTED_ROWS, 10)
                        .merge(AggregateOperator.StatKey.NUM_GROUPS, 5L))
                .addLastOperator(MultiStageOperator.Type.MAILBOX_SEND,
                    new StatMap<>(MailboxSendOperator.StatKey.class)
                        .merge(MailboxSendOperator.StatKey.STAGE, 2)
                        .merge(MailboxSendOperator.StatKey.EXECUTION_TIME_MS, 60)
                        .merge(MailboxSendOperator.StatKey.EMITTED_ROWS, 10))
                .close())
        .build();
  }
}
