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
package org.apache.pinot.integration.tests;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.helix.HelixAdmin;
import org.apache.helix.model.IdealState;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.plugin.stream.kafka.KafkaMessageBatch;
import org.apache.pinot.plugin.stream.kafka30.KafkaConsumerFactory;
import org.apache.pinot.plugin.stream.kafka30.KafkaPartitionLevelConsumer;
import org.apache.pinot.spi.stream.PartitionGroupConsumer;
import org.apache.pinot.spi.stream.PartitionGroupConsumptionStatus;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamConfigProperties;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;

import static org.testng.Assert.assertTrue;


/**
 * Integration test for low-level Kafka3 consumer.
 */
public class LLCRealtimeKafka3ClusterIntegrationTest extends LLCRealtimeClusterIntegrationTest {

  @Override
  protected Map<String, String> getStreamConfigMap() {
    Map<String, String> streamConfigMap = super.getStreamConfigMap();
    streamConfigMap.put(StreamConfigProperties.constructStreamProperty(
        streamConfigMap.get(StreamConfigProperties.STREAM_TYPE),
        StreamConfigProperties.STREAM_CONSUMER_FACTORY_CLASS), ExceptingKafka3ConsumerFactory.class.getName());
    ExceptingKafka3ConsumerFactory.init(getHelixClusterName(), _helixAdmin, getTableName());
    return streamConfigMap;
  }

  public static class ExceptingKafka3ConsumerFactory extends KafkaConsumerFactory {

    public static final int PARTITION_FOR_EXCEPTIONS = 1; // Setting this to -1 disables all exceptions thrown.
    public static final int SEQ_NUM_FOR_CREATE_EXCEPTION = 1;
    public static final int SEQ_NUM_FOR_CONSUME_EXCEPTION = 3;

    private static HelixAdmin _helixAdmin;
    private static String _helixClusterName;
    private static String _tableName;
    public ExceptingKafka3ConsumerFactory() {
      super();
    }

    public static void init(String helixClusterName, HelixAdmin helixAdmin, String tableName) {
      _helixAdmin = helixAdmin;
      _helixClusterName = helixClusterName;
      _tableName = tableName;
    }

    @Override
    public PartitionGroupConsumer createPartitionGroupConsumer(String clientId,
        PartitionGroupConsumptionStatus partitionGroupConsumptionStatus) {
      /*
       * The segment data manager is creating a consumer to consume rows into a segment.
       * Check the partition and sequence number of the segment and decide whether it
       * qualifies for:
       * - Throwing exception during create OR
       * - Throwing exception during consumption.
       * Make sure that this still works if retries are added in RealtimeSegmentDataManager
       */
      int partition = partitionGroupConsumptionStatus.getPartitionGroupId();
      boolean exceptionDuringConsume = false;
      int seqNum = getSegmentSeqNum(partition);
      if (partition == PARTITION_FOR_EXCEPTIONS) {
        if (seqNum == SEQ_NUM_FOR_CREATE_EXCEPTION) {
          throw new RuntimeException("TestException during consumer creation");
        } else if (seqNum == SEQ_NUM_FOR_CONSUME_EXCEPTION) {
          exceptionDuringConsume = true;
        }
      }
      return new ExceptingKafka3Consumer(clientId, _streamConfig, partition, exceptionDuringConsume);
    }

    private int getSegmentSeqNum(int partition) {
      IdealState is = _helixAdmin.getResourceIdealState(_helixClusterName,
          TableNameBuilder.REALTIME.tableNameWithType(_tableName));
      AtomicInteger seqNum = new AtomicInteger(-1);
      is.getPartitionSet().forEach(segmentNameStr -> {
        if (LLCSegmentName.isLLCSegment(segmentNameStr)) {
          if (is.getInstanceStateMap(segmentNameStr).values().contains(
              CommonConstants.Helix.StateModel.SegmentStateModel.CONSUMING)) {
            LLCSegmentName segmentName = new LLCSegmentName(segmentNameStr);
            if (segmentName.getPartitionGroupId() == partition) {
              seqNum.set(segmentName.getSequenceNumber());
            }
          }
        }
      });
      assertTrue(seqNum.get() >= 0, "No consuming segment found in partition: " + partition);
      return seqNum.get();
    }

    public static class ExceptingKafka3Consumer extends KafkaPartitionLevelConsumer {
      private final boolean _exceptionDuringConsume;

      public ExceptingKafka3Consumer(String clientId, StreamConfig streamConfig, int partition,
          boolean exceptionDuringConsume) {
        super(clientId, streamConfig, partition);
        _exceptionDuringConsume = exceptionDuringConsume;
      }

      @Override
      public KafkaMessageBatch fetchMessages(StreamPartitionMsgOffset startOffset, int timeoutMs) {
        if (_exceptionDuringConsume) {
          throw new RuntimeException("TestException during consumption");
        }
        return super.fetchMessages(startOffset, timeoutMs);
      }
    }
  }
}
