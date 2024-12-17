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
package org.apache.pinot.spi.stream;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import org.apache.pinot.spi.utils.IngestionConfigUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Fetches the list of {@link PartitionGroupMetadata} for all partition groups of the streams,
 * using the {@link StreamMetadataProvider}
 */
public class PartitionGroupMetadataFetcher implements Callable<Boolean> {

  private static final Logger LOGGER = LoggerFactory.getLogger(PartitionGroupMetadataFetcher.class);

  private final List<PartitionGroupMetadata> _newPartitionGroupMetadataList;
  private final List<StreamConfig> _streamConfigs;
  private final List<PartitionGroupConsumptionStatus> _partitionGroupConsumptionStatusList;
  private Exception _exception;
  private final List<String> _topicNames;

  public PartitionGroupMetadataFetcher(List<StreamConfig> streamConfigs,
      List<PartitionGroupConsumptionStatus> partitionGroupConsumptionStatusList) {
    _topicNames = streamConfigs.stream().map(StreamConfig::getTopicName).collect(Collectors.toList());
    _streamConfigs = streamConfigs;
    _partitionGroupConsumptionStatusList = partitionGroupConsumptionStatusList;
    _newPartitionGroupMetadataList = new ArrayList<>();
  }

  public List<PartitionGroupMetadata> getPartitionGroupMetadataList() {
    return _newPartitionGroupMetadataList;
  }

  public Exception getException() {
    return _exception;
  }

  /**
   * Callable to fetch the {@link PartitionGroupMetadata} list, from the stream.
   * The stream requires the list of {@link PartitionGroupConsumptionStatus} to compute the new
   * {@link PartitionGroupMetadata}
   */
  @Override
  public Boolean call()
      throws Exception {
    _newPartitionGroupMetadataList.clear();
    for (int i = 0; i < _streamConfigs.size(); i++) {
      String clientId = PartitionGroupMetadataFetcher.class.getSimpleName() + "-"
          + _streamConfigs.get(i).getTableNameWithType() + "-" + _topicNames.get(i);
      StreamConsumerFactory streamConsumerFactory = StreamConsumerFactoryProvider.create(_streamConfigs.get(i));
      final int index = i;
      List<PartitionGroupConsumptionStatus> topicPartitionGroupConsumptionStatusList =
          _partitionGroupConsumptionStatusList.stream()
              .filter(partitionGroupConsumptionStatus ->
                  IngestionConfigUtils.getStreamConfigIndexFromPinotPartitionId(
                      partitionGroupConsumptionStatus.getPartitionGroupId()) == index)
              .collect(Collectors.toList());
      try (
          StreamMetadataProvider streamMetadataProvider =
              streamConsumerFactory.createStreamMetadataProvider(clientId)) {
        _newPartitionGroupMetadataList.addAll(streamMetadataProvider.computePartitionGroupMetadata(clientId,
            _streamConfigs.get(i),
            topicPartitionGroupConsumptionStatusList, /*maxWaitTimeMs=*/15000).stream().map(
            metadata -> new PartitionGroupMetadata(
                IngestionConfigUtils.getPinotPartitionIdFromStreamPartitionId(
                    metadata.getPartitionGroupId(), index),
                metadata.getStartOffset())).collect(Collectors.toList())
        );
        if (_exception != null) {
          // We had at least one failure, but succeeded now. Log an info
          LOGGER.info("Successfully retrieved PartitionGroupMetadata for topic {}", _topicNames.get(i));
        }
      } catch (TransientConsumerException e) {
        LOGGER.warn("Transient Exception: Could not get partition count for topic {}", _topicNames.get(i), e);
        _exception = e;
        return Boolean.FALSE;
      } catch (Exception e) {
        LOGGER.warn("Could not get partition count for topic {}", _topicNames.get(i), e);
        _exception = e;
        throw e;
      }
    }
    return Boolean.TRUE;
  }
}
