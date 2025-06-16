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

  private final List<StreamConfig> _streamConfigs;
  private final List<PartitionGroupConsumptionStatus> _partitionGroupConsumptionStatusList;
  private final boolean _forceGetOffsetFromStream;
  private final List<PartitionGroupMetadata> _newPartitionGroupMetadataList = new ArrayList<>();

  private Exception _exception;

  public PartitionGroupMetadataFetcher(List<StreamConfig> streamConfigs,
      List<PartitionGroupConsumptionStatus> partitionGroupConsumptionStatusList, boolean forceGetOffsetFromStream) {
    _streamConfigs = streamConfigs;
    _partitionGroupConsumptionStatusList = partitionGroupConsumptionStatusList;
    _forceGetOffsetFromStream = forceGetOffsetFromStream;
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
    return _streamConfigs.size() == 1 ? fetchSingleStream() : fetchMultipleStreams();
  }

  private Boolean fetchSingleStream()
      throws Exception {
    StreamConfig streamConfig = _streamConfigs.get(0);
    String topicName = streamConfig.getTopicName();
    String clientId =
        PartitionGroupMetadataFetcher.class.getSimpleName() + "-" + streamConfig.getTableNameWithType() + "-"
            + topicName;
    StreamConsumerFactory streamConsumerFactory = StreamConsumerFactoryProvider.create(streamConfig);
    try (StreamMetadataProvider streamMetadataProvider = streamConsumerFactory.createStreamMetadataProvider(
        StreamConsumerFactory.getUniqueClientId(clientId))) {
      _newPartitionGroupMetadataList.addAll(streamMetadataProvider.computePartitionGroupMetadata(clientId, streamConfig,
          _partitionGroupConsumptionStatusList, /*maxWaitTimeMs=*/15000, _forceGetOffsetFromStream));
      if (_exception != null) {
        // We had at least one failure, but succeeded now. Log an info
        LOGGER.info("Successfully retrieved PartitionGroupMetadata for topic {}", topicName);
      }
    } catch (TransientConsumerException e) {
      LOGGER.warn("Transient Exception: Could not get partition count for topic {}", topicName, e);
      _exception = e;
      return Boolean.FALSE;
    } catch (Exception e) {
      LOGGER.warn("Could not get partition count for topic {}", topicName, e);
      _exception = e;
      throw e;
    }
    return Boolean.TRUE;
  }

  private Boolean fetchMultipleStreams()
      throws Exception {
    int numStreams = _streamConfigs.size();
    for (int i = 0; i < numStreams; i++) {
      StreamConfig streamConfig = _streamConfigs.get(i);
      String topicName = streamConfig.getTopicName();
      String clientId =
          PartitionGroupMetadataFetcher.class.getSimpleName() + "-" + streamConfig.getTableNameWithType() + "-"
              + topicName;
      StreamConsumerFactory streamConsumerFactory = StreamConsumerFactoryProvider.create(streamConfig);
      int index = i;
      List<PartitionGroupConsumptionStatus> topicPartitionGroupConsumptionStatusList =
          _partitionGroupConsumptionStatusList.stream()
              .filter(partitionGroupConsumptionStatus -> IngestionConfigUtils.getStreamConfigIndexFromPinotPartitionId(
                  partitionGroupConsumptionStatus.getPartitionGroupId()) == index)
              .collect(Collectors.toList());
      try (StreamMetadataProvider streamMetadataProvider = streamConsumerFactory.createStreamMetadataProvider(
          StreamConsumerFactory.getUniqueClientId(clientId))) {
        _newPartitionGroupMetadataList.addAll(
            streamMetadataProvider.computePartitionGroupMetadata(clientId,
                    streamConfig, topicPartitionGroupConsumptionStatusList, /*maxWaitTimeMs=*/15000,
                    _forceGetOffsetFromStream)
                .stream()
                .map(metadata -> new PartitionGroupMetadata(
                    IngestionConfigUtils.getPinotPartitionIdFromStreamPartitionId(metadata.getPartitionGroupId(),
                        index), metadata.getStartOffset()))
                .collect(Collectors.toList()));
        if (_exception != null) {
          // We had at least one failure, but succeeded now. Log an info
          LOGGER.info("Successfully retrieved PartitionGroupMetadata for topic {}", topicName);
        }
      } catch (TransientConsumerException e) {
        LOGGER.warn("Transient Exception: Could not get partition count for topic {}", topicName, e);
        _exception = e;
        return Boolean.FALSE;
      } catch (Exception e) {
        LOGGER.warn("Could not get partition count for topic {}", topicName, e);
        _exception = e;
        throw e;
      }
    }
    return Boolean.TRUE;
  }
}
