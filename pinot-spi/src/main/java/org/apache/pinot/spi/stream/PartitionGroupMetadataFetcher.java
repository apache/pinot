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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import org.apache.pinot.spi.utils.IngestionConfigUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Fetches the {@link StreamMetadata} for all streams of a table,
 * using the {@link StreamMetadataProvider}
 */
public class PartitionGroupMetadataFetcher implements Callable<Boolean> {
  private static final Logger LOGGER = LoggerFactory.getLogger(PartitionGroupMetadataFetcher.class);
  private static final int METADATA_FETCH_TIMEOUT_MS = 15000;

  private final List<StreamConfig> _streamConfigs;
  private final List<PartitionGroupConsumptionStatus> _partitionGroupConsumptionStatusList;
  private final boolean _forceGetOffsetFromStream;
  private final List<StreamMetadata> _streamMetadataList = new ArrayList<>();
  private final List<Integer> _pausedTopicIndices;

  private Exception _exception;

  public PartitionGroupMetadataFetcher(List<StreamConfig> streamConfigs,
      List<PartitionGroupConsumptionStatus> partitionGroupConsumptionStatusList, List<Integer> pausedTopicIndices,
      boolean forceGetOffsetFromStream) {
    _streamConfigs = streamConfigs;
    _partitionGroupConsumptionStatusList = partitionGroupConsumptionStatusList;
    _forceGetOffsetFromStream = forceGetOffsetFromStream;
    _pausedTopicIndices = pausedTopicIndices;
  }

  public List<StreamMetadata> getStreamMetadataList() {
    return Collections.unmodifiableList(_streamMetadataList);
  }

  /**
   * @deprecated after 1.5.0 release. Use {@link #getStreamMetadataList()} instead.
   */
  @Deprecated
  public List<PartitionGroupMetadata> getPartitionGroupMetadataList() {
    return _streamMetadataList.stream()
        .flatMap(sm -> sm.getPartitionGroupMetadataList().stream())
        .collect(Collectors.toList());
  }

  public Exception getException() {
    return _exception;
  }

  /**
   * Callable to fetch the {@link StreamMetadata} list from the streams.
   * The stream requires the list of {@link PartitionGroupConsumptionStatus} to compute the new
   * {@link PartitionGroupMetadata}
   */
  @Override
  public Boolean call()
      throws Exception {
    _streamMetadataList.clear();
    _exception = null;
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
      List<PartitionGroupMetadata> partitionGroupMetadataList =
          streamMetadataProvider.computePartitionGroupMetadata(clientId, streamConfig,
              _partitionGroupConsumptionStatusList, /*maxWaitTimeMs=*/METADATA_FETCH_TIMEOUT_MS,
              _forceGetOffsetFromStream);
      _streamMetadataList.add(
          new StreamMetadata(streamConfig, partitionGroupMetadataList.size(), partitionGroupMetadataList));
    } catch (TransientConsumerException e) {
      LOGGER.warn("Transient Exception: Could not get StreamMetadata for topic {}", topicName, e);
      _exception = e;
      return Boolean.FALSE;
    } catch (Exception e) {
      LOGGER.warn("Could not get StreamMetadata for topic {}", topicName, e);
      _exception = e;
      throw e;
    }
    return Boolean.TRUE;
  }

  private Boolean fetchMultipleStreams()
      throws Exception {
    int numStreams = _streamConfigs.size();
    for (int i = 0; i < numStreams; i++) {
      if (_pausedTopicIndices.contains(i)) {
        LOGGER.info("Skipping fetching StreamMetadata for paused topic: {}",
            _streamConfigs.get(i).getTopicName());
        continue;
      }
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
        List<PartitionGroupMetadata> partitionGroupMetadataList =
            streamMetadataProvider.computePartitionGroupMetadata(clientId,
                    streamConfig, topicPartitionGroupConsumptionStatusList,
                    /*maxWaitTimeMs=*/METADATA_FETCH_TIMEOUT_MS,
                    _forceGetOffsetFromStream)
                .stream()
                .map(metadata -> new PartitionGroupMetadata(
                    IngestionConfigUtils.getPinotPartitionIdFromStreamPartitionId(metadata.getPartitionGroupId(),
                        index), metadata.getStartOffset(), metadata.getSequenceNumber()))
                .collect(Collectors.toList());
        _streamMetadataList.add(
            new StreamMetadata(streamConfig, partitionGroupMetadataList.size(), partitionGroupMetadataList));
      } catch (TransientConsumerException e) {
        LOGGER.warn("Transient Exception: Could not get StreamMetadata for topic {}", topicName, e);
        _exception = e;
        return Boolean.FALSE;
      } catch (Exception e) {
        LOGGER.warn("Could not get StreamMetadata for topic {}", topicName, e);
        _exception = e;
        throw e;
      }
    }
    return Boolean.TRUE;
  }
}
