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

import java.util.List;
import java.util.concurrent.Callable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Fetches the list of {@link PartitionGroupMetadata} for all partition groups of the stream,
 * using the {@link StreamMetadataProvider}
 */
public class PartitionGroupMetadataFetcher implements Callable<Boolean> {

  private static final Logger LOGGER = LoggerFactory.getLogger(PartitionGroupMetadataFetcher.class);

  private List<PartitionGroupMetadata> _newPartitionGroupMetadataList;
  private final StreamConfig _streamConfig;
  private final List<PartitionGroupConsumptionStatus> _partitionGroupConsumptionStatusList;
  private final StreamConsumerFactory _streamConsumerFactory;
  private Exception _exception;
  private final String _topicName;

  public PartitionGroupMetadataFetcher(StreamConfig streamConfig,
      List<PartitionGroupConsumptionStatus> partitionGroupConsumptionStatusList) {
    _streamConsumerFactory = StreamConsumerFactoryProvider.create(streamConfig);
    _topicName = streamConfig.getTopicName();
    _streamConfig = streamConfig;
    _partitionGroupConsumptionStatusList = partitionGroupConsumptionStatusList;
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
    String clientId = PartitionGroupMetadataFetcher.class.getSimpleName() + "-"
        + _streamConfig.getTableNameWithType() + "-" + _topicName;
    try (
        StreamMetadataProvider streamMetadataProvider = _streamConsumerFactory.createStreamMetadataProvider(clientId)) {
      _newPartitionGroupMetadataList = streamMetadataProvider.computePartitionGroupMetadata(clientId, _streamConfig,
          _partitionGroupConsumptionStatusList, /*maxWaitTimeMs=*/5000);
      if (_exception != null) {
        // We had at least one failure, but succeeded now. Log an info
        LOGGER.info("Successfully retrieved PartitionGroupMetadata for topic {}", _topicName);
      }
      return Boolean.TRUE;
    } catch (TransientConsumerException e) {
      LOGGER.warn("Transient Exception: Could not get partition count for topic {}", _topicName, e);
      _exception = e;
      return Boolean.FALSE;
    } catch (Exception e) {
      LOGGER.warn("Could not get partition count for topic {}", _topicName, e);
      _exception = e;
      throw e;
    }
  }
}
