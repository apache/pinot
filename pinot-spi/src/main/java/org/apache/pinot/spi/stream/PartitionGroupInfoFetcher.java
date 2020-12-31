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
 * Fetches the partition count of a stream using the {@link StreamMetadataProvider}
 */
public class PartitionGroupInfoFetcher implements Callable<Boolean> {

  private static final Logger LOGGER = LoggerFactory.getLogger(PartitionGroupInfoFetcher.class);

  private List<PartitionGroupInfo> _partitionGroupInfoList;
  private final List<PartitionGroupMetadata> _currentPartitionGroupMetadata;
  private final StreamConsumerFactory _streamConsumerFactory;
  private Exception _exception;
  private final String _topicName;

  public PartitionGroupInfoFetcher(StreamConfig streamConfig, List<PartitionGroupMetadata> currentPartitionGroupMetadataList) {
    _streamConsumerFactory = StreamConsumerFactoryProvider.create(streamConfig);
    _topicName = streamConfig.getTopicName();
    _currentPartitionGroupMetadata = currentPartitionGroupMetadataList;
  }

  public List<PartitionGroupInfo> getPartitionGroupInfoList() {
    return _partitionGroupInfoList;
  }

  public Exception getException() {
    return _exception;
  }

  /**
   * Callable to fetch the partition group info for the stream
   */
  @Override
  public Boolean call()
      throws Exception {

    String clientId = PartitionGroupInfoFetcher.class.getSimpleName() + "-" + _topicName;
    try (
        StreamMetadataProvider streamMetadataProvider = _streamConsumerFactory.createStreamMetadataProvider(clientId)) {
      _partitionGroupInfoList = streamMetadataProvider.getPartitionGroupInfoList(_currentPartitionGroupMetadata, /*maxWaitTimeMs=*/5000L);
      if (_exception != null) {
        // We had at least one failure, but succeeded now. Log an info
        LOGGER.info("Successfully retrieved partition group info for topic {}", _topicName);
      }
      return Boolean.TRUE;
    } catch (TransientConsumerException e) {
      LOGGER.warn("Could not get partition count for topic {}", _topicName, e);
      _exception = e;
      return Boolean.FALSE;
    } catch (Exception e) {
      LOGGER.warn("Could not get partition count for topic {}", _topicName, e);
      _exception = e;
      throw e;
    }
  }
}
