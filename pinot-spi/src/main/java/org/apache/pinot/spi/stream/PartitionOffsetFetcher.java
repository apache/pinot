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

import java.util.concurrent.Callable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Fetches the partition offset for a stream given the offset criteria, using the {@link StreamMetadataProvider}
 */
public class PartitionOffsetFetcher implements Callable<Boolean> {

  private static final Logger LOGGER = LoggerFactory.getLogger(PartitionOffsetFetcher.class);
  private static final int STREAM_PARTITION_OFFSET_FETCH_TIMEOUT_MILLIS = 10000;

  private final String _topicName;
  private final OffsetCriteria _offsetCriteria;
  private final int _partitionGroupId;

  private Exception _exception = null;
  private StreamPartitionMsgOffset _offset;
  private StreamConsumerFactory _streamConsumerFactory;
  StreamConfig _streamConfig;

  public PartitionOffsetFetcher(final OffsetCriteria offsetCriteria, int partitionGroupId, StreamConfig streamConfig) {
    _offsetCriteria = offsetCriteria;
    _partitionGroupId = partitionGroupId;
    _streamConfig = streamConfig;
    _streamConsumerFactory = StreamConsumerFactoryProvider.create(streamConfig);
    _topicName = streamConfig.getTopicName();
  }

  public StreamPartitionMsgOffset getOffset() {
    return _offset;
  }

  public Exception getException() {
    return _exception;
  }

  /**
   * Callable to fetch the offset of the partition given the stream metadata and offset criteria
   * @return
   * @throws Exception
   */
  @Override
  public Boolean call()
      throws Exception {
    String clientId = PartitionOffsetFetcher.class.getSimpleName() + "-" + _topicName + "-" + _partitionGroupId;
    try (StreamMetadataProvider streamMetadataProvider = _streamConsumerFactory
        .createPartitionMetadataProvider(clientId, _partitionGroupId)) {
      _offset =
          streamMetadataProvider.fetchStreamPartitionOffset(_offsetCriteria, STREAM_PARTITION_OFFSET_FETCH_TIMEOUT_MILLIS);
      if (_exception != null) {
        LOGGER.info("Successfully retrieved offset({}) for stream topic {} partition {}", _offset, _topicName,
            _partitionGroupId);
      }
      return Boolean.TRUE;
    } catch (TransientConsumerException e) {
      LOGGER.warn("Temporary exception when fetching offset for topic {} partition {}:{}", _topicName,
          _partitionGroupId,
          e.getMessage());
      _exception = e;
      return Boolean.FALSE;
    } catch (Exception e) {
      _exception = e;
      throw e;
    }
  }
}
