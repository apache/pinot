/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.realtime.stream;

import java.util.concurrent.Callable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Fetches the partition count of a stream using the {@link StreamMetadataProvider}
 */
public class PartitionCountFetcher implements Callable<Boolean> {

  private static final Logger LOGGER = LoggerFactory.getLogger(PartitionCountFetcher.class);

  private int _partitionCount = -1;
  private final StreamConfig _streamConfig;
  private StreamConsumerFactory _streamConsumerFactory;
  private Exception _exception;
  private final String _topicName;

  public PartitionCountFetcher(StreamConfig streamConfig) {
    _streamConfig = streamConfig;
    _streamConsumerFactory = StreamConsumerFactoryProvider.create(_streamConfig);
    _topicName = streamConfig.getTopicName();
  }

  public int getPartitionCount() {
    return _partitionCount;
  }

  public Exception getException() {
    return _exception;
  }

  /**
   * Callable to fetch the number of partitions of the stream given the stream metadata
   * @return
   * @throws Exception
   */
  @Override
  public Boolean call() throws Exception {

    String clientId = PartitionCountFetcher.class.getSimpleName() + "-" + _topicName;
    try (
        StreamMetadataProvider streamMetadataProvider = _streamConsumerFactory.createStreamMetadataProvider(clientId)) {
      _partitionCount = streamMetadataProvider.fetchPartitionCount(/*maxWaitTimeMs=*/5000L);
      if (_exception != null) {
        // We had at least one failure, but succeeded now. Log an info
        LOGGER.info("Successfully retrieved partition count as {} for topic {}", _partitionCount, _topicName);
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
