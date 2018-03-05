/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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

import com.linkedin.pinot.common.config.TableConfig;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.core.realtime.impl.kafka.KafkaStreamMetadata;
import java.util.Map;


/**
 * Factory class to create StreamMetadata based on stream configs in table config
 */
public class StreamMetadataFactory {

  public static StreamMetadata getStreamMetadata(TableConfig realtimeTableConfig) {
    StreamMetadata streamMetadata;

    Map<String, String> streamConfigs = realtimeTableConfig.getIndexingConfig().getStreamConfigs();
    String streamType = streamConfigs.get(CommonConstants.Helix.DataSource.Realtime.STREAM_TYPE);

    switch (streamType) {
      case CommonConstants.Helix.DataSource.KAFKA:
        streamMetadata = new KafkaStreamMetadata(streamConfigs);
        break;
      default:
        throw new IllegalArgumentException("StreamType " + streamType + "is not supported");
    }
    return streamMetadata;
  }
}
