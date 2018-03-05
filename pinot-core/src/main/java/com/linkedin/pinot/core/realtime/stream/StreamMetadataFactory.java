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
