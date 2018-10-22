package com.linkedin.pinot.core.realtime.stream;

import com.linkedin.pinot.common.utils.time.TimeUtils;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A {@link StreamConfig} for a partition level stream
 * This can be removed once we remove HLC implementation from the code
 */
public class PartitionLevelStreamConfig extends StreamConfig {

  private static final Logger LOGGER = LoggerFactory.getLogger(PartitionLevelStreamConfig.class);

  final private int _flushThresholdRows;
  final private long _flushThresholdTimeMillis;

  /**
   * Initializes a partition level stream config using the map of stream configs from the table config
   * This overrides some properties for low level consumer
   * @param streamConfigMap
   */
  public PartitionLevelStreamConfig(Map<String, String> streamConfigMap) {
    super(streamConfigMap);

    int flushThresholdRows = super.getFlushThresholdRows();
    String flushThresholdRowsKey =
        StreamConfigProperties.SEGMENT_FLUSH_THRESHOLD_ROWS + StreamConfigProperties.LLC_SUFFIX;
    String flushThresholdRowsValue = streamConfigMap.get(flushThresholdRowsKey);
    if (flushThresholdRowsValue != null) {
      try {
        flushThresholdRows = Integer.parseInt(flushThresholdRowsValue);
      } catch (Exception e) {
        LOGGER.warn("Caught exception when parsing low level flush threshold rows {}:{}, defaulting to base value {}",
            flushThresholdRowsKey, flushThresholdRowsValue, flushThresholdRows, e);
      }
    }
    _flushThresholdRows = flushThresholdRows;

    long flushThresholdTime = super.getFlushThresholdTimeMillis();
    String flushThresholdTimeKey =
        StreamConfigProperties.SEGMENT_FLUSH_THRESHOLD_TIME + StreamConfigProperties.LLC_SUFFIX;
    String flushThresholdTimeValue = streamConfigMap.get(flushThresholdTimeKey);
    if (flushThresholdTimeValue != null) {
      try {
        flushThresholdTime = TimeUtils.convertPeriodToMillis(flushThresholdTimeValue);
      } catch (Exception e) {
        LOGGER.warn(
            "Caught exception when converting low level flush threshold period to millis {}:{}, defaulting to base value {}",
            flushThresholdTimeKey, flushThresholdTimeValue, flushThresholdTime, e);
      }
    }
    _flushThresholdTimeMillis = flushThresholdTime;
  }

  @Override
  public long getFlushThresholdTimeMillis() {
    return _flushThresholdTimeMillis;
  }

  @Override
  public int getFlushThresholdRows() {
    return _flushThresholdRows;
  }
}
