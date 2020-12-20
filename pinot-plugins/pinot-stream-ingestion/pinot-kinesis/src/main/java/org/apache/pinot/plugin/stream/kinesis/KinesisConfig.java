package org.apache.pinot.plugin.stream.kinesis;

import org.apache.pinot.spi.stream.StreamConfig;


public class KinesisConfig {
  private final StreamConfig _streamConfig;
  private static final String AWS_REGION = "aws-region";
  private static final String MAX_RECORDS_TO_FETCH = "max-records-to-fetch";

  private static final String DEFAULT_AWS_REGION = "us-central-1";
  private static final String DEFAULT_MAX_RECORDS = "20";

  public KinesisConfig(StreamConfig streamConfig) {
    _streamConfig = streamConfig;
  }

  public String getStream(){
    return _streamConfig.getTopicName();
  }

  public String getAwsRegion(){
    return _streamConfig.getStreamConfigsMap().getOrDefault(AWS_REGION, DEFAULT_AWS_REGION);
  }

  public Integer maxRecordsToFetch(){
    return Integer.parseInt(_streamConfig.getStreamConfigsMap().getOrDefault(MAX_RECORDS_TO_FETCH, DEFAULT_MAX_RECORDS));
  }
}
