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
package org.apache.pinot.plugin.stream.kinesis;

import com.google.common.base.Preconditions;
import java.util.Map;
import org.apache.pinot.spi.stream.StreamConfig;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;


/**
 * Kinesis stream specific config
 */
public class KinesisConfig {
  public static final String STREAM_TYPE = "kinesis";
  public static final String SHARD_ITERATOR_TYPE = "shardIteratorType";
  public static final String REGION = "region";
  public static final String ACCESS_KEY = "accessKey";
  public static final String SECRET_KEY = "secretKey";
  public static final String MAX_RECORDS_TO_FETCH = "maxRecordsToFetch";
  // TODO: this is a starting point, until a better default is figured out
  public static final String DEFAULT_MAX_RECORDS = "20";
  public static final String DEFAULT_SHARD_ITERATOR_TYPE = ShardIteratorType.LATEST.toString();

  private final String _streamTopicName;
  private final String _awsRegion;
  private final int _numMaxRecordsToFetch;
  private final ShardIteratorType _shardIteratorType;
  private final String _accessKey;
  private final String _secretKey;

  public KinesisConfig(StreamConfig streamConfig) {
    Map<String, String> props = streamConfig.getStreamConfigsMap();
    _streamTopicName = streamConfig.getTopicName();
    _awsRegion = props.get(REGION);
    Preconditions.checkNotNull(_awsRegion, "Must provide 'region' in stream config for table: %s",
        streamConfig.getTableNameWithType());
    _numMaxRecordsToFetch = Integer.parseInt(props.getOrDefault(MAX_RECORDS_TO_FETCH, DEFAULT_MAX_RECORDS));
    _shardIteratorType =
        ShardIteratorType.fromValue(props.getOrDefault(SHARD_ITERATOR_TYPE, DEFAULT_SHARD_ITERATOR_TYPE));
    _accessKey = props.get(ACCESS_KEY);
    _secretKey = props.get(SECRET_KEY);
  }

  public String getStreamTopicName() {
    return _streamTopicName;
  }

  public String getAwsRegion() {
    return _awsRegion;
  }

  public int getNumMaxRecordsToFetch() {
    return _numMaxRecordsToFetch;
  }

  public ShardIteratorType getShardIteratorType() {
    return _shardIteratorType;
  }

  public String getAccessKey() {
    return _accessKey;
  }

  public String getSecretKey() {
    return _secretKey;
  }
}
