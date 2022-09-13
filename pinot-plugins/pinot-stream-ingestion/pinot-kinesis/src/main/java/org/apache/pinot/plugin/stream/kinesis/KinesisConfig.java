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

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import java.util.Map;
import java.util.UUID;
import org.apache.pinot.spi.stream.StreamConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.pinot.spi.stream.StreamConfigProperties;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;


/**
 * Kinesis stream specific config
 */
public class KinesisConfig {
  private static final Logger LOGGER = LoggerFactory.getLogger(KinesisConfig.class);

  public static final String STREAM_TYPE = "kinesis";
  public static final String SHARD_ITERATOR_TYPE = "shardIteratorType";
  public static final String REGION = "region";
  public static final String ACCESS_KEY = "accessKey";
  public static final String SECRET_KEY = "secretKey";
  public static final String MAX_RECORDS_TO_FETCH = "maxRecordsToFetch";
  public static final String ENDPOINT = "endpoint";
  public static final String RPS_LIMIT = "requests_per_second_limit";


  // IAM role configs
  /**
   * Enable Role based access to AWS.
   * iamRoleBasedAccessEnabled - Set it to `true` to enable role based access, default: false
   * roleArn - Required. specify the ARN of the role the client should assume.
   * roleSessionName - session name to be used when creating a role based session. default: pinot-kineis-uuid
   * externalId - string external id value required by role's policy. default: null
   * sessionDurationSeconds - The duration, in seconds, of the role session. Default: 900
   * asyncSessionUpdateEnabled -
   *        Configure whether the provider should fetch credentials asynchronously in the background.
   *       If this is true, threads are less likely to block when credentials are loaded,
   *       but additional resources are used to maintain the provider. Default - `true`
   */
  public static final String IAM_ROLE_BASED_ACCESS_ENABLED = "iamRoleBasedAccessEnabled";
  public static final String ROLE_ARN = "roleArn";
  public static final String ROLE_SESSION_NAME = "roleSessionName";
  public static final String EXTERNAL_ID = "externalId";
  public static final String SESSION_DURATION_SECONDS = "sessionDurationSeconds";
  public static final String ASYNC_SESSION_UPDATED_ENABLED = "asyncSessionUpdateEnabled";

  // TODO: this is a starting point, until a better default is figured out
  public static final String DEFAULT_MAX_RECORDS = "20";
  public static final String DEFAULT_SHARD_ITERATOR_TYPE = ShardIteratorType.LATEST.toString();
  public static final String DEFAULT_IAM_ROLE_BASED_ACCESS_ENABLED = "false";
  public static final String DEFAULT_SESSION_DURATION_SECONDS = "900";
  public static final String DEFAULT_ASYNC_SESSION_UPDATED_ENABLED = "true";
  public static final String DEFAULT_RPS_LIMIT = "5";

  private final String _streamTopicName;
  private final String _awsRegion;
  private final int _numMaxRecordsToFetch;
  private final ShardIteratorType _shardIteratorType;
  private final String _accessKey;
  private final String _secretKey;
  private final String _endpoint;
  private boolean _populateMetadata = false;

  // IAM Role values
  private boolean _iamRoleBasedAccess;
  private String _roleArn;
  private String _roleSessionName;
  private String _externalId;
  private int _sessionDurationSeconds;
  private boolean _asyncSessionUpdateEnabled;
  private int _rpsLimit;

  public KinesisConfig(StreamConfig streamConfig) {
    Map<String, String> props = streamConfig.getStreamConfigsMap();
    _streamTopicName = streamConfig.getTopicName();
    _awsRegion = props.get(REGION);
    Preconditions.checkNotNull(_awsRegion, "Must provide 'region' in stream config for table: %s",
        streamConfig.getTableNameWithType());
    _numMaxRecordsToFetch = Integer.parseInt(props.getOrDefault(MAX_RECORDS_TO_FETCH, DEFAULT_MAX_RECORDS));
    _rpsLimit = Integer.parseInt(props.getOrDefault(RPS_LIMIT, DEFAULT_RPS_LIMIT));

    if (_rpsLimit <= 0) {
      LOGGER.warn("Invalid 'requests_per_second_limit' value: {}."
          + " Please provide value greater than 0. Using default: {}", _rpsLimit, DEFAULT_RPS_LIMIT);
      _rpsLimit = Integer.parseInt(DEFAULT_RPS_LIMIT);
    }

    _shardIteratorType =
        ShardIteratorType.fromValue(props.getOrDefault(SHARD_ITERATOR_TYPE, DEFAULT_SHARD_ITERATOR_TYPE));
    _accessKey = props.get(ACCESS_KEY);
    _secretKey = props.get(SECRET_KEY);
    _endpoint = props.get(ENDPOINT);

    _iamRoleBasedAccess =
        Boolean.parseBoolean(props.getOrDefault(IAM_ROLE_BASED_ACCESS_ENABLED, DEFAULT_IAM_ROLE_BASED_ACCESS_ENABLED));
    _roleArn = props.get(ROLE_ARN);
    _roleSessionName =
        props.getOrDefault(ROLE_SESSION_NAME, Joiner.on("-").join("pinot", "kinesis", UUID.randomUUID()));
    _externalId = props.get(EXTERNAL_ID);
    _sessionDurationSeconds =
        Integer.parseInt(props.getOrDefault(SESSION_DURATION_SECONDS, DEFAULT_SESSION_DURATION_SECONDS));
    _asyncSessionUpdateEnabled =
        Boolean.parseBoolean(props.getOrDefault(ASYNC_SESSION_UPDATED_ENABLED, DEFAULT_ASYNC_SESSION_UPDATED_ENABLED));

    if (_iamRoleBasedAccess) {
      Preconditions.checkNotNull(_roleArn,
          "Must provide 'roleArn' in stream config for table %s if iamRoleBasedAccess is enabled",
          streamConfig.getTableNameWithType());
    }
    _populateMetadata = Boolean.parseBoolean(streamConfig.getStreamConfigsMap().getOrDefault(
        StreamConfigProperties.METADATA_POPULATE, "false"));
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

  public int getRpsLimit() {
    return _rpsLimit;
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

  public String getEndpoint() {
    return _endpoint;
  }

  public boolean isIamRoleBasedAccess() {
    return _iamRoleBasedAccess;
  }

  public String getRoleArn() {
    return _roleArn;
  }

  public String getRoleSessionName() {
    return _roleSessionName;
  }

  public String getExternalId() {
    return _externalId;
  }

  public int getSessionDurationSeconds() {
    return _sessionDurationSeconds;
  }

  public boolean isAsyncSessionUpdateEnabled() {
    return _asyncSessionUpdateEnabled;
  }

  public boolean isPopulateMetadata() {
    return _populateMetadata;
  }
}
