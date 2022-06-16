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
package org.apache.pinot.plugin.stream.pulsar;

import com.google.common.base.Preconditions;
import java.util.Map;
import org.apache.pinot.spi.stream.OffsetCriteria;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamConfigProperties;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;


/**
 * Pulsar specific stream config
 * contains pulsar brokers list, start offset and group id/subscriber id if using high level consumer/
 */
public class PulsarConfig {
  public static final String STREAM_TYPE = "pulsar";
  public static final String BOOTSTRAP_SERVERS = "bootstrap.servers";
  public static final String AUTHENTICATION_TOKEN = "authenticationToken";
  public static final String TLS_TRUST_CERTS_FILE_PATH = "tlsTrustCertsFilePath";
  public static final String ENABLE_KEY_VALUE_STITCH = "enableKeyValueStitch";

  private String _pulsarTopicName;
  private String _subscriberId;
  private String _bootstrapServers;
  private MessageId _initialMessageId;
  private SubscriptionInitialPosition _subscriptionInitialPosition;
  private String _authenticationToken;
  private String _tlsTrustCertsFilePath;
  private boolean _enableKeyValueStitch;

  public PulsarConfig(StreamConfig streamConfig, String subscriberId) {
    Map<String, String> streamConfigMap = streamConfig.getStreamConfigsMap();
    _pulsarTopicName = streamConfig.getTopicName();
    _bootstrapServers =
        streamConfigMap.get(StreamConfigProperties.constructStreamProperty(STREAM_TYPE, BOOTSTRAP_SERVERS));
    _subscriberId = subscriberId;

    String authenticationTokenKey = StreamConfigProperties.constructStreamProperty(STREAM_TYPE, AUTHENTICATION_TOKEN);
    _authenticationToken = streamConfigMap.get(authenticationTokenKey);

    String tlsTrustCertsFilePathKey = StreamConfigProperties.
        constructStreamProperty(STREAM_TYPE, TLS_TRUST_CERTS_FILE_PATH);
    _tlsTrustCertsFilePath = streamConfigMap.get(tlsTrustCertsFilePathKey);

    String enableKeyValueStitchKey = StreamConfigProperties.
        constructStreamProperty(STREAM_TYPE, ENABLE_KEY_VALUE_STITCH);
    _enableKeyValueStitch = Boolean.parseBoolean(streamConfigMap.get(enableKeyValueStitchKey));

    Preconditions.checkNotNull(_bootstrapServers, "No brokers provided in the config");

    OffsetCriteria offsetCriteria = streamConfig.getOffsetCriteria();

    _subscriptionInitialPosition = PulsarUtils.offsetCriteriaToSubscription(offsetCriteria);
    _initialMessageId = PulsarUtils.offsetCriteriaToMessageId(offsetCriteria);
  }

  public String getPulsarTopicName() {
    return _pulsarTopicName;
  }

  public String getSubscriberId() {
    return _subscriberId;
  }

  public String getBootstrapServers() {
    return _bootstrapServers;
  }

  public MessageId getInitialMessageId() {
    return _initialMessageId;
  }

  public SubscriptionInitialPosition getInitialSubscriberPosition() {
    return _subscriptionInitialPosition;
  }

  public String getAuthenticationToken() {
    return _authenticationToken;
  }

  public String getTlsTrustCertsFilePath() {
    return _tlsTrustCertsFilePath;
  }

  public boolean getEnableKeyValueStitch() {
    return _enableKeyValueStitch;
  }
}
