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
import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.StringUtils;
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

  public static final String OAUTH_ISSUER_URL = "issuerUrl";
  public static final String OAUTH_CREDS_FILE_PATH = "credsFilePath";
  public static final String OAUTH_AUDIENCE = "audience";
  public static final String ENABLE_KEY_VALUE_STITCH = "enableKeyValueStitch";
  public static final String METADATA_FIELDS = "metadata.fields"; //list of the metadata fields comma separated

  private final String _pulsarTopicName;
  private final String _subscriberId;
  private final String _bootstrapServers;
  private final MessageId _initialMessageId;
  private final SubscriptionInitialPosition _subscriptionInitialPosition;
  private final String _authenticationToken;
  private final String _tlsTrustCertsFilePath;

  private final String _issuerUrl; // OAUTH2 issuer URL example: "https://auth.streamnative.cloud"
  private final String _credentialsFilePath; // Absolute path of your downloaded key file on the local file system.
                                             // example: file:///path/to/private_creds_file
  private final String _audience; // Audience for your OAUTH2 client: urn:sn:pulsar:test:test-cluster

  // Deprecated since pulsar supports record key extraction
  @Deprecated
  private final boolean _enableKeyValueStitch;
  private final boolean _populateMetadata;
  private final Set<PulsarStreamMessageMetadata.PulsarMessageMetadataValue> _metadataFields;
  public PulsarConfig(StreamConfig streamConfig, String subscriberId) {
    Map<String, String> streamConfigMap = streamConfig.getStreamConfigsMap();
    _subscriberId = subscriberId;

    _pulsarTopicName = streamConfig.getTopicName();
    _bootstrapServers = getConfigValue(streamConfigMap, BOOTSTRAP_SERVERS);
    Preconditions.checkNotNull(_bootstrapServers, "No brokers provided in the config");

    _authenticationToken = getConfigValue(streamConfigMap, AUTHENTICATION_TOKEN);
    _tlsTrustCertsFilePath = getConfigValue(streamConfigMap, TLS_TRUST_CERTS_FILE_PATH);
    _enableKeyValueStitch = Boolean.parseBoolean(getConfigValue(streamConfigMap, ENABLE_KEY_VALUE_STITCH));

    OffsetCriteria offsetCriteria = streamConfig.getOffsetCriteria();

    _subscriptionInitialPosition = PulsarUtils.offsetCriteriaToSubscription(offsetCriteria);
    _initialMessageId = PulsarUtils.offsetCriteriaToMessageId(offsetCriteria);
    _populateMetadata = Boolean.parseBoolean(getConfigValueOrDefault(streamConfigMap,
        StreamConfigProperties.METADATA_POPULATE, "false"));
    String metadataFieldsToExtractCSV = getConfigValueOrDefault(streamConfigMap, METADATA_FIELDS, "");
    if (StringUtils.isBlank(metadataFieldsToExtractCSV) || !_populateMetadata) {
      _metadataFields = Collections.emptySet();
    } else {
      _metadataFields = parseConfigStringToEnumSet(metadataFieldsToExtractCSV);
    }
    _issuerUrl = getConfigValue(streamConfigMap, OAUTH_ISSUER_URL);
    _credentialsFilePath = getConfigValue(streamConfigMap, OAUTH_CREDS_FILE_PATH);
    if (StringUtils.isNotBlank(_credentialsFilePath)) {
      validateOAuthCredFile();
    }
    _audience = getConfigValue(streamConfigMap, OAUTH_AUDIENCE);
  }

  protected void validateOAuthCredFile() {
    try {
      URL credFilePathUrl = new URL(_credentialsFilePath);
      if (!"file".equals(credFilePathUrl.getProtocol())) {
        throw new IllegalArgumentException("Invalid credentials file path: " + _credentialsFilePath
            + ". URL protocol must be file://");
      }
      File credFile = new File(credFilePathUrl.getPath());
      if (!credFile.exists()) {
        throw new IllegalArgumentException("Invalid credentials file path: " + _credentialsFilePath
            + ". File does not exist.");
      }
    } catch (MalformedURLException mue) {
      throw new IllegalArgumentException("Invalid credentials file path: " + _credentialsFilePath, mue);
    }
  }

  private String getConfigValue(Map<String, String> streamConfigMap, String key) {
    return streamConfigMap.get(StreamConfigProperties.constructStreamProperty(STREAM_TYPE, key));
  }

  private String getConfigValueOrDefault(Map<String, String> streamConfigMap, String key, String defaultValue) {
    return streamConfigMap.getOrDefault(StreamConfigProperties.constructStreamProperty(STREAM_TYPE, key), defaultValue);
  }

  private Set<PulsarStreamMessageMetadata.PulsarMessageMetadataValue> parseConfigStringToEnumSet(
      String listOfMetadataFields) {
    try {
      String[] metadataFieldsArr = listOfMetadataFields.split(",");
      return Stream.of(metadataFieldsArr)
          .map(String::trim)
          .filter(StringUtils::isNotBlank)
          .map(PulsarStreamMessageMetadata.PulsarMessageMetadataValue::findByKey)
          .filter(Objects::nonNull)
          .collect(Collectors.toSet());
    } catch (Exception e) {
      throw new IllegalArgumentException("Invalid metadata fields list: " + listOfMetadataFields, e);
    }
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
  public boolean isPopulateMetadata() {
    return _populateMetadata;
  }

  public Set<PulsarStreamMessageMetadata.PulsarMessageMetadataValue> getMetadataFields() {
    return _metadataFields;
  }

  public String getIssuerUrl() {
    return _issuerUrl;
  }

  public String getCredentialsFilePath() {
    return _credentialsFilePath;
  }

  public String getAudience() {
    return _audience;
  }
}
