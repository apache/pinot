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
package org.apache.pinot.plugin.inputformat.protobuf;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.protobuf.Message;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import javax.annotation.Nullable;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.security.ssl.DefaultSslEngineFactory;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordExtractor;
import org.apache.pinot.spi.plugin.PluginManager;
import org.apache.pinot.spi.stream.StreamMessageDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkState;


public class KafkaConfluentSchemaRegistryProtoBufMessageDecoder implements StreamMessageDecoder<byte[]> {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(KafkaConfluentSchemaRegistryProtoBufMessageDecoder.class);
  private static final String SCHEMA_REGISTRY_REST_URL = "schema.registry.rest.url";
  private static final String SCHEMA_REGISTRY_OPTS_PREFIX = "schema.registry.";
  public static final String CACHED_SCHEMA_MAP_CAPACITY = "cached.schema.map.capacity";
  public static final String DEFAULT_CACHED_SCHEMA_MAP_CAPACITY = "1000";

  private KafkaProtobufDeserializer<Message> _deserializer;
  private RecordExtractor<Message> _protoBufRecordExtractor;
  private String _topicName;
  @Nullable
  private final Consumer<Message> _onMessage;

  /**
   * Creates a new instance of this decoder. This constructor with no argument is usually called by reflection
   */
  public KafkaConfluentSchemaRegistryProtoBufMessageDecoder() {
    _onMessage = null;
  }

  /**
   * Creates a new instance of this decoder. This constructor with a Consumer argument is used by test to be able to
   * analyze the received message.
   */
  @VisibleForTesting
  KafkaConfluentSchemaRegistryProtoBufMessageDecoder(@Nullable Consumer<Message> onMessage) {
    _onMessage = onMessage;
  }

  private RestService createRestService(String schemaRegistryUrl, Map<String, String> configs) {
    RestService restService = new RestService(schemaRegistryUrl);

    ConfigDef configDef = new ConfigDef();
    SslConfigs.addClientSslSupport(configDef);
    Map<String, ConfigDef.ConfigKey> configKeyMap = configDef.configKeys();
    Map<String, Object> sslConfigs = new HashMap<>();
    for (String key : configs.keySet()) {
      if (!key.equals(SCHEMA_REGISTRY_REST_URL) && key.startsWith(SCHEMA_REGISTRY_OPTS_PREFIX)) {
        String value = configs.get(key);
        String schemaRegistryOptKey = key.substring(SCHEMA_REGISTRY_OPTS_PREFIX.length());

        if (configKeyMap.containsKey(schemaRegistryOptKey)) {
          if (configKeyMap.get(schemaRegistryOptKey).type == ConfigDef.Type.PASSWORD) {
            sslConfigs.put(schemaRegistryOptKey, new Password(value));
          } else {
            sslConfigs.put(schemaRegistryOptKey, value);
          }
        }
      }
    }

    if (!sslConfigs.isEmpty()) {
      DefaultSslEngineFactory sslFactory = new DefaultSslEngineFactory();
      sslFactory.configure(sslConfigs);
      restService.setSslSocketFactory(sslFactory.sslContext().getSocketFactory());
    }
    return restService;
  }

  @Override
  public void init(Map<String, String> props, Set<String> fieldsToRead, String topicName)
      throws Exception {
    checkState(props.containsKey(SCHEMA_REGISTRY_REST_URL), "Missing required property '%s'", SCHEMA_REGISTRY_REST_URL);
    String schemaRegistryUrl = props.get(SCHEMA_REGISTRY_REST_URL);
    ProtobufSchemaProvider protobufSchemaProvider = new ProtobufSchemaProvider();
    int identityMapCapacity = Integer.parseInt(
        props.getOrDefault(CACHED_SCHEMA_MAP_CAPACITY, DEFAULT_CACHED_SCHEMA_MAP_CAPACITY));
    SchemaRegistryClient schemaRegistryClient =
        new CachedSchemaRegistryClient(createRestService(schemaRegistryUrl, props),
            identityMapCapacity, Collections.singletonList(protobufSchemaProvider), props, null);
    _deserializer = new KafkaProtobufDeserializer<>(schemaRegistryClient);
    Preconditions.checkNotNull(topicName, "Topic must be provided");
    _topicName = topicName;
    _protoBufRecordExtractor = PluginManager.get().createInstance(ProtoBufRecordExtractor.class.getName());
    _protoBufRecordExtractor.init(fieldsToRead, null);
  }

  @Override
  public GenericRow decode(byte[] payload, GenericRow destination) {
    try {
      Message protoMessage = _deserializer.deserialize(_topicName, payload);
      if (_onMessage != null) {
        _onMessage.accept(protoMessage);
      }
      return _protoBufRecordExtractor.extract(protoMessage, destination);
    } catch (RuntimeException e) {
      ignoreOrRethrowException(e);
      return null;
    }
  }

  @Override
  public GenericRow decode(byte[] payload, int offset, int length, GenericRow destination) {
    return decode(Arrays.copyOfRange(payload, offset, offset + length), destination);
  }

  /**
   * This method handles specific serialisation exceptions. If the exception cannot be ignored the method
   * re-throws the exception.
   *
   * @param e exception to handle
   */
  private void ignoreOrRethrowException(RuntimeException e) {
    if (isUnknownMagicByte(e) || isUnknownMagicByte(e.getCause())) {
      // Do nothing, the message is not an ProtoBuf message and can't be decoded
      LOGGER.error("Caught exception while decoding row in topic {}, discarding row", _topicName, e);
      return;
    }
    throw e;
  }

  private boolean isUnknownMagicByte(Throwable e) {
    return e != null && e instanceof SerializationException && e.getMessage() != null && e.getMessage().toLowerCase()
        .contains("unknown magic byte");
  }
}
