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
package org.apache.pinot.plugin.inputformat.avro.confluent;

import com.google.common.base.Preconditions;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import org.apache.avro.generic.GenericData.Record;
import org.apache.pinot.plugin.inputformat.avro.AvroRecordExtractor;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordExtractor;
import org.apache.pinot.spi.plugin.PluginManager;
import org.apache.pinot.spi.stream.StreamMessageDecoder;

import static com.google.common.base.Preconditions.checkState;


/**
 * Decodes avro messages with confluent schema registry.
 * First byte is MAGIC = 0, second 4 bytes are the schema id, the remainder is the value.
 * NOTE: Do not use schema in the implementation, as schema will be removed from the params
 */
public class KafkaConfluentSchemaRegistryAvroMessageDecoder implements StreamMessageDecoder<byte[]> {
  private static final String SCHEMA_REGISTRY_REST_URL = "schema.registry.rest.url";
  private KafkaAvroDeserializer _deserializer;
  private RecordExtractor<Record> _avroRecordExtractor;
  private String _topicName;

  @Override
  public void init(Map<String, String> props, Set<String> fieldsToRead, String topicName)
      throws Exception {
    checkState(props.containsKey(SCHEMA_REGISTRY_REST_URL), "Missing required property '%s'", SCHEMA_REGISTRY_REST_URL);
    String schemaRegistryUrl = props.get(SCHEMA_REGISTRY_REST_URL);
    SchemaRegistryClient schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryUrl, 1000);
    _deserializer = new KafkaAvroDeserializer(schemaRegistryClient);
    Preconditions.checkNotNull(topicName, "Topic must be provided");
    _topicName = topicName;
    _avroRecordExtractor = PluginManager.get().createInstance(AvroRecordExtractor.class.getName());
    _avroRecordExtractor.init(fieldsToRead, null);
  }

  @Override
  public GenericRow decode(byte[] payload, GenericRow destination) {
    Record avroRecord = (Record) _deserializer.deserialize(_topicName, payload);
    return _avroRecordExtractor.extract(avroRecord, destination);
  }

  @Override
  public GenericRow decode(byte[] payload, int offset, int length, GenericRow destination) {
    return decode(Arrays.copyOfRange(payload, offset, offset + length), destination);
  }
}
