package com.linkedin.pinot.core.realtime.impl.kafka;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.Arrays;
import java.util.Map;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.core.data.GenericRow;


public class KafkaAvroMessageDecoder implements KafkaMessageDecoder {
  private static final Logger logger = Logger.getLogger(KafkaAvroMessageDecoder.class);

  public static final String SCHEMA_REGISTRY_REST_URL = "schema.registry.rest.url";
  private org.apache.avro.Schema defaultAvroSchema;
  private String schemaRegistryBaseUrl;
  private String kafkaTopicName;
  private DecoderFactory decoderFactory;
  private AvroRecordToPinotRowGenerator avroRecordConvetrer;

  @Override
  public void init(Map<String, String> props, Schema indexingSchema, String topicName) throws Exception {
    schemaRegistryBaseUrl = props.get(SCHEMA_REGISTRY_REST_URL);
    StringUtils.chomp(schemaRegistryBaseUrl, "/");
    kafkaTopicName = topicName;
    defaultAvroSchema = fetchSchema(new URL(schemaRegistryBaseUrl + "/latest_with_type=" + kafkaTopicName));
    this.avroRecordConvetrer = new AvroRecordToPinotRowGenerator(indexingSchema);
    this.decoderFactory = new DecoderFactory();
  }

  @Override
  public GenericRow decode(byte[] payload) {
    if (payload == null || payload.length == 0) {
      return null;
    }

    // can use the md5 hash to fetch id specific schema
    // will implement that later
    byte[] md5 = new byte[16];
    md5 = Arrays.copyOfRange(payload, 1, 1 + md5.length);

    int start = 1 + md5.length;
    int length = payload.length - 1 - md5.length;

    DatumReader<Record> reader = new GenericDatumReader<Record>(defaultAvroSchema);
    try {
      GenericData.Record avroRecord =
          reader.read(null, decoderFactory.createBinaryDecoder(payload, start, length, null));
      return avroRecordConvetrer.transform(avroRecord);
    } catch (IOException e) {
      logger.error(e.getMessage());
      return null;
    }
  }

  public static org.apache.avro.Schema fetchSchema(URL url) throws Exception {
    BufferedReader reader = null;

    reader = new BufferedReader(new InputStreamReader(url.openStream(), "UTF-8"));
    StringBuilder queryResp = new StringBuilder();
    for (String respLine; (respLine = reader.readLine()) != null;) {
      queryResp.append(respLine);
    }
    return org.apache.avro.Schema.parse(queryResp.toString());
  }

}
