/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.plugin.inputformat.avro.datahub;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Preconditions;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.commons.codec.binary.Base64;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicNameValuePair;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.pinot.plugin.inputformat.avro.AvroRecordExtractor;
import org.apache.pinot.plugin.inputformat.avro.AvroRecordExtractorConfig;
import org.apache.pinot.plugin.inputformat.avro.datahub.util.HttpUtil;
import org.apache.pinot.plugin.inputformat.avro.datahub.util.MutableByteArrayInputStream;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordExtractor;
import org.apache.pinot.spi.plugin.PluginManager;
import org.apache.pinot.spi.stream.StreamMessageDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Decodes avro messages with datahub schema registry.
 * NOTE: Do not use schema in the implementation, as schema will be removed from the params
 */
public class KafkaDatahubSchemaRegistryAvroMessageDecoder implements StreamMessageDecoder<byte[]> {

  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaDatahubSchemaRegistryAvroMessageDecoder.class);

  static final byte MAGIC_BYTE = 0;

  public static final String TOKEN_URL = "https://idbroker.webex.com";
  public static final String LIST_SCHEMA = "https://datahub.webex.com/api/v1/kafka/avroschemas";

  private String _topicName;
  private RecordExtractor<Record> _avroRecordExtractor;

  private String clusterParam;
  private String machineAccountOrgId;
  private String machineAccountName;
  private String machineAccountPassword;
  private String machineClientId;
  private String machineClientSecret;

  private transient MutableByteArrayInputStream inputStream;
  private transient Decoder decoder;
  private final Map<String, Schema> schemaVersionMap = new HashMap<>();
  private final Map<String, GenericDatumReader<GenericRecord>> datumReaderMap = new HashMap<>();

  @Override
  public void init(Map<String, String> props, Set<String> fieldsToRead, String topicName)
      throws Exception {
    _topicName = topicName;
    Preconditions.checkNotNull(topicName, "Topic must be provided");

    clusterParam = props.get("schema.registry.cluster");
    machineAccountOrgId = props.get("schema.registry.machineAccountOrgId");
    machineAccountName = props.get("schema.registry.machineAccountName");
    machineClientId = props.get("schema.registry.machineClientId");
    machineClientSecret = props.get("schema.registry.machineClientSecret");
    machineAccountPassword = props.get("schema.registry.machineAccountPassword");


    this.inputStream = new MutableByteArrayInputStream();
    this.decoder = DecoderFactory.get().binaryDecoder(this.inputStream, null);

    AvroRecordExtractorConfig config = new AvroRecordExtractorConfig();
    config.init(props);
    _avroRecordExtractor = PluginManager.get().createInstance(AvroRecordExtractor.class.getName());
    _avroRecordExtractor.init(fieldsToRead, config);
  }

  @Override
  public GenericRow decode(byte[] payload, GenericRow destination) {
    inputStream.setBuffer(payload);

    if (inputStream.read() != 0) {
      throw new SerializationException("Unknown magic byte!");
    }

    // read avro data
    Record avroRecord = null;
    try {
      int versionBytesNum = inputStream.read();
      //int schemaId =  ByteBuffer.wrap(inputStream.readNBytes(4)).getInt();
      String schemaVersion =  new String(inputStream.readNBytes(versionBytesNum), StandardCharsets.UTF_8);
      if (!schemaVersionMap.containsKey(schemaVersion)) {
        Schema schema = fetchAvroSchema(schemaVersion);
        schemaVersionMap.put(schemaVersion, schema);
        GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema, schema,
            new GenericData(Thread.currentThread().getContextClassLoader()));
        datumReaderMap.put(schemaVersion, datumReader);
      }

      GenericDatumReader<GenericRecord> datumReader = datumReaderMap.get(schemaVersion);
      avroRecord = (Record) datumReader.read(null, decoder);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return _avroRecordExtractor.extract(avroRecord, destination);
  }

  @Override
  public GenericRow decode(byte[] payload, int offset, int length, GenericRow destination) {
    return decode(Arrays.copyOfRange(payload, offset, offset + length), destination);
  }

  private Schema fetchAvroSchema(String schemaVersion) throws Exception {
    HashMap<String, String> httpHeaders = new HashMap<>();
    httpHeaders.put("accept", "*/*");
    httpHeaders.put("Authorization", "Bearer " + getAccessToken());

    String urlSchema = LIST_SCHEMA + "?cluster=" + clusterParam + "&topic=" + _topicName;
    LOGGER.info("fetching using url {}", urlSchema);

    String res = HttpUtil.get(urlSchema, httpHeaders);
    JSONArray jsonArray = JSONArray.parseArray(res);

    if (jsonArray == null || jsonArray.isEmpty()) {
      throw new IllegalArgumentException("Could not find schema for topic in datahub");
    }
    Object firstElement = jsonArray.get(0);
    if (firstElement instanceof String) {
      throw new RuntimeException("Received malformed response from datahub " + firstElement);
    }

    for (int i = 0; i < jsonArray.size(); i++) {
      JSONObject jsonObject = jsonArray.getJSONObject(i);
      String schemaStr = jsonObject.getString("schema");
      String version = jsonObject.getString("version");
      if (version.equalsIgnoreCase(schemaVersion)) {
        LOGGER.info("fetched schema => [{}]", schemaStr);
        return (new Schema.Parser()).parse(schemaStr);
      }
    }
    throw new IllegalStateException("Could not find configured schema");
  }

  public String getAccessToken() throws Exception {
    String userUrl =
        TOKEN_URL + "/idb/token/" + machineAccountOrgId + "/v2/actions/GetBearerToken/invoke";
    HashMap<String, String> headers = new HashMap<>();
    headers.put("Accept", "application/json");
    headers.put("Content-Type", "application/json;charset=utf-8");

    JSONObject paramJson = new JSONObject();
    paramJson.put("name", machineAccountName);
    paramJson.put("password", machineAccountPassword);
    StringEntity se = new StringEntity(paramJson.toString(), "UTF-8");

    String responseBody = HttpUtil.post(userUrl, se, headers);
    JSONObject resOne = JSONObject.parseObject(responseBody);

    String userUrl2 = TOKEN_URL + "/idb/oauth2/v1/access_token";
    String basicToken = Base64.encodeBase64String((machineClientId + ":"
        + machineClientSecret).getBytes(StandardCharsets.UTF_8));

    HashMap<String, String> headersTwo = new HashMap<>();
    headersTwo.put("Authorization", "Basic " + basicToken);
    headersTwo.put("Accept", "application/json");
    headersTwo.put("Content-Type", "application/x-www-form-urlencoded");

    HashMap<String, String> paramsMap = new HashMap<>();
    paramsMap.put("grant_type", "urn:ietf:params:oauth:grant-type:saml2-bearer");
    paramsMap.put("assertion", resOne.get("BearerToken").toString());
    //paramsMap.put("scope", "Identity:SCIM");
    paramsMap.put("scope", "Identity:SCIM Identity:Organization Identity:Config Identity:OAuthKeyService notification-service:notification_read cjp:billing_config_read cjp:organization webexsquare:billing");
    paramsMap.put("self_contained_token", "true");

    List<NameValuePair> pairs = new ArrayList<>();

    for (Map.Entry<String, String> entry : paramsMap.entrySet()) {
      String key = entry.getKey();
      Object value = entry.getValue();
      pairs.add(new BasicNameValuePair(key, String.valueOf(value)));
    }

    UrlEncodedFormEntity ue = new UrlEncodedFormEntity(pairs, "UTF-8");
    String resTwo = HttpUtil.post(userUrl2, ue, headersTwo);
    JSONObject jbTwo = JSONObject.parseObject(resTwo);
    if (jbTwo == null) {
      throw new IllegalStateException("Could not get the access token");
    }
    return jbTwo.get("access_token").toString();
  }
}
