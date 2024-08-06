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
package org.apache.pinot.plugin.inputformat.avro.datahub.deserializer;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaDatahubAvroDeserializer {

  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaDatahubAvroDeserializer.class);

  private static final String DEFAULT_DATAHUB_URL = "https://datahub.webex.com";
  private static final String DEFAULT_ID_BROKER_URL = "https://idbroker.webex.com";
  private static final String LIST_SCHEMA = "/api/v1/kafka/avroschemas";
  private static final byte MAGIC_BYTE = 0;

  private final String _datahubUrl;
  private final String _idBrokerUrl;
  private final String _cluster;
  private final String _topic;
  private final String _machineAccountOrgId;
  private final String _machineAccountName;
  private final String _machineClientId;
  private final String _machineClientSecret;
  private final String _machineAccountPassword;

  private final MutableByteArrayInputStream _inputStream;
  private final Decoder _decoder;
  private final Map<String, GenericDatumReader<GenericRecord>> _datumReaderCache;

  public KafkaDatahubAvroDeserializer(String topic, Map<String, String> props) {
    _topic = topic;
    _datahubUrl = props.getOrDefault("datahub.url", DEFAULT_DATAHUB_URL);
    _idBrokerUrl = props.getOrDefault("datahub.idBrokerUrl", DEFAULT_ID_BROKER_URL);
    _cluster = props.get("datahub.cluster");
    _machineAccountOrgId = props.get("datahub.machineAccountOrgId");
    _machineAccountName = props.get("datahub.machineAccountName");
    _machineClientId = props.get("datahub.machineClientId");
    _machineClientSecret = resolveSecureProp(props.get("datahub.machineClientSecret"));
    _machineAccountPassword = resolveSecureProp(props.get("datahub.machineAccountPassword"));

    _inputStream = new MutableByteArrayInputStream();
    _decoder = DecoderFactory.get().binaryDecoder(_inputStream, null);
    _datumReaderCache = new HashMap<>();
  }

  public GenericRecord deserialize(byte[] payload) {
    _inputStream.setBuffer(payload);
    if (_inputStream.read() != MAGIC_BYTE) {
      throw new SerializationException("Unknown magic byte!");
    }
    int numOfVersionBytes = _inputStream.read();
    try {
      String schemaVersion = new String(_inputStream.readNBytes(numOfVersionBytes),
          StandardCharsets.UTF_8);
      return getDatumReader(schemaVersion).read(null, _decoder);
    } catch (Exception e) {
      throw new RuntimeException("Could not deserialize the record", e);
    }
  }

  private GenericDatumReader<GenericRecord> getDatumReader(
      final String schemaVersion) {
    return _datumReaderCache.computeIfAbsent(schemaVersion, schemaVer -> {
      Schema schema = fetchAvroSchema(schemaVer);
      return new GenericDatumReader<>(schema, schema,
          new GenericData(Thread.currentThread().getContextClassLoader()));
    });
  }

  private String resolveSecureProp(final String secret) {
    return secret;
  }

  private Schema fetchAvroSchema(final String schemaVersion) {
    HashMap<String, String> httpHeaders = new HashMap<>();
    httpHeaders.put("accept", "*/*");
    httpHeaders.put("Authorization", "Bearer " + getAccessToken());

    String urlSchema = _datahubUrl + LIST_SCHEMA + "?cluster=" + _cluster + "&topic=" + _topic;
    LOGGER.info("fetching using url {}", urlSchema);

    String res = HttpUtil.get(urlSchema, httpHeaders);
    JSONArray jsonArray = JSONArray.parseArray(res);

    if (jsonArray == null || jsonArray.isEmpty()) {
      throw new IllegalArgumentException("Could not find schema for topic in datahub");
    }
    Object firstElement = jsonArray.get(0);
    if (firstElement instanceof String) {
      throw new RuntimeException("Received failure response: " + firstElement);
    }

    for (int i = 0; i < jsonArray.size(); i++) {
      JSONObject jsonObject = jsonArray.getJSONObject(i);
      String schemaStr = jsonObject.getString("schema");
      String version = jsonObject.getString("version");
      if (version.equalsIgnoreCase(schemaVersion)) {
        LOGGER.info("fetched schema for topic {}, version {} => [{}]",
            _topic,
            schemaVersion,
            schemaStr);
        return (new Schema.Parser()).parse(schemaStr);
      }
    }
    throw new IllegalStateException(String.format("Could not find schema: topic %s, version $%s",
        _topic, schemaVersion));
  }

  public String getAccessToken() {
    String userUrl =
        _idBrokerUrl + "/idb/token/" + _machineAccountOrgId + "/v2/actions/GetBearerToken/invoke";
    HashMap<String, String> headers = new HashMap<>();
    headers.put("Accept", "application/json");
    headers.put("Content-Type", "application/json;charset=utf-8");

    JSONObject paramJson = new JSONObject();
    paramJson.put("name", _machineAccountName);
    paramJson.put("password", _machineAccountPassword);
    StringEntity se = new StringEntity(paramJson.toString(), "UTF-8");

    String responseBody = HttpUtil.post(userUrl, se, headers);
    JSONObject bearerTokenRes = JSONObject.parseObject(responseBody);
    if (bearerTokenRes == null) {
      throw new IllegalStateException("Could not fetch bearer token");
    }
    String bearerToken = bearerTokenRes.getString("BearerToken");

    String userUrl2 = _idBrokerUrl + "/idb/oauth2/v1/access_token";
    String basicToken = Base64.encodeBase64String((_machineClientId + ":"
        + _machineClientSecret).getBytes(StandardCharsets.UTF_8));

    HashMap<String, String> headersTwo = new HashMap<>();
    headersTwo.put("Authorization", "Basic " + basicToken);
    headersTwo.put("Accept", "application/json");
    headersTwo.put("Content-Type", "application/x-www-form-urlencoded");

    HashMap<String, String> paramsMap = new HashMap<>();
    paramsMap.put("grant_type", "urn:ietf:params:oauth:grant-type:saml2-bearer");
    paramsMap.put("assertion", bearerToken);
    paramsMap.put("scope", "Identity:SCIM");
    paramsMap.put("self_contained_token", "true");

    List<NameValuePair> pairs = new ArrayList<>();

    for (Map.Entry<String, String> entry : paramsMap.entrySet()) {
      String key = entry.getKey();
      Object value = entry.getValue();
      pairs.add(new BasicNameValuePair(key, String.valueOf(value)));
    }

    UrlEncodedFormEntity ue;
    try {
      ue = new UrlEncodedFormEntity(pairs, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new IllegalStateException("Could not prepare the access token url params", e);
    }
    String resTwo = HttpUtil.post(userUrl2, ue, headersTwo);
    JSONObject jbTwo = JSONObject.parseObject(resTwo);
    if (jbTwo == null) {
      throw new IllegalStateException("Could not get the access token");
    }
    return jbTwo.get("access_token").toString();
  }
}
