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
package org.apache.pinot.plugin.stream.microbatch.kafka30;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.Base64;


/**
 * Version 1 payload for MicroBatch protocol.
 *
 * <p>Supports two message types:
 * <ul>
 *   <li>URI - reference to a file (file://, s3://, hdfs://, etc.)</li>
 *   <li>DATA - inline base64-encoded data</li>
 * </ul>
 *
 * <p>Example JSON payloads:
 * <pre>
 * // URI type
 * {"type":"uri","format":"avro","uri":"s3://bucket/file.avro","numRecords":1000}
 *
 * // DATA type
 * {"type":"data","format":"avro","data":"<base64>","numRecords":100}
 * </pre>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class MicroBatchPayloadV1 {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  public enum Type {
    URI, DATA
  }

  public enum Format {
    AVRO, PARQUET, JSON
  }

  @JsonProperty("type")
  private String _typeStr;

  @JsonProperty("format")
  private String _formatStr;

  @JsonProperty("uri")
  private String _uri;

  @JsonProperty("data")
  private String _dataBase64;

  @JsonProperty("numRecords")
  private int _numRecords;

  // Cached parsed values
  private transient Type _type;
  private transient Format _format;
  private transient byte[] _decodedData;

  // Default constructor for Jackson
  public MicroBatchPayloadV1() {
  }

  /**
   * Parse payload from JSON bytes.
   */
  public static MicroBatchPayloadV1 parse(byte[] jsonBytes) throws IOException {
    MicroBatchPayloadV1 payload = MAPPER.readValue(jsonBytes, MicroBatchPayloadV1.class);
    payload.validate();
    return payload;
  }

  /**
   * Validate the payload after deserialization.
   */
  private void validate() {
    if (_typeStr == null) {
      throw new IllegalArgumentException("Missing required field: 'type'");
    }
    if (_formatStr == null) {
      throw new IllegalArgumentException("Missing required field: 'format'");
    }

    // Parse and validate type
    try {
      _type = Type.valueOf(_typeStr.toUpperCase());
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Invalid type: " + _typeStr + ". Valid values: uri, data");
    }

    // Parse and validate format
    try {
      _format = Format.valueOf(_formatStr.toUpperCase());
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "Invalid format: " + _formatStr + ". Valid values: avro, parquet, json");
    }

    // Validate numRecords is provided and positive
    if (_numRecords <= 0) {
      throw new IllegalArgumentException(
          "Missing or invalid required field 'numRecords': must be a positive integer, got: " + _numRecords);
    }

    // Validate type-specific fields - ensure only the appropriate field is set for each type
    switch (_type) {
      case URI:
        if (_uri == null || _uri.isEmpty()) {
          throw new IllegalArgumentException("Missing required field 'uri' for type=uri");
        }
        if (_dataBase64 != null) {
          throw new IllegalArgumentException("Field 'data' must not be set when type=uri");
        }
        break;
      case DATA:
        if (_dataBase64 == null) {
          throw new IllegalArgumentException("Missing required field 'data' for type=data");
        }
        if (_uri != null && !_uri.isEmpty()) {
          throw new IllegalArgumentException("Field 'uri' must not be set when type=data");
        }
        // Decode base64 data (empty string is valid - decodes to empty byte array)
        try {
          _decodedData = Base64.getDecoder().decode(_dataBase64);
        } catch (IllegalArgumentException e) {
          throw new IllegalArgumentException("Invalid base64 encoding in 'data' field", e);
        }
        break;
      default:
        throw new IllegalArgumentException("Unknown type: " + _typeStr);
    }
  }

  /**
   * Create a URI payload.
   */
  public static MicroBatchPayloadV1 createUri(String uri, Format format, int numRecords) {
    MicroBatchPayloadV1 payload = new MicroBatchPayloadV1();
    payload._type = Type.URI;
    payload._typeStr = "uri";
    payload._format = format;
    payload._formatStr = format.name().toLowerCase();
    payload._uri = uri;
    payload._numRecords = numRecords;
    return payload;
  }

  /**
   * Create a DATA payload.
   */
  public static MicroBatchPayloadV1 createData(byte[] data, Format format, int numRecords) {
    MicroBatchPayloadV1 payload = new MicroBatchPayloadV1();
    payload._type = Type.DATA;
    payload._typeStr = "data";
    payload._format = format;
    payload._formatStr = format.name().toLowerCase();
    payload._decodedData = data;
    payload._dataBase64 = Base64.getEncoder().encodeToString(data);
    payload._numRecords = numRecords;
    return payload;
  }

  /**
   * Serialize payload to JSON bytes.
   */
  public byte[] toJsonBytes() throws IOException {
    return MAPPER.writeValueAsBytes(this);
  }

  // Getters

  public Type getType() {
    return _type;
  }

  public Format getFormat() {
    return _format;
  }

  public String getUri() {
    return _uri;
  }

  /**
   * Get the decoded data bytes (for type=DATA).
   */
  public byte[] getData() {
    return _decodedData;
  }

  public int getNumRecords() {
    return _numRecords;
  }
}
