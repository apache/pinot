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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;


/**
 * MicroBatch protocol message format.
 *
 * <h2>Wire Format</h2>
 * <pre>
 * +----------+---------------------------+
 * | version  | payload bytes             |
 * | (1 byte) | (variable length)         |
 * +----------+---------------------------+
 * </pre>
 *
 * <h2>Versioning Scheme</h2>
 * <p>The first byte indicates the protocol version (0-255), which determines how to parse
 * the remaining payload bytes. This design:
 * <ul>
 *   <li>Enables efficient version detection without parsing the payload</li>
 *   <li>Allows future protocol evolution while maintaining backward compatibility</li>
 *   <li>Supports up to 255 protocol versions</li>
 * </ul>
 *
 * <p>Version allocation:
 * <ul>
 *   <li>Version 1: JSON payload parsed as {@link MicroBatchPayloadV1}</li>
 *   <li>Versions 2-255: Reserved for future use</li>
 * </ul>
 *
 * <p>When adding a new version:
 * <ol>
 *   <li>Define a new VERSION_N constant</li>
 *   <li>Create a new payload class (e.g., MicroBatchPayloadV2)</li>
 *   <li>Update {@link #parse(byte[])} to handle the new version</li>
 *   <li>Add accessor method (e.g., getPayloadV2())</li>
 * </ol>
 *
 * <h2>Usage</h2>
 * <pre>
 * // Parsing
 * MicroBatchProtocol protocol = MicroBatchProtocol.parse(kafkaMessageBytes);
 * MicroBatchPayloadV1 payload = protocol.getPayloadV1();
 *
 * // Creating
 * byte[] message = MicroBatchProtocol.createUriMessage("s3://bucket/file.avro", Format.AVRO, 1000);
 * </pre>
 */
public class MicroBatchProtocol {

  public static final int VERSION_1 = 1;
  public static final int CURRENT_VERSION = VERSION_1;

  private final int _version;
  private final MicroBatchPayloadV1 _payloadV1;

  private MicroBatchProtocol(int version, MicroBatchPayloadV1 payloadV1) {
    _version = version;
    _payloadV1 = payloadV1;
  }

  /**
   * Parse a MicroBatch protocol message from raw bytes.
   *
   * @param bytes the raw message bytes (version byte + payload)
   * @return parsed protocol message
   * @throws IOException if parsing fails
   * @throws IllegalArgumentException if version is unsupported or payload is invalid
   */
  public static MicroBatchProtocol parse(byte[] bytes) throws IOException {
    if (bytes == null || bytes.length < 2) {
      throw new IllegalArgumentException(
          "Invalid MicroBatch message: must have at least 2 bytes (version + payload)");
    }

    int version = Byte.toUnsignedInt(bytes[0]);
    byte[] payloadBytes = Arrays.copyOfRange(bytes, 1, bytes.length);

    switch (version) {
      case VERSION_1:
        MicroBatchPayloadV1 payload = MicroBatchPayloadV1.parse(payloadBytes);
        return new MicroBatchProtocol(version, payload);

      default:
        throw new IllegalArgumentException(
            "Unsupported MicroBatch protocol version: " + version
                + ". Supported versions: " + VERSION_1);
    }
  }

  /**
   * Create a URI message with the current protocol version.
   *
   * @param uri the file URI (s3://, hdfs://, file://, etc.)
   * @param format the data format
   * @return serialized protocol message bytes
   */
  public static byte[] createUriMessage(String uri, MicroBatchPayloadV1.Format format)
      throws IOException {
    return createUriMessage(uri, format, 0);
  }

  /**
   * Create a URI message with the current protocol version.
   *
   * @param uri the file URI (s3://, hdfs://, file://, etc.)
   * @param format the data format
   * @param numRecords number of records in the file (0 if unknown)
   * @return serialized protocol message bytes
   */
  public static byte[] createUriMessage(String uri, MicroBatchPayloadV1.Format format,
      long numRecords) throws IOException {
    MicroBatchPayloadV1 payload =
        MicroBatchPayloadV1.createUri(uri, format, (int) numRecords);
    return createMessage(CURRENT_VERSION, payload.toJsonBytes());
  }

  /**
   * Create a DATA message with inline data.
   *
   * @param data the raw data bytes
   * @param format the data format
   * @return serialized protocol message bytes
   */
  public static byte[] createDataMessage(byte[] data, MicroBatchPayloadV1.Format format)
      throws IOException {
    return createDataMessage(data, format, 0);
  }

  /**
   * Create a DATA message with inline data.
   *
   * @param data the raw data bytes
   * @param format the data format
   * @param numRecords number of records in the data (0 if unknown)
   * @return serialized protocol message bytes
   */
  public static byte[] createDataMessage(byte[] data, MicroBatchPayloadV1.Format format,
      long numRecords) throws IOException {
    MicroBatchPayloadV1 payload =
        MicroBatchPayloadV1.createData(data, format, (int) numRecords);
    return createMessage(CURRENT_VERSION, payload.toJsonBytes());
  }

  /**
   * Create a protocol message with the given version and payload.
   */
  private static byte[] createMessage(int version, byte[] payloadBytes) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream(1 + payloadBytes.length);
    out.write(version);
    out.write(payloadBytes);
    return out.toByteArray();
  }

  /**
   * Check if the given bytes look like a valid MicroBatch protocol message.
   * This performs a quick check without full parsing.
   *
   * @param bytes the bytes to check
   * @return true if the bytes appear to be a MicroBatch protocol message
   */
  public static boolean isProtocol(byte[] bytes) {
    if (bytes == null || bytes.length < 2) {
      return false;
    }
    int version = Byte.toUnsignedInt(bytes[0]);
    // Check for known versions and that payload starts with '{'
    return version == VERSION_1 && bytes[1] == '{';
  }

  // Getters

  public int getVersion() {
    return _version;
  }

  /**
   * Get the V1 payload. Only valid if version == 1.
   *
   * @return the V1 payload
   * @throws IllegalStateException if version is not 1
   */
  public MicroBatchPayloadV1 getPayloadV1() {
    if (_version != VERSION_1) {
      throw new IllegalStateException(
          "Cannot get V1 payload for version " + _version);
    }
    return _payloadV1;
  }

  // Convenience methods that delegate to the payload

  /**
   * Get the message type (URI or DATA).
   */
  public MicroBatchPayloadV1.Type getType() {
    return _payloadV1.getType();
  }

  /**
   * Get the data format (AVRO, PARQUET, JSON).
   */
  public MicroBatchPayloadV1.Format getFormat() {
    return _payloadV1.getFormat();
  }

  /**
   * Get the URI (for type=URI).
   */
  public String getUri() {
    return _payloadV1.getUri();
  }

  /**
   * Get the decoded data bytes (for type=DATA).
   */
  public byte[] getData() {
    return _payloadV1.getData();
  }

  /**
   * Get the number of records (0 if unknown).
   */
  public int getNumRecords() {
    return _payloadV1.getNumRecords();
  }
}
