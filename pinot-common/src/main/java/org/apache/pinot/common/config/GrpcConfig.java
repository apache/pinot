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
package org.apache.pinot.common.config;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.pinot.common.utils.TlsUtils;
import org.apache.pinot.spi.env.PinotConfiguration;


public class GrpcConfig {
  public static final String GRPC_TLS_PREFIX = "tls";
  public static final String CONFIG_USE_PLAIN_TEXT = "usePlainText";
  public static final String CONFIG_MAX_INBOUND_MESSAGE_BYTES_SIZE = "maxInboundMessageSizeBytes";
  // Default max message size to 128MB
  public static final int DEFAULT_MAX_INBOUND_MESSAGE_BYTES_SIZE = 128 * 1024 * 1024;

  private final int _maxInboundMessageSizeBytes;
  private final boolean _usePlainText;
  private final TlsConfig _tlsConfig;
  private final PinotConfiguration _pinotConfig;

  public static GrpcConfig buildGrpcQueryConfig(PinotConfiguration pinotConfig) {
    return new GrpcConfig();
  }

  public GrpcConfig() {
    this(DEFAULT_MAX_INBOUND_MESSAGE_BYTES_SIZE, true);
  }

  public GrpcConfig(int maxInboundMessageSizeBytes, boolean usePlainText) {
    this(ImmutableMap.of(CONFIG_MAX_INBOUND_MESSAGE_BYTES_SIZE, maxInboundMessageSizeBytes, CONFIG_USE_PLAIN_TEXT,
        usePlainText));
  }

  public GrpcConfig(Map<String, Object> configMap) {
    _pinotConfig = new PinotConfiguration(configMap);
    _maxInboundMessageSizeBytes =
        _pinotConfig.getProperty(CONFIG_MAX_INBOUND_MESSAGE_BYTES_SIZE, DEFAULT_MAX_INBOUND_MESSAGE_BYTES_SIZE);
    _usePlainText = Boolean.valueOf(configMap.get(CONFIG_USE_PLAIN_TEXT).toString());
    _tlsConfig = TlsUtils.extractTlsConfig(_pinotConfig, GRPC_TLS_PREFIX);
  }

  // Allow get customized configs.
  public Object get(String key) {
    return _pinotConfig.getProperty(key);
  }

  public int getMaxInboundMessageSizeBytes() {
    return _maxInboundMessageSizeBytes;
  }

  public boolean isUsePlainText() {
    return _usePlainText;
  }

  public TlsConfig getTlsConfig() {
    return _tlsConfig;
  }

  public PinotConfiguration getPinotConfig() {
    return _pinotConfig;
  }
}
