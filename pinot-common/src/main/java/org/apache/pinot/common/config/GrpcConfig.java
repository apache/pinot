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
import org.apache.pinot.common.tls.TlsUtils;
import org.apache.pinot.spi.env.PinotConfiguration;


public class GrpcConfig {
  public static final String GRPC_TLS_PREFIX = "tls";
  public static final String CONFIG_USE_PLAIN_TEXT = "usePlainText";
  public static final String CONFIG_MAX_INBOUND_MESSAGE_BYTES_SIZE = "maxInboundMessageSizeBytes";
  // Default max message size to 128MB
  public static final int DEFAULT_MAX_INBOUND_MESSAGE_BYTES_SIZE = 128 * 1024 * 1024;
  // Default use plain text for transport
  private static final String DEFAULT_IS_USE_PLAIN_TEXT = "true";

  private final TlsConfig _tlsConfig;
  private final PinotConfiguration _pinotConfig;

  public static GrpcConfig buildGrpcQueryConfig(PinotConfiguration pinotConfig) {
    return new GrpcConfig(pinotConfig);
  }

  public GrpcConfig(PinotConfiguration pinotConfig) {
    _pinotConfig = pinotConfig;
    _tlsConfig = TlsUtils.extractTlsConfig(_pinotConfig, GRPC_TLS_PREFIX);
  }

  public GrpcConfig(Map<String, Object> configMap) {
    this(new PinotConfiguration(configMap));
  }

  public GrpcConfig(int maxInboundMessageSizeBytes, boolean usePlainText) {
    this(ImmutableMap.of(CONFIG_MAX_INBOUND_MESSAGE_BYTES_SIZE, maxInboundMessageSizeBytes, CONFIG_USE_PLAIN_TEXT,
        usePlainText));
  }

  // Allow get customized configs.
  public Object get(String key) {
    return _pinotConfig.getProperty(key);
  }

  public int getMaxInboundMessageSizeBytes() {
    return _pinotConfig.getProperty(CONFIG_MAX_INBOUND_MESSAGE_BYTES_SIZE, DEFAULT_MAX_INBOUND_MESSAGE_BYTES_SIZE);
  }

  public boolean isUsePlainText() {
    return Boolean.parseBoolean(_pinotConfig.getProperty(CONFIG_USE_PLAIN_TEXT, DEFAULT_IS_USE_PLAIN_TEXT));
  }

  public TlsConfig getTlsConfig() {
    return _tlsConfig;
  }

  public PinotConfiguration getPinotConfig() {
    return _pinotConfig;
  }
}
