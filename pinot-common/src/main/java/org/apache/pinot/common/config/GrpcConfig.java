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
import org.apache.pinot.common.utils.tls.TlsUtils;
import org.apache.pinot.spi.env.PinotConfiguration;


public class GrpcConfig {
  public static final String GRPC_TLS_PREFIX = "tls";
  public static final String CONFIG_USE_PLAIN_TEXT = "usePlainText";
  public static final String CONFIG_MAX_INBOUND_MESSAGE_BYTES_SIZE = "maxInboundMessageSizeBytes";

  private static final String CONFIG_CHANNEL_SHUTDOWN_TIMEOUT_SECONDS = "channelShutdownTimeoutSeconds";
  private static final int DEFAULT_CHANNEL_SHUTDOWN_TIMEOUT_SECONDS = 10;

  // KeepAlive configs
  private static final String CONFIG_CHANNEL_KEEP_ALIVE_TIME_SECONDS = "channelKeepAliveTimeSeconds";
  private static final int DEFAULT_CHANNEL_KEEP_ALIVE_TIME_SECONDS = -1; // Set value > 0 to enable keep alive

  private static final String CONFIG_CHANNEL_KEEP_ALIVE_TIMEOUT_SECONDS = "channelKeepAliveTimeoutSeconds";
  private static final int DEFAULT_CHANNEL_KEEP_ALIVE_TIMEOUT_SECONDS = 20; // 20 seconds

  private static final String CONFIG_CHANNEL_KEEP_ALIVE_WITHOUT_CALLS = "channelKeepAliveWithoutCalls";
  private static final boolean DEFAULT_CHANNEL_KEEP_ALIVE_WITHOUT_CALLS = true;

  // Default max message size to 128MB
  public static final int DEFAULT_MAX_INBOUND_MESSAGE_BYTES_SIZE = 128 * 1024 * 1024;
  // Default use plain text for transport
  private static final String DEFAULT_IS_USE_PLAIN_TEXT = "true";

  public static final String CONFIG_QUERY_WORKER_THREADS = "queryWorkerThreads";

  // memory usage threshold that triggers request throttling
  public static final String REQUEST_THROTTLING_MEMORY_THRESHOLD_BYTES = "requestThrottlingMemoryThresholdBytes";
  // Default threshold in bytes (16GB)
  public static final long DEFAULT_REQUEST_THROTTLING_MEMORY_THRESHOLD_BYTES = 16 * 1024 * 1024 * 1024L;

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

  public int getChannelShutdownTimeoutSecond() {
    return _pinotConfig.getProperty(CONFIG_CHANNEL_SHUTDOWN_TIMEOUT_SECONDS, DEFAULT_CHANNEL_SHUTDOWN_TIMEOUT_SECONDS);
  }

  public int getChannelKeepAliveTimeSeconds() {
    return _pinotConfig.getProperty(CONFIG_CHANNEL_KEEP_ALIVE_TIME_SECONDS, DEFAULT_CHANNEL_KEEP_ALIVE_TIME_SECONDS);
  }

  public int getChannelKeepAliveTimeoutSeconds() {
    return _pinotConfig.getProperty(CONFIG_CHANNEL_KEEP_ALIVE_TIMEOUT_SECONDS,
        DEFAULT_CHANNEL_KEEP_ALIVE_TIMEOUT_SECONDS);
  }

  public boolean isChannelKeepAliveWithoutCalls() {
    return _pinotConfig.getProperty(CONFIG_CHANNEL_KEEP_ALIVE_WITHOUT_CALLS, DEFAULT_CHANNEL_KEEP_ALIVE_WITHOUT_CALLS);
  }

  public TlsConfig getTlsConfig() {
    return _tlsConfig;
  }

  public PinotConfiguration getPinotConfig() {
    return _pinotConfig;
  }

  public int getQueryWorkerThreads() {
    return _pinotConfig.getProperty(CONFIG_QUERY_WORKER_THREADS, Integer.class);
  }

  public long getRequestThrottlingMemoryThresholdBytes() {
    return _pinotConfig.getProperty(REQUEST_THROTTLING_MEMORY_THRESHOLD_BYTES, Long.class);
  }

  public boolean isRequestThrottlingMemroyThresholdSet() {
    return _pinotConfig.containsKey(REQUEST_THROTTLING_MEMORY_THRESHOLD_BYTES);
  }

  public boolean isQueryWorkerThreadsSet() {
    return _pinotConfig.containsKey(CONFIG_QUERY_WORKER_THREADS);
  }
}
