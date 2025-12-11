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
package org.apache.pinot.query.runtime.context;

import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nullable;
import org.apache.pinot.query.runtime.operator.factory.QueryOperatorFactoryProvider;
import org.apache.pinot.spi.config.instance.InstanceType;


/**
 * The <code>QueryRuntimeContext</code> class is a singleton class which contains query runtime related context.
 */
public class QueryRuntimeContext {
  private static final QueryRuntimeContext INSTANCE = new QueryRuntimeContext();

  private QueryRuntimeContext() {
  }

  public static QueryRuntimeContext getInstance() {
    return INSTANCE;
  }

  // Must be set during initialization; null indicates no provider configured yet.
  @Nullable
  private QueryOperatorFactoryProvider _queryOperatorFactoryProvider;
  private final Map<InstanceType, SslContext> _clientGrpcSslContexts = new ConcurrentHashMap<>();
  private final Map<InstanceType, SslContext> _serverGrpcSslContexts = new ConcurrentHashMap<>();

  /**
   * Returns the configured provider or null if none has been set yet.
   */
  @Nullable
  public QueryOperatorFactoryProvider getQueryOperatorFactoryProvider() {
    return _queryOperatorFactoryProvider;
  }

  public void setQueryOperatorFactoryProvider(QueryOperatorFactoryProvider queryOperatorFactoryProvider) {
    _queryOperatorFactoryProvider =
        Objects.requireNonNull(queryOperatorFactoryProvider, "queryOperatorFactoryProvider must be set");
  }

  @Nullable
  public SslContext getClientGrpcSslContext(InstanceType instanceType) {
    return _clientGrpcSslContexts.get(instanceType);
  }

  public void setClientGrpcSslContext(InstanceType instanceType, SslContext clientGrpcSslContext) {
    Objects.requireNonNull(instanceType, "instanceType must be set");
    _clientGrpcSslContexts.put(instanceType,
        Objects.requireNonNull(clientGrpcSslContext, "clientGrpcSslContext must be set"));
  }

  @Nullable
  public SslContext getServerGrpcSslContext(InstanceType instanceType) {
    return _serverGrpcSslContexts.get(instanceType);
  }

  public void setServerGrpcSslContext(InstanceType instanceType, SslContext serverGrpcSslContext) {
    Objects.requireNonNull(instanceType, "instanceType must be set");
    _serverGrpcSslContexts.put(instanceType,
        Objects.requireNonNull(serverGrpcSslContext, "serverGrpcSslContext must be set"));
  }
}
