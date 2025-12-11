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
package org.apache.pinot.server;

import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;
import java.util.Objects;
import javax.annotation.Nullable;
import javax.net.ssl.SSLContext;
import org.apache.pinot.query.runtime.context.QueryRuntimeContext;
import org.apache.pinot.query.runtime.operator.factory.QueryOperatorFactoryProvider;
import org.apache.pinot.spi.config.instance.InstanceType;


/**
 * The <code>ServerContext</code> class is a singleton class which contains all server related context.
 */
public class ServerContext {
  private static final ServerContext INSTANCE = new ServerContext();
  private final QueryRuntimeContext _queryRuntimeContext = QueryRuntimeContext.getInstance();

  private ServerContext() {
  }

  public static ServerContext getInstance() {
    return INSTANCE;
  }

  @Nullable
  private SSLContext _clientHttpsContext;
  @Nullable
  private SSLContext _serverHttpsContext;

  /**
   * Returns the configured provider or null if none has been set yet.
   */
  @Nullable
  public QueryOperatorFactoryProvider getQueryOperatorFactoryProvider() {
    return _queryRuntimeContext.getQueryOperatorFactoryProvider();
  }

  public void setQueryOperatorFactoryProvider(QueryOperatorFactoryProvider queryOperatorFactoryProvider) {
    _queryRuntimeContext.setQueryOperatorFactoryProvider(queryOperatorFactoryProvider);
  }

  @Nullable
  public SslContext getClientGrpcSslContext() {
    return _queryRuntimeContext.getClientGrpcSslContext(InstanceType.SERVER);
  }

  public void setClientGrpcSslContext(SslContext clientGrpcSslContext) {
    _queryRuntimeContext.setClientGrpcSslContext(InstanceType.SERVER, clientGrpcSslContext);
  }

  @Nullable
  public SslContext getServerGrpcSslContext() {
    return _queryRuntimeContext.getServerGrpcSslContext(InstanceType.SERVER);
  }

  public void setServerGrpcSslContext(SslContext serverGrpcSslContext) {
    _queryRuntimeContext.setServerGrpcSslContext(InstanceType.SERVER, serverGrpcSslContext);
  }

  @Nullable
  public SSLContext getClientHttpsContext() {
    return _clientHttpsContext;
  }

  public void setClientHttpsContext(SSLContext clientHttpsContext) {
    _clientHttpsContext = Objects.requireNonNull(clientHttpsContext, "clientHttpsContext must be set");
  }

  @Nullable
  public SSLContext getServerHttpsContext() {
    return _serverHttpsContext;
  }

  public void setServerHttpsContext(SSLContext serverHttpsContext) {
    _serverHttpsContext = Objects.requireNonNull(serverHttpsContext, "serverHttpsContext must be set");
  }
}
