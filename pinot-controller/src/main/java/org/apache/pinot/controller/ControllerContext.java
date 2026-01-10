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
package org.apache.pinot.controller;

import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;
import java.util.Objects;
import javax.annotation.Nullable;
import javax.net.ssl.SSLContext;
import org.apache.pinot.query.runtime.context.QueryRuntimeContext;
import org.apache.pinot.spi.config.instance.InstanceType;


/**
 * The <code>ControllerContext</code> class is a singleton class which contains all controller related context.
 */
public class ControllerContext {
  private static final ControllerContext INSTANCE = new ControllerContext();
  private final QueryRuntimeContext _queryRuntimeContext = QueryRuntimeContext.getInstance();

  private ControllerContext() {
  }

  public static ControllerContext getInstance() {
    return INSTANCE;
  }

  @Nullable
  private SSLContext _clientHttpsContext;
  @Nullable
  private SSLContext _serverHttpsContext;

  @Nullable
  public SslContext getClientGrpcSslContext() {
    return _queryRuntimeContext.getClientGrpcSslContext(InstanceType.CONTROLLER);
  }

  public void setClientGrpcSslContext(SslContext clientGrpcSslContext) {
    _queryRuntimeContext.setClientGrpcSslContext(InstanceType.CONTROLLER, clientGrpcSslContext);
  }

  @Nullable
  public SslContext getServerGrpcSslContext() {
    return _queryRuntimeContext.getServerGrpcSslContext(InstanceType.CONTROLLER);
  }

  public void setServerGrpcSslContext(SslContext serverGrpcSslContext) {
    _queryRuntimeContext.setServerGrpcSslContext(InstanceType.CONTROLLER, serverGrpcSslContext);
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
