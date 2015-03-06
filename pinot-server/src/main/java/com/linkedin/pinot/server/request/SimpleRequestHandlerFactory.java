/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.server.request;

import com.linkedin.pinot.common.metrics.ServerMetrics;
import com.linkedin.pinot.common.query.QueryExecutor;
import com.linkedin.pinot.transport.netty.NettyServer.RequestHandler;
import com.linkedin.pinot.transport.netty.NettyServer.RequestHandlerFactory;


/**
 * A simple implementation of RequestHandlerFactory.
 * Only return a SimpleRequestHandler.
 * 
 * @author xiafu
 *
 */
public class SimpleRequestHandlerFactory implements RequestHandlerFactory {

  private QueryExecutor _queryExecutor;

  private ServerMetrics _serverMetrics;

  public SimpleRequestHandlerFactory() {

  }

  public SimpleRequestHandlerFactory(QueryExecutor queryExecutor, ServerMetrics serverMetrics) {
    _queryExecutor = queryExecutor;
    _serverMetrics = serverMetrics;
  }

  public void init(QueryExecutor queryExecutor) {
    _queryExecutor = queryExecutor;
  }

  @Override
  public RequestHandler createNewRequestHandler() {
    return new SimpleRequestHandler(_queryExecutor, _serverMetrics);
  }

}
