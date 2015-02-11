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
