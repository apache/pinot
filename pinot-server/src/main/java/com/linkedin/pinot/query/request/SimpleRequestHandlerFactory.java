package com.linkedin.pinot.query.request;

import com.linkedin.pinot.query.executor.QueryExecutor;
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

  public SimpleRequestHandlerFactory(QueryExecutor queryExecutor) {
    _queryExecutor = queryExecutor;
  }

  @Override
  public RequestHandler createNewRequestHandler() {
    return new SimpleRequestHandler(_queryExecutor);
  }

}
