package org.apache.pinot.broker.broker;

import java.io.IOException;
import javax.inject.Inject;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.PreMatching;
import javax.ws.rs.ext.Provider;
import org.apache.pinot.common.config.provider.TableCache;
import org.apache.pinot.common.utils.DatabaseUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Provider
@PreMatching
public class BrokerRequestFilter implements ContainerRequestFilter {

  public static final Logger LOGGER = LoggerFactory.getLogger(BrokerRequestFilter.class);

  @Inject
  private TableCache _tableCache;

  @Override
  public void filter(ContainerRequestContext requestContext)
      throws IOException {
    // uses the database name from header to build the actual table name
    DatabaseUtils.translateTableNameQueryParam(requestContext, _tableCache);
  }
}
