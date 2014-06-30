package com.linkedin.pinot.server.conf;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;

import com.linkedin.pinot.query.config.QueryExecutorConfig;


/**
 * The config used for Server.
 * 
 * @author xiafu
 *
 */
public class ServerConf {

  private Configuration _serverConf;

  public ServerConf(Configuration serverConfig) {
    _serverConf = serverConfig;
  }

  public void init(Configuration serverConfig) {
    _serverConf = serverConfig;
  }

  public InstanceDataManagerConfig buildInstanceDataManagerConfig() throws ConfigurationException {
    return new InstanceDataManagerConfig(_serverConf.subset("pinot"));

  }

  public QueryExecutorConfig buildQueryExecutorConfig() {
    return new QueryExecutorConfig(_serverConf.subset("pinot"));
  }

}
