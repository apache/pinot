package com.linkedin.pinot.server.conf;

import org.apache.commons.configuration.Configuration;


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

  public Configuration getInstanceDataManagerConfig() {
    return _serverConf.subset("pinot");
  }

  public Configuration getQueryExecutorConfig() {
    return _serverConf.subset("pinot");
  }

  //
  //  public InstanceDataManagerConfig buildInstanceDataManagerConfig() throws ConfigurationException {
  //    return new InstanceDataManagerConfig(_serverConf.subset("pinot"));
  //  }

//  public QueryExecutorConfig buildQueryExecutorConfig() {
//    return new QueryExecutorConfig(_serverConf.subset("pinot"));
//  }

}
