package com.linkedin.pinot.server.starter;

import java.io.File;
import java.util.Map;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.query.executor.QueryExecutor;
import com.linkedin.pinot.query.request.SimpleRequestHandlerFactory;
import com.linkedin.pinot.server.conf.ServerConf;
import com.linkedin.pinot.server.instance.InstanceDataManager;
import com.linkedin.pinot.transport.netty.NettyServer.RequestHandlerFactory;


/**
 * Initial a ServerBuilder with serverConf file.
 * 
 * @author xiafu
 *
 */
public class ServerBuilder {

  private static Logger LOGGER = LoggerFactory.getLogger(ServerBuilder.class);
  public static final String PINOT_PROPERTIES = "pinot.properties";

  private final File _serverConfFile;
  private final ServerConf _serverConf;

  public ServerConf getConfiguration() {
    return _serverConf;
  }

  public ServerBuilder(File confDir) throws Exception {
    this(confDir, null);
  }

  public ServerBuilder(File confDir, Map<String, Object> properties) throws Exception {
    _serverConfFile = new File(confDir, PINOT_PROPERTIES);
    if (!_serverConfFile.exists()) {
      LOGGER.error("configuration file: " + _serverConfFile.getAbsolutePath() + " does not exist.");
      throw new ConfigurationException("configuration file: " + _serverConfFile.getAbsolutePath() + " does not exist.");
    }
    // _serverConf
    Configuration ServerConf = new PropertiesConfiguration();
    ((PropertiesConfiguration) ServerConf).setDelimiterParsingDisabled(false);
    ((PropertiesConfiguration) ServerConf).load(_serverConfFile);
    _serverConf = new ServerConf(ServerConf);
  }

  public InstanceDataManager buildInstanceDataManager() throws ConfigurationException {
    InstanceDataManager instanceDataManager = InstanceDataManager.getInstanceDataManager();
    instanceDataManager.init(_serverConf.buildInstanceDataManagerConfig());
    return instanceDataManager;
  }

  public QueryExecutor buildQueryExecutor(InstanceDataManager instanceDataManager) {
    return new QueryExecutor(_serverConf.buildQueryExecutorConfig(), instanceDataManager);
  }

  public RequestHandlerFactory buildRequestHandlerFactory(QueryExecutor queryExecutor) {
    return new SimpleRequestHandlerFactory(queryExecutor);
  }

}
