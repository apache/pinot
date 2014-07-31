package com.linkedin.pinot.server.starter;

import java.io.File;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.common.data.DataManager;
import com.linkedin.pinot.common.query.QueryExecutor;
import com.linkedin.pinot.server.conf.ServerConf;
import com.linkedin.pinot.transport.netty.NettyServer.RequestHandlerFactory;


/**
 * Initialize a ServerBuilder with serverConf file.
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

  /**
   * Construct from config file path
   * @param configFilePath Path to the config file
   * @throws Exception
   */
  public ServerBuilder(File configFilePath) throws Exception
  {
    _serverConfFile = configFilePath;
    if (!_serverConfFile.exists()) {
      LOGGER.error("configuration file: " + _serverConfFile.getAbsolutePath() + " does not exist.");
      throw new ConfigurationException("configuration file: " + _serverConfFile.getAbsolutePath() + " does not exist.");
    }

    // build _serverConf
    PropertiesConfiguration serverConf = new PropertiesConfiguration();
    serverConf.setDelimiterParsingDisabled(false);
    serverConf.load(_serverConfFile);
    _serverConf = new ServerConf(serverConf);
  }

  /**
   * Construct from config directory and a config file which resides under it
   * @param confDir Directory under which config file is present
   * @param file Config File
   * @throws Exception
   */
  public ServerBuilder(String confDir, String file) throws Exception {
    this(new File(confDir, file));
  }

  /**
   * Construct from config directory and default config file
   * @param confDir Directory under which pinot.properties file is present
   * @throws Exception
   */
  public ServerBuilder(String confDir) throws Exception {
    this(new File(confDir, PINOT_PROPERTIES));
  }

  //  public InstanceDataManager buildInstanceDataManager() throws ConfigurationException {
  //    InstanceDataManager instanceDataManager = InstanceDataManager.getInstanceDataManager();
  //    instanceDataManager.init(_serverConf.buildInstanceDataManagerConfig());
  //    return instanceDataManager;
  //  }

  public DataManager buildInstanceDataManager() throws InstantiationException,
      IllegalAccessException, ClassNotFoundException {
    String className = _serverConf.getInstanceDataManagerConfig().getString("instance.data.manager.class");
    LOGGER.info("Trying to Load Instance DataManager by Class : " + className);
    DataManager instanceDataManager = (DataManager) Class.forName(className).newInstance();
    instanceDataManager.init(_serverConf.getInstanceDataManagerConfig());
    return instanceDataManager;
  }

  public QueryExecutor buildQueryExecutor(DataManager instanceDataManager) throws InstantiationException,
      IllegalAccessException, ClassNotFoundException {
    String className = _serverConf.getInstanceDataManagerConfig().getString("query.executor.class");
    LOGGER.info("Trying to Load Query Executor by Class : " + className);
    QueryExecutor queryExecutor = (QueryExecutor) Class.forName(className).newInstance();
    queryExecutor.init(_serverConf.getQueryExecutorConfig(), instanceDataManager);
    return queryExecutor;
  }

  public RequestHandlerFactory buildRequestHandlerFactory(QueryExecutor queryExecutor) throws InstantiationException,
      IllegalAccessException, ClassNotFoundException {
    String className = _serverConf.getInstanceDataManagerConfig().getString("requestHandlerFactory.class");
    LOGGER.info("Trying to Load Request Handler Factory by Class : " + className);
    RequestHandlerFactory requestHandlerFactory = (RequestHandlerFactory) Class.forName(className).newInstance();
    requestHandlerFactory.init(queryExecutor);
    return requestHandlerFactory;
  }

}
