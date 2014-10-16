package com.linkedin.pinot.server.starter.helix;

import java.util.Iterator;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;

import com.linkedin.pinot.server.conf.ServerConf;


public class DefaultHelixStarterServerConfig {

  public static ServerConf getDefaultHelixServerConfig(Configuration externalConfigs) {
    Configuration defaultConfigs = loadDefaultServerConf();
    @SuppressWarnings("unchecked")
    Iterator<String> iterable = externalConfigs.getKeys();
    while (iterable.hasNext()) {
      String key = iterable.next();
      defaultConfigs.addProperty(key, externalConfigs.getProperty(key));
    }
    return new ServerConf(defaultConfigs);
  }

  public static Configuration loadDefaultServerConf() {
    Configuration serverConf = new PropertiesConfiguration();
    serverConf.addProperty("pinot.server.instance.dataDir", "/tmp/pinot/test/index");
    serverConf.addProperty("pinot.server.instance.segmentTarDir", "/tmp/pinot/test/segmentTar");

    serverConf.addProperty("pinot.server.instance.readMode", "heap");
    serverConf.addProperty("pinot.server.instance.data.manager.class",
        "com.linkedin.pinot.core.data.manager.HelixInstanceDataManager");
    serverConf.addProperty("pinot.server.instance.segment.metadata.loader.class",
        "com.linkedin.pinot.core.indexsegment.columnar.ColumnarSegmentMetadataLoader");

    // query executor parameters
    serverConf.addProperty("pinot.server.query.executor.pruner.class", "TableNameSegmentPruner");
    serverConf.addProperty("pinot.server.query.executor.pruner.TableNameSegmentPruner.id", "0");
    serverConf.addProperty("pinot.server.query.executor.timeout", "150000");
    serverConf.addProperty("pinot.server.query.executor.class",
        "com.linkedin.pinot.core.query.executor.ServerQueryExecutorV1Impl");

    // request handler factory parameters
    serverConf.addProperty("pinot.server.requestHandlerFactory.class",
        "com.linkedin.pinot.server.request.SimpleRequestHandlerFactory");

    // netty port
    serverConf.addProperty("pinot.server.netty.port", "8098");

    return serverConf;
  }

}
