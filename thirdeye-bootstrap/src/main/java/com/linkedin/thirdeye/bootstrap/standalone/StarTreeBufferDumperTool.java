package com.linkedin.thirdeye.bootstrap.standalone;

import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.StarTreeRecord;
import com.linkedin.thirdeye.api.StarTreeRecordStore;
import com.linkedin.thirdeye.api.StarTreeRecordStoreFactory;
import com.linkedin.thirdeye.impl.StarTreeRecordStoreFactoryFixedCircularBufferImpl;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.util.Properties;
import java.util.UUID;

public class StarTreeBufferDumperTool
{
  private static final Logger LOG = LoggerFactory.getLogger(StarTreeBufferDumperTool.class);

  public static void main(String[] args) throws Exception
  {
    if (args.length != 3)
    {
      throw new IllegalArgumentException("usage: config.json rootDir nodeId");
    }

    StarTreeConfig config = StarTreeConfig.fromJson(new ObjectMapper().readTree(new FileInputStream(args[0])));

    Properties props = new Properties();
    props.put("rootDir", args[1]);

    StarTreeRecordStoreFactory recordStoreFactory = new StarTreeRecordStoreFactoryFixedCircularBufferImpl();
    recordStoreFactory.init(config.getDimensionNames(), config.getMetricNames(), props);

    StarTreeRecordStore recordStore = recordStoreFactory.createRecordStore(UUID.fromString(args[2]));
    recordStore.open();

    for (StarTreeRecord record : recordStore)
    {
      LOG.info("{}", record);
    }
  }
}
