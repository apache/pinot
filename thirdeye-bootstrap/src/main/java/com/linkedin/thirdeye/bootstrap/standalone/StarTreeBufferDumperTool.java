package com.linkedin.thirdeye.bootstrap.standalone;

import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.StarTreeRecord;
import com.linkedin.thirdeye.api.StarTreeRecordStore;
import com.linkedin.thirdeye.api.StarTreeRecordStoreFactory;
import com.linkedin.thirdeye.impl.StarTreeRecordStoreFactoryFixedCircularBufferImpl;
import com.linkedin.thirdeye.impl.StarTreeRecordStoreFixedCircularBufferImpl;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;

public class StarTreeBufferDumperTool
{
  private static final Logger LOG = LoggerFactory.getLogger(StarTreeBufferDumperTool.class);

  public static void main(String[] args) throws Exception
  {
    if (args.length != 4)
    {
      throw new IllegalArgumentException("usage: config.json rootDir nodeId raw");
    }

    StarTreeConfig config = StarTreeConfig.fromJson(new ObjectMapper().readTree(new FileInputStream(args[0])));


    if (Boolean.valueOf(args[3]))
    {
      int[] currentDimensions = new int[config.getDimensionNames().size()];
      long[] currentMetrics = new long[config.getMetricNames().size()];

      File file = new File(args[1], args[2] + ".buf");
      FileChannel fileChannel = new RandomAccessFile(file, "r").getChannel();
      ByteBuffer byteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, file.length());

      byteBuffer.rewind();
      while (byteBuffer.position() < byteBuffer.limit())
      {
        int bucket = byteBuffer.getInt();

        for (int i = 0; i < config.getDimensionNames().size(); i++)
        {
          currentDimensions[i] = byteBuffer.getInt();
        }

        long currentTime = byteBuffer.getLong();

        for (int i = 0; i < config.getMetricNames().size(); i++)
        {
          currentMetrics[i] = byteBuffer.getLong();
        }

        LOG.info("{}:{}:{}:{}", bucket, Arrays.toString(currentDimensions), currentTime, Arrays.toString(currentMetrics));
      }
    }
    else
    {
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
}
