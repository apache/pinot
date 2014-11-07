package com.linkedin.thirdeye.bootstrap.standalone;

import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.impl.StarTreeRecordStoreCircularBufferImpl;
import com.linkedin.thirdeye.impl.StarTreeRecordStoreFactoryCircularBufferImpl;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.File;
import java.io.FileInputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class StarTreeBufferDumperTool
{
  public static void main(String[] args) throws Exception
  {
    if (args.length != 4)
    {
      throw new IllegalArgumentException("usage: config.json rootDir nodeId numBuckets");
    }

    StarTreeConfig config = StarTreeConfig.fromJson(new ObjectMapper().readTree(new FileInputStream(args[0])));

    File file = new File(args[1], args[2] + StarTreeRecordStoreFactoryCircularBufferImpl.BUFFER_SUFFIX);

    FileChannel fileChannel = new RandomAccessFile(file, "r").getChannel();

    ByteBuffer buffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, file.length());

    StarTreeRecordStoreCircularBufferImpl.dumpBuffer(buffer,
                                                     System.out,
                                                     config.getDimensionNames(),
                                                     config.getMetricNames(),
                                                     Integer.valueOf(args[3]));
  }
}
