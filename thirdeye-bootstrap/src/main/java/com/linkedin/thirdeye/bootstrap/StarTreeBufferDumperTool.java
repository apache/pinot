package com.linkedin.thirdeye.bootstrap;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.impl.StarTreeRecordStoreCircularBufferImpl;
import com.linkedin.thirdeye.impl.StarTreeRecordStoreFactoryCircularBufferImpl;

import java.io.File;
import java.io.FileInputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class StarTreeBufferDumperTool
{
  public static void main(String[] args) throws Exception
  {
    if (args.length != 2)
    {
      throw new IllegalArgumentException("usage: config.json nodeId");
    }

    JsonNode jsonNode = new ObjectMapper().readTree(new FileInputStream(args[0]));
    JsonNode rootDir = jsonNode.get("recordStoreFactoryConfig").get("rootDir");
    JsonNode timeBuckets = jsonNode.get("recordStoreFactoryConfig").get("numTimeBuckets");

    StarTreeConfig config = StarTreeConfig.fromJson(jsonNode);

    File file = new File(rootDir.asText(), args[1] + StarTreeRecordStoreFactoryCircularBufferImpl.BUFFER_SUFFIX);

    FileChannel fileChannel = new RandomAccessFile(file, "r").getChannel();

    ByteBuffer buffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, file.length());

    StarTreeRecordStoreCircularBufferImpl.dumpBuffer(buffer,
                                                     System.out,
                                                     config.getDimensionNames(),
                                                     config.getMetricNames(),
                                                     timeBuckets.asInt());
  }
}
