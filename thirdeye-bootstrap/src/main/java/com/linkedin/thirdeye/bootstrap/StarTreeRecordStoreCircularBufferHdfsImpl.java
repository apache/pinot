package com.linkedin.thirdeye.bootstrap;

import com.linkedin.thirdeye.impl.StarTreeRecordStoreCircularBufferImpl;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class StarTreeRecordStoreCircularBufferHdfsImpl extends StarTreeRecordStoreCircularBufferImpl
{
  private final Path path;

  public StarTreeRecordStoreCircularBufferHdfsImpl(UUID nodeId,
                                                   Path path,
                                                   List<String> dimensionNames,
                                                   List<String> metricNames,
                                                   Map<String, Map<String, Integer>> forwardIndex,
                                                   int numTimeBuckets)
  {
    super(nodeId, null, dimensionNames, metricNames, forwardIndex, numTimeBuckets);
    this.path = path;
  }

  @Override
  public void open() throws IOException
  {
    synchronized (sync)
    {
      if (!isOpen)
      {
        isOpen = true;

        // Read buffer from HDFS
      }
    }
  }

  @Override
  public void close() throws IOException
  {
    synchronized (sync)
    {
      if (isOpen)
      {
        isOpen = false;

        // Flush buffer to HDFS (i.e. overwrite existing one)
      }
    }
  }
}
