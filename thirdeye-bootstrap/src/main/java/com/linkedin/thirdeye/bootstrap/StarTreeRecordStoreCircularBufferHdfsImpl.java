package com.linkedin.thirdeye.bootstrap;

import com.linkedin.thirdeye.impl.StarTreeRecordStoreCircularBufferImpl;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class StarTreeRecordStoreCircularBufferHdfsImpl extends StarTreeRecordStoreCircularBufferImpl
{
  private final Path path;
  private final Configuration conf;

  public StarTreeRecordStoreCircularBufferHdfsImpl(UUID nodeId,
                                                   Path path,
                                                   Configuration conf,
                                                   List<String> dimensionNames,
                                                   List<String> metricNames,
                                                   Map<String, Map<String, Integer>> forwardIndex,
                                                   int numTimeBuckets)
  {
    super(nodeId, null, dimensionNames, metricNames, forwardIndex, numTimeBuckets);
    this.path = path;
    this.conf = conf;
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
        FileSystem fileSystem = FileSystem.get(path.toUri(), conf);
        InputStream inputStream = fileSystem.open(path);
        byte[] bytes = IOUtils.toByteArray(inputStream);
        buffer = ByteBuffer.wrap(bytes);
        fileSystem.close();
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
        FileSystem fileSystem = FileSystem.get(path.toUri(), conf);
        OutputStream outputStream = fileSystem.create(path, true);
        byte[] bytes = buffer.array();
        IOUtils.write(bytes, outputStream);
        outputStream.flush();
        fileSystem.close();
      }
    }
  }
}
