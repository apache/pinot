package com.linkedin.thirdeye.tools;

import com.fasterxml.jackson.databind.util.ByteBufferBackedInputStream;
import com.linkedin.thirdeye.api.StarTreeConstants;
import org.apache.avro.mapred.Pair;
import org.apache.avro.mapred.SequenceFileReader;
import org.apache.avro.util.ByteBufferInputStream;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.UUID;

public class StarTreeExtractorTool
{
  private static final Logger LOG = LoggerFactory.getLogger(StarTreeExtractorTool.class);

  private static void extract(File inputFile, File outputDir) throws IOException
  {
    SequenceFileReader<NullWritable, ByteBuffer> fileReader
            = new SequenceFileReader<NullWritable, ByteBuffer>(inputFile);

    while (fileReader.hasNext())
    {
      Pair<NullWritable, ByteBuffer> next = fileReader.next();
      ByteBuffer bytes = next.value();
      DataInputStream dis = new DataInputStream(new ByteBufferBackedInputStream(bytes));
      while (dis.available() > 0)
      {
        // Read
        UUID nodeId = UUID.fromString(new String(readNext(dis)));
        byte[] index = readNext(dis);
        byte[] buffer = readNext(dis);

        // Write index
        File indexFile = new File(outputDir, nodeId + StarTreeConstants.INDEX_FILE_SUFFIX);
        if (!indexFile.createNewFile())
        {
          throw new IOException(indexFile + " already exists");
        }
        IOUtils.copy(new ByteArrayInputStream(index), new FileOutputStream(indexFile));

        // Write buffer
        File bufferFile = new File(outputDir, nodeId + StarTreeConstants.BUFFER_FILE_SUFFIX);
        if (!bufferFile.createNewFile())
        {
          throw new IOException(bufferFile + " already exists");
        }
        IOUtils.copy(new ByteArrayInputStream(buffer), new FileOutputStream(bufferFile));

        LOG.info("Extracted node {}", nodeId);
      }
      dis.close();
    }
    fileReader.close();
  }

  private static byte[] readNext(DataInputStream dis) throws IOException
  {
    int size = dis.readInt();
    byte[] bytes = new byte[size];
    int read = dis.read(bytes);
    if (read != size) {
      throw new IOException("Read " + read + " bytes but expected " + size);
    }
    return bytes;
  }

  public static void main(String[] args) throws Exception
  {
    if (args.length < 1)
    {
      throw new IllegalArgumentException("usage: outputDir inputFile(s) ...");
    }

    File outputDir = new File(args[0]);

    for (int i = 1; i < args.length; i++)
    {
      File inputFile = new File(args[i]);
      LOG.info("Extracting {} into {}", inputFile, outputDir);
      extract(inputFile, outputDir);
    }
  }
}
