package com.linkedin.thirdeye.impl.storage;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.api.MetricSchema;
import com.linkedin.thirdeye.api.MetricSpec;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.StarTreeConstants;
import com.linkedin.thirdeye.api.TimeRange;
import com.linkedin.thirdeye.impl.NumberUtils;

/**
 * Converts the Fixed size buffer format data directory to variable size buffer format
 * @author kgopalak
 */
public class FixedToVariableFormatConvertor
{
  private static final Logger LOGGER = LoggerFactory
      .getLogger(FixedToVariableFormatConvertor.class);

  private String dataDirectory;
  private StarTreeConfig config;
  private MetricSchema metricSchema;
  private int numMetrics;

  private IndexMetadata indexMetadata;

  FixedToVariableFormatConvertor(String dataDirectory) throws IOException
  {
    this.dataDirectory = dataDirectory;
    config =
        StarTreeConfig.decode(new FileInputStream(new File(dataDirectory,
            StarTreeConstants.CONFIG_FILE_NAME)));
    metricSchema = MetricSchema.fromMetricSpecs(config.getMetrics());
    numMetrics = config.getMetrics().size();
    InputStream indexMetadataFile =
        new FileInputStream(new File(dataDirectory, StarTreeConstants.METADATA_FILE_NAME));
    Properties indexMetadataProps = new Properties();
    indexMetadataProps.load(indexMetadataFile);
    indexMetadataFile.close();
    indexMetadata = IndexMetadata.fromProperties(indexMetadataProps);

  }

  FixedToVariableFormatConvertor(File inputDataDirectory) throws IOException
  {
    this(inputDataDirectory.getAbsolutePath());
  }

  public void convert() throws IOException
  {
    if (!indexMetadata.getIndexFormat().equals(IndexFormat.FIXED_SIZE))
    {
      LOGGER.warn("Index format is not in FIXED_SIZE format, aborting conversion for directory:{}",
          dataDirectory);
      return;
    }
    File inputDir = new File(dataDirectory, StarTreeConstants.METRIC_STORE);
    FilenameFilter filter = new FilenameFilter()
    {

      @Override
      public boolean accept(File dir, String name)
      {
        return name.endsWith(StarTreeConstants.BUFFER_FILE_SUFFIX);
      }
    };
    File[] listFiles = inputDir.listFiles(filter);
    for (File bufferFile : listFiles)
    {
      LOGGER.debug("START:Converting bufferfile:{} original size:{}"
          , bufferFile.getAbsolutePath(), bufferFile.length());
      convert(bufferFile);
    }
    indexMetadata.setIndexFormat(IndexFormat.VARIABLE_SIZE);
    FileOutputStream fos =
        new FileOutputStream(new File(dataDirectory, StarTreeConstants.METADATA_FILE_NAME));
    indexMetadata.toProperties().store(fos,
        "This segment was generated during conversion from Fixed Format to Variable Size Format");
  }

  /**
   * Converts a buffer file from fixed format to variable format
   * @param bufferFile
   * @throws IOException
   */
  public void convert(File bufferFile) throws IOException
  {
    String indexFilePath =
        bufferFile.getAbsolutePath().replace(StarTreeConstants.BUFFER_FILE_SUFFIX,
            StarTreeConstants.INDEX_FILE_SUFFIX);
    File indexFile = new File(indexFilePath);
    if (!indexFile.exists())
    {
      throw new IllegalArgumentException(
          "Invalid directory, Metric Index File missing for buffer file:" + bufferFile);
    }
    // read the metric buffer and metric Index File
    List<MetricIndexEntry> metricIndexEntries = StorageUtils.readMetricIndex(indexFile);
    RandomAccessFile randomAccessFile = new RandomAccessFile(bufferFile, "r");
    FileChannel channel = randomAccessFile.getChannel();
    ByteBuffer metricBuffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, bufferFile.length());
    metricBuffer.order(ByteOrder.BIG_ENDIAN);
    Map<MetricIndexEntry, List<MetricTimeSeries>> map =
        new LinkedHashMap<MetricIndexEntry, List<MetricTimeSeries>>();
    for (MetricIndexEntry indexEntry : metricIndexEntries)
    {
      List<MetricTimeSeries> timeSeriesList = new ArrayList<MetricTimeSeries>();
      metricBuffer.position(indexEntry.getStartOffset());
      ByteBuffer slicedBuffer = metricBuffer.slice();
      slicedBuffer.limit(indexEntry.getLength());
      if (indexEntry.getLength() == 0)
      {
        map.put(indexEntry, timeSeriesList);
        continue;
      }
      TimeRange timeRange = indexEntry.getTimeRange();
      long bucketSize =
          (timeRange.totalBuckets()) * (Long.SIZE / 8 + metricSchema.getRowSizeInBytes());
      float numLogicalOffsets = indexEntry.getLength() / bucketSize;
      // System.out.println(numLogicalOffsets);
      List<Number> numbers = new ArrayList<Number>(numMetrics);

      for (int logicalOffset = 0; logicalOffset < numLogicalOffsets; logicalOffset++)
      {

        MetricTimeSeries timeSeries = new MetricTimeSeries(metricSchema);
        for (long time = timeRange.getStart(); time <= timeRange.getEnd(); time++)
        {
          // must be same as the time variable we are iterating on
          long timeFromBuffer = slicedBuffer.getLong();
          assert timeFromBuffer == time;
          boolean skip = true;
          numbers.clear();
          for (MetricSpec metricSpec : config.getMetrics())
          {
            Number value = NumberUtils.readFromBuffer(slicedBuffer, metricSpec.getType());
            numbers.add(value);
            if (!NumberUtils.isZero(value, metricSpec.getType()))
            {
              skip = false;
            }
          }
          if (!skip)
          {
            for (int i = 0; i < numMetrics; i++)
            {
              timeSeries.set(time, config.getMetrics().get(i).getName(), numbers.get(i));
            }
          }
        }
        assert (slicedBuffer.position() == indexEntry.getLength());// we should have read all the
                                                                   // data in this entry
        timeSeriesList.add(timeSeries);
      }
      map.put(indexEntry, timeSeriesList);
    }

    randomAccessFile.close();
    // generate variable size format
    List<MetricIndexEntry> newMetricIndexEntries = new ArrayList<MetricIndexEntry>();
    int startOffset = 0;
    // delete original files
    bufferFile.delete();
    indexFile.delete();
    FileOutputStream fos = new FileOutputStream(bufferFile);
    for (MetricIndexEntry indexEntry : map.keySet())
    {
      List<MetricTimeSeries> timeSeriesList = map.get(indexEntry);
      ByteBuffer newMetricBuffer =
          VariableSizeBufferUtil.createMetricBuffer(config, timeSeriesList);
      byte[] bytes = newMetricBuffer.array();
      int length = bytes.length;
      MetricIndexEntry newIndexEntry =
          new MetricIndexEntry(indexEntry.getNodeId(), indexEntry.getFileId(), startOffset, length,
              indexEntry.getTimeRange());
      newMetricIndexEntries.add(newIndexEntry);
      startOffset += length;
      fos.write(bytes);
    }
    fos.close();
    VariableSizeBufferUtil.writeObjects(newMetricIndexEntries, indexFile);

  }

  /**
   * USAGE: FixedToVariableFormatConvertor <dataDirectory>
   * @param args
   * @throws Exception
   */
  public static void main(String[] args) throws Exception
  {
    String inputDataDirectory = args[0];
    FixedToVariableFormatConvertor convertor =
        new FixedToVariableFormatConvertor(inputDataDirectory);
    convertor.convert();
  }

}
