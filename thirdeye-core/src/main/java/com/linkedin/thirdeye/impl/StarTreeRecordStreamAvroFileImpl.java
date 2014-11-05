package com.linkedin.thirdeye.impl;

import com.linkedin.thirdeye.api.StarTreeRecord;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.FileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public class StarTreeRecordStreamAvroFileImpl implements Iterable<StarTreeRecord>
{
  private final FileReader<GenericRecord> fileReader;
  private final List<String> dimensionNames;
  private final List<String> metricNames;
  private final String timeColumnName;

  public StarTreeRecordStreamAvroFileImpl(File avroFile,
                                          List<String> dimensionNames,
                                          List<String> metricNames,
                                          String timeColumnName) throws IOException
  {
    this(DataFileReader.openReader(avroFile, new GenericDatumReader<GenericRecord>()),
         dimensionNames,
         metricNames,
         timeColumnName);
  }

  public StarTreeRecordStreamAvroFileImpl(FileReader<GenericRecord> fileReader,
                                          List<String> dimensionNames,
                                          List<String> metricNames,
                                          String timeColumnName)
  {
    this.fileReader = fileReader;
    this.dimensionNames = dimensionNames;
    this.metricNames = metricNames;
    this.timeColumnName = timeColumnName;
  }

  @Override
  public Iterator<StarTreeRecord> iterator()
  {
    final Iterator<GenericRecord> itr = fileReader.iterator();

    return new Iterator<StarTreeRecord>()
    {
      @Override
      public boolean hasNext()
      {
        return itr.hasNext();
      }

      @Override
      public StarTreeRecord next()
      {
        StarTreeRecordImpl.Builder builder = new StarTreeRecordImpl.Builder();

        GenericRecord genericRecord = itr.next();

        // Extract dimensions
        for (String dimensionName : dimensionNames)
        {
          Object o = genericRecord.get(dimensionName);
          if (o == null)
          {
            throw new IllegalArgumentException("Found null dimension value in " + genericRecord);
          }
          builder.setDimensionValue(dimensionName, o.toString());
        }

        // Extract metrics
        for (String metricName : metricNames)
        {
          Object o = genericRecord.get(metricName);
          if (o == null)
          {
            builder.setMetricValue(metricName, 0L);
          }
          else if (o instanceof Integer)
          {
            builder.setMetricValue(metricName, ((Integer) o).longValue());
          }
          else if (o instanceof Long)
          {
            builder.setMetricValue(metricName, (Long) o);
          }
          else
          {
            throw new IllegalArgumentException("Invalid metric field: " + o);
          }
        }

        // Extract time
        Object o = genericRecord.get(timeColumnName);
        if (o == null)
        {
          throw new IllegalArgumentException("Found null metric value in " + genericRecord);
        }
        else if (o instanceof Integer)
        {
          builder.setMetricValue(timeColumnName, ((Integer) o).longValue());
        }
        else if (o instanceof Long)
        {
          builder.setMetricValue(timeColumnName, (Long) o);
        }
        else
        {
          throw new IllegalArgumentException("Invalid time field: " + o);
        }

        return builder.build();
      }

      @Override
      public void remove()
      {
        itr.remove();
      }
    };
  }
}
