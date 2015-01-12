package com.linkedin.thirdeye.impl;

import com.linkedin.thirdeye.api.DimensionSpec;
import com.linkedin.thirdeye.api.MetricSpec;
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
  private final List<DimensionSpec> dimensionSpecs;
  private final List<MetricSpec> metricSpecs;
  private final String timeColumnName;

  public StarTreeRecordStreamAvroFileImpl(File avroFile,
                                          List<DimensionSpec> dimensionSpecs,
                                          List<MetricSpec> metricSpecs,
                                          String timeColumnName) throws IOException
  {
    this(DataFileReader.openReader(avroFile, new GenericDatumReader<GenericRecord>()),
         dimensionSpecs,
         metricSpecs,
         timeColumnName);
  }

  public StarTreeRecordStreamAvroFileImpl(FileReader<GenericRecord> fileReader,
                                          List<DimensionSpec> dimensionSpecs,
                                          List<MetricSpec> metricSpecs,
                                          String timeColumnName)
  {
    this.fileReader = fileReader;
    this.dimensionSpecs = dimensionSpecs;
    this.metricSpecs = metricSpecs;
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
        for (DimensionSpec dimensionSpec : dimensionSpecs)
        {
          Object o = genericRecord.get(dimensionSpec.getName());
          if (o == null)
          {
            throw new IllegalArgumentException("Found null dimension value in " + genericRecord);
          }
          builder.setDimensionValue(dimensionSpec.getName(), o.toString());
        }

        // Extract metrics
        for (int i=0;i < metricSpecs.size();i++)
        {
          String metricName = metricSpecs.get(i).getName();
          builder.setMetricType(metricName, metricSpecs.get(i).getType());
          Object o = genericRecord.get(metricName);
          if (o == null)
          {
            builder.setMetricValue(metricName, 0);
          }
          else if (o instanceof Integer)
          {
            builder.setMetricValue(metricName, (Integer) o);
          }
          else if (o instanceof Long)
          {
            builder.setMetricValue(metricName, ((Long) o).intValue());
          }
          else if (o instanceof Double)
          {
            builder.setMetricValue(metricName, ((Double) o).intValue());
          }
          else if (o instanceof Float)
          {
            builder.setMetricValue(metricName, ((Float) o).intValue());
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
          builder.setTime(((Integer) o).longValue());
        }
        else if (o instanceof Long)
        {
          builder.setTime((Long) o);
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
