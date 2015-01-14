package com.linkedin.thirdeye.impl;

import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.DimensionSpec;
import com.linkedin.thirdeye.api.MetricSchema;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.StarTreeRecord;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.FileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

public class StarTreeRecordStreamAvroFileImpl implements Iterable<StarTreeRecord>
{
  private final StarTreeConfig config;
  private final FileReader<GenericRecord> fileReader;
  private final String timeColumnName;
  private final MetricSchema metricSchema;

  public StarTreeRecordStreamAvroFileImpl(StarTreeConfig config,
                                          File avroFile,
                                          String timeColumnName) throws IOException
  {
    this(config,
         DataFileReader.openReader(avroFile, new GenericDatumReader<GenericRecord>()),
         timeColumnName);
  }

  public StarTreeRecordStreamAvroFileImpl(StarTreeConfig config,
                                          FileReader<GenericRecord> fileReader,
                                          String timeColumnName)
  {
    this.config = config;
    this.fileReader = fileReader;
    this.timeColumnName = timeColumnName;
    this.metricSchema = MetricSchema.fromMetricSpecs(config.getMetrics());
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
        String[] dimensionValues = new String[config.getDimensions().size()];
        for (int i = 0; i < config.getDimensions().size(); i++)
        {
          DimensionSpec dimensionSpec = config.getDimensions().get(i);
          Object o = genericRecord.get(dimensionSpec.getName());
          if (o == null)
          {
            throw new IllegalArgumentException("Found null dimension value in " + genericRecord);
          }
          dimensionValues[i] = o.toString();
        }

        // Extract time
        Object o = genericRecord.get(timeColumnName);
        if (o == null)
        {
          throw new IllegalArgumentException("Found null metric value in " + genericRecord);
        }
        Long time = ((Number) o).longValue();

        // Extract metrics
        MetricTimeSeries timeSeries = new MetricTimeSeries(metricSchema);
        for (int i=0;i < config.getMetrics().size();i++)
        {
          String metricName = config.getMetrics().get(i).getName();
          o = genericRecord.get(metricName);
          if (o == null)
          {
            o = 0;
          }
          timeSeries.increment(time, metricName, (Number) o);
        }

        return builder.setDimensionKey(new DimensionKey(dimensionValues))
                      .setMetricTimeSeries(timeSeries)
                      .build(config);
      }

      @Override
      public void remove()
      {
        itr.remove();
      }
    };
  }
}
