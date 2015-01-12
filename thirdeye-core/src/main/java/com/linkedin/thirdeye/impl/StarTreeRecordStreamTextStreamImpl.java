package com.linkedin.thirdeye.impl;

import com.linkedin.thirdeye.api.MetricType;
import com.linkedin.thirdeye.api.StarTreeRecord;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class StarTreeRecordStreamTextStreamImpl implements Iterable<StarTreeRecord>
{
  private final InputStream inputStream;
  private final List<String> dimensionNames;
  private final List<String> metricNames;
  private final List<MetricType> metricTypes;
  private final String columnSeparator;
  private final boolean hasTime;

  public StarTreeRecordStreamTextStreamImpl(InputStream inputStream,
                                            List<String> dimensionNames,
                                            List<String> metricNames,
                                            List<MetricType> metricTypes,
                                            String columnSeparator,
                                            boolean hasTime)
  {
    this.inputStream = inputStream;
    this.dimensionNames = dimensionNames;
    this.metricNames = metricNames;
    this.metricTypes = metricTypes;
    this.columnSeparator = columnSeparator;
    this.hasTime = hasTime;
  }

  @Override
  public Iterator<StarTreeRecord> iterator()
  {
    try
    {
      final BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));

      return new Iterator<StarTreeRecord>()
      {
        String nextLine = null;

        @Override
        public boolean hasNext()
        {
          try
          {
            if (nextLine == null)
            {
              nextLine = reader.readLine();
            }
            return nextLine != null;
          }
          catch (Exception e)
          {
            throw new IllegalStateException(e);
          }
        }

        @Override
        public StarTreeRecord next()
        {
          if (!hasNext()) // advances line
          {
            throw new NoSuchElementException();
          }

          int idx = 0;
          String[] tokens = nextLine.split(columnSeparator);

          StarTreeRecordImpl.Builder builder = new StarTreeRecordImpl.Builder();

          for (String dimensionName : dimensionNames)
          {
            builder.setDimensionValue(dimensionName, tokens[idx++]);
          }

          for (int i=0;i< metricNames.size();i++)
          {
            String metricName = metricNames.get(i);
            builder.setMetricValue(metricName, Integer.valueOf(tokens[idx++]));
            builder.setMetricType(metricName, metricTypes.get(i));
          }

          builder.setTime(hasTime ? Long.valueOf(tokens[idx]) : 0L);

          nextLine = null; // to advance

          return builder.build();
        }

        @Override
        public void remove()
        {
          throw new UnsupportedOperationException("This stream is read only");
        }
      };
    }
    catch (Exception e)
    {
      throw new IllegalStateException(e);
    }
  }
}
