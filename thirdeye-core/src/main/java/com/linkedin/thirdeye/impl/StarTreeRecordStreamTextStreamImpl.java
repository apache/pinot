package com.linkedin.thirdeye.impl;

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
  private final String columnSeparator;

  public StarTreeRecordStreamTextStreamImpl(InputStream inputStream,
                                            List<String> dimensionNames,
                                            List<String> metricNames,
                                            String columnSeparator)
  {
    this.inputStream = inputStream;
    this.dimensionNames = dimensionNames;
    this.metricNames = metricNames;
    this.columnSeparator = columnSeparator;
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

          for (String metricName : metricNames)
          {
            builder.setMetricValue(metricName, Integer.valueOf(tokens[idx++]));
          }

          builder.setTime(Long.valueOf(tokens[idx]));

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
