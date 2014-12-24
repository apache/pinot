package com.linkedin.thirdeye.bootstrap.util;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.bootstrap.DimensionKey;
import com.linkedin.thirdeye.bootstrap.MetricTimeSeries;
import com.linkedin.thirdeye.impl.StarTreeUtils;

public class CircularBufferUtil {
  private static final Logger LOG = LoggerFactory
      .getLogger(CircularBufferUtil.class);

  public static void createLeafBufferFile(
      Map<DimensionKey, MetricTimeSeries> map, List<int[]> leafRecords,
      String fileName, List<String> dimensionNames, List<String> metricNames,
      int numTimeBuckets, Map<String, Map<Integer, String>> reverseForwardIndex)
      throws IOException {
    int numRecords = leafRecords.size();
    int perRecordDimesionKeySize = dimensionNames.size() * (Integer.SIZE / 8);
    int perTimeWindowMetricSize = Long.SIZE / 8
        + (metricNames.size() * (Integer.SIZE / 8));
    int perRecordMetricSize = numTimeBuckets * perTimeWindowMetricSize;
    int perRecordEntrySize = perRecordDimesionKeySize + perRecordMetricSize;
    int totalBufferSize = numRecords * perRecordEntrySize;

    LOG.info("Generating buffer index for size: {}", leafRecords.size());
    // sort the data
    Comparator<? super int[]> c = new Comparator<int[]>() {
      @Override
      public int compare(int[] o1, int[] o2) {
        int ret = 0;
        int length = o1.length;
        for (int i = 0; i < length; i++) {
          ret = Integer.compare(o1[i], o2[i]);
          if (ret != 0) {
            break;
          }
        }
        return ret;
      }
    };
    RandomAccessFile raf = new RandomAccessFile(fileName, "rw");
    FileChannel fc = raf.getChannel();
    MappedByteBuffer buffer = fc.map(FileChannel.MapMode.READ_WRITE, 0,
        totalBufferSize);
    Collections.sort(leafRecords, c);
    int recordIndex = 0;
    for (int[] leafRecord : leafRecords) {

      int recordStartOffset = recordIndex * perRecordEntrySize;
      buffer.position(recordStartOffset);
      for (int id : leafRecord) {
        buffer.putInt(id);
      }
      // set everything to 0;
      for (int i = 0; i < numTimeBuckets; i++) {
        // set the timeValue to 0
        buffer.putLong(i);
        for (int j = 0; j < metricNames.size(); j++) {
          buffer.putInt(0);
        }
      }

      // now write the metrics;
      String[] dimValues = StarTreeUtils.convertToStringValue(
          reverseForwardIndex, leafRecord, dimensionNames);
      // get the aggregated timeseries for this dimensionKey
      DimensionKey dimensionKey = new DimensionKey(dimValues);
      MetricTimeSeries metricTimeSeries = map.get(dimensionKey);
      if (metricTimeSeries != null) {

        // add a star tree record for each timewindow
        for (long timeWindow : metricTimeSeries.getTimeWindowSet()) {
          int bucket = (int) (timeWindow % numTimeBuckets);
          buffer.position(recordStartOffset + perRecordDimesionKeySize + bucket
              * perTimeWindowMetricSize);
          buffer.putLong(timeWindow);
          for (String metricName : metricNames) {
            Number number = metricTimeSeries.get(timeWindow, metricName);
            buffer.putInt(number.intValue());
          }
        }
      }
      recordIndex = recordIndex + 1;
    }
    fc.close();
    raf.close();
  }
}
