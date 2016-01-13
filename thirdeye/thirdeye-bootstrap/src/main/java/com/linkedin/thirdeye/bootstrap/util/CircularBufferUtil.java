package com.linkedin.thirdeye.bootstrap.util;

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

import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.MetricTimeSeries;
import com.linkedin.thirdeye.api.MetricType;
import com.linkedin.thirdeye.impl.StarTreeUtils;

public class CircularBufferUtil {
  private static final Logger LOGGER = LoggerFactory.getLogger(CircularBufferUtil.class);

  public static void createLeafBufferFile(Map<DimensionKey, MetricTimeSeries> map,
      List<int[]> leafRecords, String fileName, List<String> dimensionNames,
      List<String> metricNames, List<MetricType> metricTypes, int numTimeBuckets,
      Map<String, Map<Integer, String>> reverseForwardIndex) throws IOException {
    int numRecords = leafRecords.size();
    int perRecordDimesionKeySize = dimensionNames.size() * (Integer.SIZE / 8);
    int metricSize = 0;
    for (MetricType type : metricTypes) {
      metricSize += type.byteSize();
    }
    int perTimeWindowMetricSize = Long.SIZE / 8 + metricSize;
    int perRecordMetricSize = numTimeBuckets * perTimeWindowMetricSize;
    int perRecordEntrySize = perRecordDimesionKeySize + perRecordMetricSize;
    int totalBufferSize = numRecords * perRecordEntrySize;

    LOGGER.info("Generating buffer index for size: {}", leafRecords.size());
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
    MappedByteBuffer buffer = fc.map(FileChannel.MapMode.READ_WRITE, 0, totalBufferSize);
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
          switch (metricTypes.get(j)) {
          case SHORT:
            buffer.putShort((short) 0);
            break;
          case INT:
            buffer.putInt((int) 0);
            break;
          case LONG:
            buffer.putLong((long) 0);
            break;
          case FLOAT:
            buffer.putFloat((float) 0);
            break;
          case DOUBLE:
            buffer.putDouble((double) 0);
            break;
          }
        }
      }

      // now write the metrics;
      String[] dimValues =
          StarTreeUtils.convertToStringValue(reverseForwardIndex, leafRecord, dimensionNames);
      // get the aggregated timeseries for this dimensionKey
      DimensionKey dimensionKey = new DimensionKey(dimValues);
      MetricTimeSeries metricTimeSeries = map.get(dimensionKey);
      if (metricTimeSeries != null) {

        // add a star tree record for each timewindow
        for (long timeWindow : metricTimeSeries.getTimeWindowSet()) {
          int bucket = (int) (timeWindow % numTimeBuckets);
          buffer.position(
              recordStartOffset + perRecordDimesionKeySize + bucket * perTimeWindowMetricSize);
          buffer.putLong(timeWindow);
          for (int j = 0; j < metricNames.size(); j++) {
            String metricName = metricNames.get(j);
            Number number = metricTimeSeries.get(timeWindow, metricName);
            switch (metricTypes.get(j)) {
            case SHORT:
              buffer.putShort(number.shortValue());
              break;
            case INT:
              buffer.putInt(number.intValue());
              break;
            case LONG:
              buffer.putLong(number.longValue());
              break;
            case FLOAT:
              buffer.putFloat(number.floatValue());
              break;
            case DOUBLE:
              buffer.putDouble(number.doubleValue());
              break;
            }
          }
        }
      }
      recordIndex = recordIndex + 1;
    }
    fc.close();
    raf.close();
  }
}
