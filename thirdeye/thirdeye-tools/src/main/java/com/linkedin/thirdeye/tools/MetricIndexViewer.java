package com.linkedin.thirdeye.tools;

import com.linkedin.thirdeye.impl.storage.MetricIndexEntry;
import com.linkedin.thirdeye.impl.storage.StorageUtils;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class MetricIndexViewer {
  public static void main(String[] args) throws IOException {
    if (args.length == 0) {
      throw new IllegalArgumentException("usage: metric_index_file ...");
    }

    for (String arg : args) {
      List<MetricIndexEntry> indexEntries = StorageUtils.readMetricIndex(new File(arg));

      for (MetricIndexEntry indexEntry : indexEntries) {
        System.out.println(indexEntry);
      }
    }
  }
}
