package com.linkedin.thirdeye.tools;

import com.linkedin.thirdeye.impl.storage.DimensionIndexEntry;
import com.linkedin.thirdeye.impl.storage.StorageUtils;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class DimensionIndexViewer {
  public static void main(String[] args) throws IOException {
    if (args.length == 0) {
      throw new IllegalArgumentException("usage: dimension_index_file ...");
    }

    for (String arg : args) {
      List<DimensionIndexEntry> indexEntries = StorageUtils.readDimensionIndex(new File(arg));

      for (DimensionIndexEntry indexEntry : indexEntries) {
        System.out.println(indexEntry);
      }
    }
  }
}
