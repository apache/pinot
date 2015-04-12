/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.perf;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import com.javamex.classmexer.MemoryUtil;
import com.linkedin.pinot.core.segment.index.BitmapInvertedIndex;
import com.linkedin.pinot.core.segment.index.ColumnMetadata;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;


public class BitmapPerformanceBenchmark {
  public static void main(String[] args) throws Exception {
    String indexDir = "";

    File[] listFiles = new File(indexDir).listFiles();
    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(new File(indexDir));
    List<BitmapInvertedIndex> arrayList = new ArrayList<BitmapInvertedIndex>();

    for (File file : listFiles) {
      if (!file.getName().endsWith("bitmap.inv")) {
        continue;
      }
      String column = file.getName().replaceAll(".bitmap.inv", "");
      ColumnMetadata columnMetadata = segmentMetadata.getColumnMetadataFor(column);
      int cardinality = columnMetadata.getCardinality();
      System.out.println(column + "\t\t\t" + cardinality);
      BitmapInvertedIndex bitmapInvertedIndex = new BitmapInvertedIndex(file, cardinality, true);
      System.out.println(column + ":\t" + MemoryUtil.deepMemoryUsageOf(bitmapInvertedIndex));
      arrayList.add(bitmapInvertedIndex);

    }
  }
}
