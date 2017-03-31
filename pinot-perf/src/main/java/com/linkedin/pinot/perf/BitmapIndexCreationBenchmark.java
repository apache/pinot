/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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

import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.core.indexsegment.columnar.ColumnarSegmentLoader;
import com.linkedin.pinot.core.segment.index.loader.IndexLoadingConfig;
import java.io.File;
import java.util.Arrays;
import java.util.HashSet;


public class BitmapIndexCreationBenchmark {

  public static void main(String[] args)
      throws Exception {
    System.out.println("Starting generation");
    IndexLoadingConfig indexLoadingConfig = new IndexLoadingConfig();
    indexLoadingConfig.setReadMode(ReadMode.heap);
    indexLoadingConfig.setInvertedIndexColumns(new HashSet<>(Arrays.asList("contract_id", "seat_id")));
    File indexDir = new File(
        "/home/kgopalak/pinot_perf/index_dir/capReportingEvents_OFFLINE/capReportingEvents_capReportingEvents_daily_2");
    long start = System.currentTimeMillis();
    ColumnarSegmentLoader.load(indexDir, indexLoadingConfig);
    long end = System.currentTimeMillis();
    System.out.println("Took " + (end - start) + " to generate bitmap index");
  }
}
