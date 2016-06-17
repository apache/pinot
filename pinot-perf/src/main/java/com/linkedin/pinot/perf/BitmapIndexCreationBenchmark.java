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

import java.io.File;
import java.util.List;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;

import com.google.common.collect.Lists;
import com.linkedin.pinot.common.metadata.segment.IndexLoadingConfigMetadata;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.core.segment.index.loader.Loaders;

public class BitmapIndexCreationBenchmark {

  public static void main(String[] args) throws Exception {

    System.out.println("Starting generation");
    Configuration tableDataManagerConfig = new PropertiesConfiguration();
    List<String> indexColumns = Lists.newArrayList("contract_id", "seat_id");
    tableDataManagerConfig.setProperty(IndexLoadingConfigMetadata.KEY_OF_LOADING_INVERTED_INDEX,
        indexColumns);
    IndexLoadingConfigMetadata indexLoadingConfigMetadata =
        new IndexLoadingConfigMetadata(tableDataManagerConfig);
    ReadMode mode = ReadMode.heap;
    File indexDir = new File(
        "/home/kgopalak/pinot_perf/index_dir/capReportingEvents_OFFLINE/capReportingEvents_capReportingEvents_daily_2");
    long start = System.currentTimeMillis();
    Loaders.IndexSegment.load(indexDir, mode, indexLoadingConfigMetadata);
    long end = System.currentTimeMillis();
    System.out.println("Took " + (end - start) + " to generate bitmap index");
  }
}
