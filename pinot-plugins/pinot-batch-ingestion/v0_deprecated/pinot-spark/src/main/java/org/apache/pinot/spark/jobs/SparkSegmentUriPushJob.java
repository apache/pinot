/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.spark.jobs;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import org.apache.hadoop.fs.Path;
import org.apache.pinot.ingestion.common.ControllerRestApi;
import org.apache.pinot.ingestion.common.JobConfigConstants;
import org.apache.pinot.ingestion.jobs.SegmentUriPushJob;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;


public class SparkSegmentUriPushJob extends SegmentUriPushJob {
  private final boolean _enableParallelPush;
  private int _pushJobParallelism;

  public SparkSegmentUriPushJob(Properties properties) {
    super(properties);
    _enableParallelPush =
        Boolean.parseBoolean(properties.getProperty(JobConfigConstants.ENABLE_PARALLEL_PUSH, JobConfigConstants.DEFAULT_ENABLE_PARALLEL_PUSH));
    _pushJobParallelism = Integer.parseInt(properties.getProperty(JobConfigConstants.PUSH_JOB_PARALLELISM, JobConfigConstants.DEFAULT_PUSH_JOB_PARALLELISM));
  }

  @Override
  public void run()
      throws Exception {
    if (!_enableParallelPush) {
      super.run();
    } else {
      List<Path> tarFilePaths = getDataFilePaths(_segmentPattern);
      retainRecentFiles(tarFilePaths, _lookBackPeriod);
      List<String> segmentUris = new ArrayList<>(tarFilePaths.size());
      for (Path tarFilePath : tarFilePaths) {
        segmentUris.add(_segmentUriPrefix + tarFilePath.toUri().getRawPath() + _segmentUriSuffix);
      }
      JavaSparkContext sparkContext = JavaSparkContext.fromSparkContext(SparkContext.getOrCreate());
      if (_pushJobParallelism == -1) {
        _pushJobParallelism = segmentUris.size();
      }
      JavaRDD<String> pathRDD = sparkContext.parallelize(segmentUris, _pushJobParallelism);
      pathRDD.foreach(segmentUri -> {
        try (ControllerRestApi controllerRestApi = getControllerRestApi()) {
          controllerRestApi.sendSegmentUris(Arrays.asList(segmentUri));
        }
      });
    }
  }
}
