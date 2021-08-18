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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.pinot.ingestion.common.ControllerRestApi;
import org.apache.pinot.ingestion.common.DefaultControllerRestApi;
import org.apache.pinot.ingestion.common.JobConfigConstants;
import org.apache.pinot.ingestion.jobs.SegmentTarPushJob;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;


public class SparkSegmentTarPushJob extends SegmentTarPushJob {
  private final boolean _enableParallelPush;
  private int _pushJobParallelism;
  private int _pushJobRetry;

  public SparkSegmentTarPushJob(Properties properties) {
    super(properties);
    _enableParallelPush =
        Boolean.parseBoolean(properties.getProperty(JobConfigConstants.ENABLE_PARALLEL_PUSH, JobConfigConstants.DEFAULT_ENABLE_PARALLEL_PUSH));
    _pushJobParallelism = Integer.parseInt(properties.getProperty(JobConfigConstants.PUSH_JOB_PARALLELISM, JobConfigConstants.DEFAULT_PUSH_JOB_PARALLELISM));
    _pushJobRetry = Integer.parseInt(properties.getProperty(JobConfigConstants.PUSH_JOB_RETRY, JobConfigConstants.DEFAULT_PUSH_JOB_RETRY));
  }

  @Override
  public void run()
      throws Exception {
    if (!_enableParallelPush) {
      super.run();
    } else {
      List<Path> segmentPathsToPush = getDataFilePaths(_segmentPattern);
      retainRecentFiles(segmentPathsToPush, _lookBackPeriod);
      List<String> segmentsToPush = new ArrayList<>();
      segmentPathsToPush.forEach(path -> {
        segmentsToPush.add(path.toString());
      });
      JavaSparkContext sparkContext = JavaSparkContext.fromSparkContext(SparkContext.getOrCreate());
      if (_pushJobParallelism == -1) {
        _pushJobParallelism = segmentsToPush.size();
      }
      JavaRDD<String> pathRDD = sparkContext.parallelize(segmentsToPush, _pushJobParallelism);
      pathRDD.foreach(segmentTarPath -> {
        try (ControllerRestApi controllerRestApi = getControllerRestApi()) {
          FileSystem fileSystem = FileSystem.get(new Path(segmentTarPath).toUri(), new Configuration());
          // TODO: Deal with invalid prefixes in the future
          List<String> currentSegments = controllerRestApi.getAllSegments("OFFLINE");
          controllerRestApi.pushSegments(fileSystem, Arrays.asList(new Path(segmentTarPath)));
          if (_deleteExtraSegments) {
            controllerRestApi.deleteSegmentUris(getSegmentsToDelete(currentSegments, Arrays.asList(new Path(segmentTarPath))));
          }
        }
      });
    }
  }

  protected ControllerRestApi getControllerRestApi() {
    return new DefaultControllerRestApi(_pushLocations, _rawTableName, _pushJobRetry);
  }
}
