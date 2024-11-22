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
package org.apache.pinot.plugin.ingestion.batch.spark;

import java.util.Arrays;
import java.util.List;
import org.apache.pinot.plugin.ingestion.batch.spark.common.AbstractSparkSegmentUriPushJobRunner;
import org.apache.pinot.segment.local.utils.SegmentPushUtils;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.filesystem.PinotFSFactory;
import org.apache.pinot.spi.ingestion.batch.spec.PinotFSSpec;
import org.apache.pinot.spi.ingestion.batch.spec.SegmentGenerationJobSpec;
import org.apache.pinot.spi.plugin.PluginManager;
import org.apache.pinot.spi.utils.retry.AttemptsExceededException;
import org.apache.pinot.spi.utils.retry.RetriableOperationException;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;


public class SparkSegmentUriPushJobRunner extends AbstractSparkSegmentUriPushJobRunner {

  public SparkSegmentUriPushJobRunner() {
    super();
  }

  public SparkSegmentUriPushJobRunner(SegmentGenerationJobSpec spec) {
    super(spec);
  }

  @Override
  public void parallelizeUriPushJob(List<PinotFSSpec> pinotFSSpecs, List<String> segmentUris, int pushParallelism) {
    JavaSparkContext sparkContext = JavaSparkContext.fromSparkContext(SparkContext.getOrCreate());
    JavaRDD<String> pathRDD = sparkContext.parallelize(segmentUris, pushParallelism);
    // Prevent using lambda expression in Spark to avoid potential serialization exceptions, use inner function
    // instead.
    pathRDD.foreach(new VoidFunction<String>() {
      @Override
      public void call(String segmentUri)
          throws Exception {
        try {
          PluginManager.get().init();
          for (PinotFSSpec pinotFSSpec : pinotFSSpecs) {
            PinotFSFactory
                .register(pinotFSSpec.getScheme(), pinotFSSpec.getClassName(), new PinotConfiguration(pinotFSSpec));
          }
          SegmentPushUtils.sendSegmentUris(_spec, Arrays.asList(segmentUri));
        } catch (RetriableOperationException | AttemptsExceededException e) {
          throw new RuntimeException(e);
        }
      }
    });
  }
}
