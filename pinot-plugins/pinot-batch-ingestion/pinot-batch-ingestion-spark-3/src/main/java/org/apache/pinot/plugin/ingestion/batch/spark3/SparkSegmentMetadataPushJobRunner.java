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
package org.apache.pinot.plugin.ingestion.batch.spark3;

import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.pinot.common.segment.generation.SegmentGenerationUtils;
import org.apache.pinot.segment.local.utils.ConsistentDataPushUtils;
import org.apache.pinot.segment.local.utils.SegmentPushUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.apache.pinot.spi.filesystem.PinotFSFactory;
import org.apache.pinot.spi.ingestion.batch.runner.IngestionJobRunner;
import org.apache.pinot.spi.ingestion.batch.spec.Constants;
import org.apache.pinot.spi.ingestion.batch.spec.PinotClusterSpec;
import org.apache.pinot.spi.ingestion.batch.spec.PinotFSSpec;
import org.apache.pinot.spi.ingestion.batch.spec.SegmentGenerationJobSpec;
import org.apache.pinot.spi.plugin.PluginManager;
import org.apache.pinot.spi.utils.retry.AttemptsExceededException;
import org.apache.pinot.spi.utils.retry.RetriableOperationException;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.scheduler.JobFailed;
import org.apache.spark.scheduler.JobResult;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerJobEnd;

public class SparkSegmentMetadataPushJobRunner implements IngestionJobRunner, Serializable {
  // This listener is added to the SparkContext and is executed when the Spark job fails.
  // It handles the failure by calling ConsistentDataPushUtils.handleUploadException.
  // The listener is only added if consistent data push is enabled and the pushParallelism is greater than 1.
  // This listener is not a required part of the implementation, as the start replace segments protocol
  // will cleanup past failures as part of the fresh consistent data push, but it's still cleaner to handle
  // the failure as soon as possible.
  private static class ConsistentDataPushFailureHandler extends SparkListener {
    private final SegmentGenerationJobSpec _spec;
    private final Map<URI, String> _uriToLineageEntryIdMap;

    public ConsistentDataPushFailureHandler(SegmentGenerationJobSpec spec, Map<URI, String> uriToLineageEntryIdMap) {
      _spec = spec;
      _uriToLineageEntryIdMap = uriToLineageEntryIdMap;
    }

    @Override
    public void onJobEnd(SparkListenerJobEnd jobEnd) {
      JobResult jobResult = jobEnd.jobResult();
      if (jobResult instanceof JobFailed) {
        try {
          ConsistentDataPushUtils.handleUploadException(_spec, _uriToLineageEntryIdMap,
              ((JobFailed) jobResult).exception());
        } catch (Exception e) {
          throw new RuntimeException("Failed to handle upload exception", e);
        }
      }
    }
  }

  private SegmentGenerationJobSpec _spec;

  public SparkSegmentMetadataPushJobRunner() {
  }

  @Override
  public void init(SegmentGenerationJobSpec spec) {
    _spec = spec;
  }

  @Override
  public void run()
      throws Exception {
    // Initialize all file systems
    setupFileSystems();

    // Get outputFS for writing output pinot segments
    URI outputDirURI = validateOutputDirURI(_spec.getOutputDirURI());
    PinotFS outputDirFS = PinotFSFactory.create(outputDirURI.getScheme());

    // Collect files to process
    List<String> segmentsToPush = getSegmentsToPush(outputDirFS, outputDirURI);

    // Ensure tableConfigURI is set if missing
    setupTableConfigURI();

    // Retrieve table config and check for consistent push
    TableConfig tableConfig =
        SegmentGenerationUtils.getTableConfig(_spec.getTableSpec().getTableConfigURI(), _spec.getAuthToken());
    boolean consistentPushEnabled = ConsistentDataPushUtils.consistentDataPushEnabled(tableConfig);

    // Determine push parallelism
    int pushParallelism = _spec.getPushJobSpec().getPushParallelism();
    if (pushParallelism < 1) {
      pushParallelism = segmentsToPush.size();
    }

    // Call the appropriate push helper
    if (consistentPushEnabled) {
      handleConsistentPush(segmentsToPush, outputDirURI, pushParallelism);
    } else {
      handleNonConsistentPush(segmentsToPush, outputDirFS, outputDirURI, pushParallelism);
    }
  }

  private void handleConsistentPush(List<String> segmentsToPush, URI outputDirURI, int pushParallelism)
      throws Exception {
    Map<String, String> segmentsToPushUriToTarPathMap =
        SegmentPushUtils.getSegmentUriToTarPathMap(outputDirURI, _spec.getPushJobSpec(),
            segmentsToPush.toArray(new String[0]));
    Map<URI, String> uriToLineageEntryIdMap =
        ConsistentDataPushUtils.preUpload(_spec, getSegmentsToReplace(segmentsToPushUriToTarPathMap));

    if (pushParallelism == 1) {
      // Single push
      SegmentPushUtils.sendSegmentUriAndMetadata(_spec, PinotFSFactory.create(outputDirURI.getScheme()),
          segmentsToPushUriToTarPathMap);
    } else {
      // Parallel push using Spark
      JavaSparkContext sparkContext = JavaSparkContext.fromSparkContext(SparkContext.getOrCreate());
      sparkContext.sc().addSparkListener(new ConsistentDataPushFailureHandler(_spec, uriToLineageEntryIdMap));
      JavaRDD<String> pathRDD = sparkContext.parallelize(segmentsToPush, pushParallelism);

      if (_spec.getPushJobSpec().isBatchSegmentUpload()) {
        // Process segments in batch mode using foreachPartition
        pathRDD.foreachPartition(segmentIterator -> {
          setupFileSystems();
          List<String> segmentsInPartition = new ArrayList<>();
          segmentIterator.forEachRemaining(segmentsInPartition::add);

          Map<String, String> segmentUriToTarPathMap =
              SegmentPushUtils.getSegmentUriToTarPathMap(outputDirURI, _spec.getPushJobSpec(),
                  segmentsInPartition.toArray(new String[0]));
          SegmentPushUtils.sendSegmentUriAndMetadata(_spec, PinotFSFactory.create(outputDirURI.getScheme()),
              segmentUriToTarPathMap);
        });
      } else {
        // Process segments one by one using foreach
        pathRDD.foreach(segmentTarPath -> {
          setupFileSystems();
          Map<String, String> segmentUriToTarPathMap =
              SegmentPushUtils.getSegmentUriToTarPathMap(outputDirURI, _spec.getPushJobSpec(),
                  new String[]{segmentTarPath});
          SegmentPushUtils.sendSegmentUriAndMetadata(_spec, PinotFSFactory.create(outputDirURI.getScheme()),
              segmentUriToTarPathMap);
        });
      }
    }

    executePostUpload(uriToLineageEntryIdMap);
  }


  private void handleNonConsistentPush(List<String> segmentsToPush, PinotFS outputDirFS, URI outputDirURI,
      int pushParallelism)
      throws Exception {
    if (pushParallelism == 1) {
      // Push from driver
      try {
        SegmentPushUtils.pushSegments(_spec, outputDirFS, segmentsToPush);
      } catch (RetriableOperationException | AttemptsExceededException e) {
        throw new RuntimeException(e);
      }
    } else {
      JavaSparkContext sparkContext = JavaSparkContext.fromSparkContext(SparkContext.getOrCreate());
      JavaRDD<String> pathRDD = sparkContext.parallelize(segmentsToPush, pushParallelism);

      if (_spec.getPushJobSpec().isBatchSegmentUpload()) {
        // Process segments in batch mode using foreachPartition
        pathRDD.foreachPartition(new VoidFunction<Iterator<String>>() {
          @Override
          public void call(Iterator<String> segmentIterator) throws Exception {
            PluginManager.get().init();
            setupFileSystems();

            List<String> segmentsInPartition = new ArrayList<>();
            segmentIterator.forEachRemaining(segmentsInPartition::add);

            try {
              Map<String, String> segmentUriToTarPathMap =
                  SegmentPushUtils.getSegmentUriToTarPathMap(outputDirURI, _spec.getPushJobSpec(),
                      segmentsInPartition.toArray(new String[0]));
              SegmentPushUtils.sendSegmentUriAndMetadata(_spec, PinotFSFactory.create(outputDirURI.getScheme()),
                  segmentUriToTarPathMap);
            } catch (RetriableOperationException | AttemptsExceededException e) {
              throw new RuntimeException(e);
            }
          }
        });
      } else {
        // Process segments one by one using foreach
        pathRDD.foreach(new VoidFunction<String>() {
          @Override
          public void call(String segmentTarPath) throws Exception {
            PluginManager.get().init();
            setupFileSystems();
            try {
              Map<String, String> segmentUriToTarPathMap =
                  SegmentPushUtils.getSegmentUriToTarPathMap(outputDirURI, _spec.getPushJobSpec(),
                      new String[]{segmentTarPath});
              SegmentPushUtils.sendSegmentUriAndMetadata(_spec, PinotFSFactory.create(outputDirURI.getScheme()),
                  segmentUriToTarPathMap);
            } catch (RetriableOperationException | AttemptsExceededException e) {
              throw new RuntimeException(e);
            }
          }
        });
      }
    }
  }


  private List<String> getSegmentsToPush(PinotFS outputDirFS, URI outputDirURI) {
    String[] files = listFiles(outputDirFS, outputDirURI);
    return Arrays.stream(files).filter(file -> file.endsWith(Constants.TAR_GZ_FILE_EXT)).collect(Collectors.toList());
  }

  private void setupFileSystems() {
    List<PinotFSSpec> pinotFSSpecs = _spec.getPinotFSSpecs();
    for (PinotFSSpec pinotFSSpec : pinotFSSpecs) {
      PinotFSFactory.register(pinotFSSpec.getScheme(), pinotFSSpec.getClassName(), new PinotConfiguration(pinotFSSpec));
    }
  }

  private URI validateOutputDirURI(String outputDir) {
    URI outputDirURI;
    try {
      outputDirURI = new URI(outputDir);
      if (outputDirURI.getScheme() == null) {
        outputDirURI = new File(outputDir).toURI();
      }
    } catch (URISyntaxException e) {
      throw new RuntimeException("outputDirURI is not valid - '" + outputDir + "'");
    }
    return outputDirURI;
  }

  private String[] listFiles(PinotFS outputDirFS, URI outputDirURI)
      throws RuntimeException {
    try {
      return outputDirFS.listFiles(outputDirURI, true);
    } catch (IOException e) {
      throw new RuntimeException("Unable to list all files under outputDirURI - '" + outputDirURI + "'");
    }
  }

  private void setupTableConfigURI() {
    if (_spec.getTableSpec().getTableConfigURI() == null) {
      if (_spec.getPinotClusterSpecs() == null || _spec.getPinotClusterSpecs().length == 0) {
        throw new RuntimeException("Missing property 'tableConfigURI' in 'tableSpec'");
      }
      PinotClusterSpec pinotClusterSpec = _spec.getPinotClusterSpecs()[0];
      String tableConfigURI = SegmentGenerationUtils.generateTableConfigURI(pinotClusterSpec.getControllerURI(),
          _spec.getTableSpec().getTableName());
      _spec.getTableSpec().setTableConfigURI(tableConfigURI);
    }
  }

  private void executePostUpload(Map<URI, String> uriToLineageEntryIdMap) {
    try {
      ConsistentDataPushUtils.postUpload(_spec, uriToLineageEntryIdMap);
    } catch (Exception e) {
      ConsistentDataPushUtils.handleUploadException(_spec, uriToLineageEntryIdMap, e);
      throw new RuntimeException(e);
    }
  }

  /**
   * Returns segment names, which will be supplied to the segment replacement protocol as the new set of segments to
   * atomically update when consistent data push is enabled.
   * @param segmentsUriToTarPathMap Map from segment URI to corresponding tar path. Either the URIs (keys), the
   *                                tarPaths (values), or both may be used depending on upload mode.
   */
  public List<String> getSegmentsToReplace(Map<String, String> segmentsUriToTarPathMap) {
    Collection<String> tarFilePaths = segmentsUriToTarPathMap.values();
    List<String> segmentNames = new ArrayList<>(tarFilePaths.size());
    for (String tarFilePath : tarFilePaths) {
      File tarFile = new File(tarFilePath);
      String fileName = tarFile.getName();
      Preconditions.checkArgument(fileName.endsWith(Constants.TAR_GZ_FILE_EXT));
      String segmentName = fileName.substring(0, fileName.length() - Constants.TAR_GZ_FILE_EXT.length());
      segmentNames.add(segmentName);
    }
    return segmentNames;
  }
}
