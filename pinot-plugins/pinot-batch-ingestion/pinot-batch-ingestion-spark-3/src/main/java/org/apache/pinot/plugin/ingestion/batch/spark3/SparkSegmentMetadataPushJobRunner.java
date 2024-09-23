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
import java.util.Collection;
import java.util.List;
import java.util.Map;
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

    public ConsistentDataPushFailureHandler(SegmentGenerationJobSpec spec,
                                            Map<URI, String> uriToLineageEntryIdMap) {
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

  public SparkSegmentMetadataPushJobRunner(SegmentGenerationJobSpec spec) {
    init(spec);
  }

  @Override
  public void init(SegmentGenerationJobSpec spec) {
    _spec = spec;
  }

  @Override
  public void run()
      throws Exception {
    //init all file systems
    List<PinotFSSpec> pinotFSSpecs = _spec.getPinotFSSpecs();
    for (PinotFSSpec pinotFSSpec : pinotFSSpecs) {
      PinotFSFactory.register(pinotFSSpec.getScheme(), pinotFSSpec.getClassName(), new PinotConfiguration(pinotFSSpec));
    }

    //Get outputFS for writing output pinot segments
    URI outputDirURI;
    try {
      outputDirURI = new URI(_spec.getOutputDirURI());
      if (outputDirURI.getScheme() == null) {
        outputDirURI = new File(_spec.getOutputDirURI()).toURI();
      }
    } catch (URISyntaxException e) {
      throw new RuntimeException("outputDirURI is not valid - '" + _spec.getOutputDirURI() + "'");
    }
    PinotFS outputDirFS = PinotFSFactory.create(outputDirURI.getScheme());
    //Get list of files to process
    String[] files;
    try {
      files = outputDirFS.listFiles(outputDirURI, true);
    } catch (IOException e) {
      throw new RuntimeException("Unable to list all files under outputDirURI - '" + outputDirURI + "'");
    }

    List<String> segmentsToPush = new ArrayList<>();
    for (String file : files) {
      if (file.endsWith(Constants.TAR_GZ_FILE_EXT)) {
        segmentsToPush.add(file);
      }
    }
    Map<String, String> segmentsToPushUriToTarPathMap =
            SegmentPushUtils.getSegmentUriToTarPathMap(
                    outputDirURI,
                    _spec.getPushJobSpec(),
                    segmentsToPush.toArray(new String[0])
            );

    if (_spec.getTableSpec().getTableConfigURI() == null) {
      if (_spec.getPinotClusterSpecs() == null || _spec.getPinotClusterSpecs().length == 0) {
        throw new RuntimeException("Missing property 'tableConfigURI' in 'tableSpec'");
      }
      PinotClusterSpec pinotClusterSpec = _spec.getPinotClusterSpecs()[0];
      String tableConfigURI = SegmentGenerationUtils
          .generateTableConfigURI(pinotClusterSpec.getControllerURI(), _spec.getTableSpec().getTableName());
      _spec.getTableSpec().setTableConfigURI(tableConfigURI);
    }

    TableConfig tableConfig =
        SegmentGenerationUtils.getTableConfig(_spec.getTableSpec().getTableConfigURI(), _spec.getAuthToken());
    boolean consistentPushEnabled = ConsistentDataPushUtils.consistentDataPushEnabled(tableConfig);

    Map<URI, String> uriToLineageEntryIdMap = null;
    if (consistentPushEnabled) {
      List<String> segmentNames = getSegmentsToReplace(segmentsToPushUriToTarPathMap);
      uriToLineageEntryIdMap = ConsistentDataPushUtils.preUpload(_spec, segmentNames);
    }

    int pushParallelism = _spec.getPushJobSpec().getPushParallelism();
    if (pushParallelism < 1) {
      pushParallelism = segmentsToPush.size();
    }

    if (pushParallelism == 1) {
      // Push from driver
      if (consistentPushEnabled) {
        SegmentPushUtils.sendSegmentUriAndMetadata(_spec, PinotFSFactory.create(outputDirURI.getScheme()),
                segmentsToPushUriToTarPathMap);
        try {
          ConsistentDataPushUtils.postUpload(_spec, uriToLineageEntryIdMap);
        } catch (Exception e) {
          ConsistentDataPushUtils.handleUploadException(_spec, uriToLineageEntryIdMap, e);
          throw new RuntimeException(e);
        }
      } else {
        try {
          SegmentPushUtils.pushSegments(_spec, outputDirFS, segmentsToPush);
        } catch (RetriableOperationException | AttemptsExceededException e) {
          throw new RuntimeException(e);
        }
      }
    } else {
      // Push from Spark executors when pushParallelism > 1
      JavaSparkContext sparkContext = JavaSparkContext.fromSparkContext(SparkContext.getOrCreate());
      JavaRDD<String> pathRDD = sparkContext.parallelize(segmentsToPush, pushParallelism);
      URI finalOutputDirURI = outputDirURI;

      if (consistentPushEnabled) {
        // Parallel upload tasks executed by Spark executors
        // In this case, we need to handle the case where the Spark job fails
        sparkContext.sc().addSparkListener(new ConsistentDataPushFailureHandler(_spec, uriToLineageEntryIdMap));

        pathRDD.foreach(segmentTarPath -> {
          PluginManager.get().init();
          for (PinotFSSpec pinotFSSpec : pinotFSSpecs) {
            PinotFSFactory.register(pinotFSSpec.getScheme(), pinotFSSpec.getClassName(),
                new PinotConfiguration(pinotFSSpec));
          }
          Map<String, String> segmentUriToTarPathMap = SegmentPushUtils.getSegmentUriToTarPathMap(finalOutputDirURI,
              _spec.getPushJobSpec(), new String[]{segmentTarPath});
          SegmentPushUtils.sendSegmentUriAndMetadata(_spec, PinotFSFactory.create(finalOutputDirURI.getScheme()),
              segmentUriToTarPathMap);
        });

        // Once all segments are uploaded, execute post-upload
        try {
          ConsistentDataPushUtils.postUpload(_spec, uriToLineageEntryIdMap);
        } catch (Exception e) {
          ConsistentDataPushUtils.handleUploadException(_spec, uriToLineageEntryIdMap, e);
          throw new RuntimeException(e);
        }
      } else {
        // Non-consistent data push tasks executed by Spark executors
        pathRDD.foreach(segmentTarPath -> {
          PluginManager.get().init();
          for (PinotFSSpec pinotFSSpec : pinotFSSpecs) {
            PinotFSFactory.register(pinotFSSpec.getScheme(), pinotFSSpec.getClassName(),
                new PinotConfiguration(pinotFSSpec));
          }
          try {
            Map<String, String> segmentUriToTarPathMap = SegmentPushUtils.getSegmentUriToTarPathMap(finalOutputDirURI,
                _spec.getPushJobSpec(), new String[]{segmentTarPath});
            SegmentPushUtils.sendSegmentUriAndMetadata(_spec, PinotFSFactory.create(finalOutputDirURI.getScheme()),
                segmentUriToTarPathMap);
          } catch (RetriableOperationException | AttemptsExceededException e) {
            throw new RuntimeException(e);
          }
        });
      }
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
