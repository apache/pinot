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
package org.apache.pinot.tools.admin.command;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.common.auth.AuthProviderUtils;
import org.apache.pinot.common.metadata.segment.SegmentPartitionMetadata;
import org.apache.pinot.common.restlet.resources.StartReplaceSegmentsRequest;
import org.apache.pinot.common.utils.SegmentApiClient;
import org.apache.pinot.common.utils.SimpleHttpResponse;
import org.apache.pinot.common.utils.http.HttpClient;
import org.apache.pinot.plugin.ingestion.batch.common.SegmentGenerationTaskRunner;
import org.apache.pinot.plugin.minion.tasks.SegmentConversionUtils;
import org.apache.pinot.segment.local.utils.ConsistentDataPushUtils;
import org.apache.pinot.segment.spi.partition.PartitionFunctionFactory;
import org.apache.pinot.segment.spi.partition.metadata.ColumnPartitionMetadata;
import org.apache.pinot.spi.auth.AuthProvider;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.apache.pinot.spi.filesystem.PinotFSFactory;
import org.apache.pinot.spi.ingestion.batch.BatchConfigProperties.SegmentNameGeneratorType;
import org.apache.pinot.spi.ingestion.batch.IngestionJobLauncher;
import org.apache.pinot.spi.ingestion.batch.IngestionJobLauncher.PinotIngestionJobType;
import org.apache.pinot.spi.ingestion.batch.spec.Constants;
import org.apache.pinot.spi.ingestion.batch.spec.SegmentGenerationJobSpec;
import org.apache.pinot.spi.ingestion.batch.spec.SegmentNameGeneratorSpec;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.GroovyTemplateUtils;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

@CommandLine.Command(name = "LaunchBackfillIngestionJob")
public class LaunchBackfillIngestionJobCommand extends LaunchDataIngestionJobCommand {

  private static final Logger LOGGER = LoggerFactory.getLogger(LaunchBackfillIngestionJobCommand.class);

  private static final String OFFLINE_TABLE_TYPE = "OFFLINE";

  @CommandLine.Option(names = {"-startDate"}, required = true, description = "Backfill start date (inclusive). "
      + "Segments in backfill date range will be disabled.")
  private String _backfillStartDate;
  @CommandLine.Option(names = {"-endDate"}, required = true, description = "Backfill end date (exclusive). "
      + "Segments in backfill date range will be disabled.")
  private String _backfillEndDate;
  @CommandLine.Option(names = {"-partitionColumn"}, required = false, description = "The column name on which "
      + "segments were partitioned using BoundedColumnValue function.")
  private String _partitionColumn;
  @CommandLine.Option(names = {"-partitionColumnValue"}, required = false, description =
      "Segments with this partition column value " + "will only be eligible for backfill.")
  private String _partitionColumnValue;

  @Override
  public boolean execute() throws Exception {
    SegmentGenerationJobSpec spec;
    try {
      // Prepare job spec and controller client
      spec = prepareSegmentGeneratorSpec();
      URI controllerURI = URI.create(spec.getPinotClusterSpecs()[0].getControllerURI());
      SegmentApiClient segmentApiClient = new SegmentApiClient();

      // 1. Fetch existing segments that need to be backfilled (to be replaced)
      List<String> segmentsToBackfill = fetchSegmentsToBackfill(spec, segmentApiClient, controllerURI);

      // 2. Generate new segments locally
      List<String> generatedSegments = generateSegments(spec);

      // Build table name and authentication info
      String tableNameWithType = TableNameBuilder.forType(TableType.OFFLINE)
          .tableNameWithType(spec.getTableSpec().getTableName());
      String uploadURL = spec.getPinotClusterSpecs()[0].getControllerURI();
      AuthProvider authProvider = AuthProviderUtils.makeAuthProvider(spec.getAuthToken());

      // 3. Create a lineage entry to track replacement (old vs new segments)
      String segmentLineageEntryId = createSegmentLineageEntry(
          tableNameWithType, uploadURL, segmentsToBackfill, generatedSegments, authProvider);

      // 4. Push new segments to Pinot and mark lineage entry as completed
      pushSegmentsAndEndReplace(spec, tableNameWithType, uploadURL, segmentLineageEntryId, authProvider, controllerURI);
    } catch (Exception e) {
      LOGGER.error("Got exception to generate IngestionJobSpec for backfill ingestion job - ", e);
      throw e;
    }
    return true;
  }

  private SegmentGenerationJobSpec prepareSegmentGeneratorSpec() throws Exception {
    SegmentGenerationJobSpec spec;
    try {
       spec = IngestionJobLauncher.getSegmentGenerationJobSpec(
          _jobSpecFile, _propertyFile, GroovyTemplateUtils.getTemplateContext(_values), System.getenv());

      long currentTime = System.currentTimeMillis();
      spec.setOutputDirURI(spec.getOutputDirURI() + File.separator + currentTime);

      SegmentNameGeneratorSpec nameGeneratorSpec = spec.getSegmentNameGeneratorSpec();
      if (nameGeneratorSpec == null) {
        nameGeneratorSpec = new SegmentNameGeneratorSpec();
      }
      nameGeneratorSpec.setType(SegmentNameGeneratorType.SIMPLE);
      Map<String, String> configs =
          nameGeneratorSpec.getConfigs() != null ? nameGeneratorSpec.getConfigs() : new HashMap<>();
      configs.putIfAbsent(SegmentGenerationTaskRunner.SEGMENT_NAME_POSTFIX, String.valueOf(currentTime));
      nameGeneratorSpec.setConfigs(configs);
      spec.setSegmentNameGeneratorSpec(nameGeneratorSpec);
    } catch (Exception e) {
      LOGGER.error("Got exception to generate IngestionJobSpec for backill ingestion job - ", e);
      throw e;
    }

    return spec;
  }

  private List<String> fetchSegmentsToBackfill(SegmentGenerationJobSpec spec, SegmentApiClient segmentApiClient,
      URI controllerURI) throws Exception {
    List<String> segmentsToBackfill = new ArrayList<>();
    long startMs = getMillis(_backfillStartDate);
    long endMs = getMillis(_backfillEndDate);

    String segmentsJsonStr = segmentApiClient.selectSegments(
        SegmentApiClient.getSelectSegmentURI(controllerURI, spec.getTableSpec().getTableName(), OFFLINE_TABLE_TYPE),
        startMs, endMs, true, SegmentApiClient.makeAuthHeader(spec.getAuthToken())
    ).getResponse();

    JsonNode segmentsJson = new ObjectMapper().readTree(segmentsJsonStr);
    segmentsJson.findPath(OFFLINE_TABLE_TYPE).forEach(seg -> segmentsToBackfill.add(seg.textValue()));

    // Partition filter
    if (StringUtils.isNotEmpty(_partitionColumn) && StringUtils.isNotEmpty(_partitionColumnValue)) {
      segmentsToBackfill.removeIf(segmentName -> !isSegmentMatchPartition(spec, segmentName));
    }

    LOGGER.info("Existing segments: \n{}", JsonUtils.objectToString(segmentsToBackfill));
    return segmentsToBackfill;
  }

  /**
   * Checks if a segment matches the specified partition column and value.
   *
   * @param spec SegmentGenerationJobSpec containing table and cluster info
   * @param segmentName Name of the segment to check
   * @return true if the segment matches the partition, false otherwise
   */
  private boolean isSegmentMatchPartition(SegmentGenerationJobSpec spec, String segmentName) {
    try {
      URI controllerURI = URI.create(spec.getPinotClusterSpecs()[0].getControllerURI());

      // Fetch segment metadata from the controller
      SimpleHttpResponse response = new SegmentApiClient().getSegmentMetadata(
          SegmentApiClient.getSegmentMetadataURI(controllerURI,
              TableNameBuilder.OFFLINE.tableNameWithType(spec.getTableSpec().getTableName()),
              segmentName),
          SegmentApiClient.makeAuthHeader(spec.getAuthToken())
      );

      Map<String, String> segmentMetadata = JsonUtils.stringToObject(
          response.getResponse(), new TypeReference<Map<String, String>>() {
          }
      );

      // Skip segments without partition metadata
      if (!segmentMetadata.containsKey(CommonConstants.Segment.PARTITION_METADATA)) {
        return false;
      }

      SegmentPartitionMetadata partitionMetadata =
          SegmentPartitionMetadata.fromJsonString(segmentMetadata.get(CommonConstants.Segment.PARTITION_METADATA));

      ColumnPartitionMetadata columnMetadata = partitionMetadata.getColumnPartitionMap().get(_partitionColumn);
      if (columnMetadata == null || columnMetadata.getPartitions().size() != 1) {
        return false;
      }

      // Compute partition ID for the specified partition column value
      int partitionId = PartitionFunctionFactory.getPartitionFunction(
          columnMetadata.getFunctionName(),
          columnMetadata.getNumPartitions(),
          columnMetadata.getFunctionConfig()
      ).getPartition(_partitionColumnValue);

      // Return true if segment contains the computed partition
      return columnMetadata.getPartitions().contains(partitionId);
    } catch (Exception e) {
      LOGGER.warn("Failed to fetch partition info for segment [{}], skipping.", segmentName, e);
      return false;
    }
  }

  private List<String> generateSegments(SegmentGenerationJobSpec spec) throws Exception {
    spec.setJobType(String.valueOf(PinotIngestionJobType.SegmentCreation));
    IngestionJobLauncher.runIngestionJob(spec);

    List<String> generatedSegments = collectSegmentNames(spec);
    LOGGER.info("New segments: \n{}", JsonUtils.objectToString(generatedSegments));
    return generatedSegments;
  }

  private List<String> collectSegmentNames(SegmentGenerationJobSpec spec) {
    URI outputDirURI;
    try {
      outputDirURI = new URI(spec.getOutputDirURI());
      if (outputDirURI.getScheme() == null) {
        outputDirURI = new File(spec.getOutputDirURI()).toURI();
      }
    } catch (URISyntaxException e) {
      throw new RuntimeException("outputDirURI is not valid - '" + spec.getOutputDirURI() + "'");
    }
    PinotFS outputDirFS = PinotFSFactory.create(outputDirURI.getScheme());

    // Get list of files to process
    String[] files;
    try {
      files = outputDirFS.listFiles(outputDirURI, true);
    } catch (IOException e) {
      throw new RuntimeException("Unable to list all files under outputDirURI - '" + outputDirURI + "'");
    }

    List<String> generatedSegments = new ArrayList<>();
    for (String file : files) {
      if (file.endsWith(Constants.TAR_GZ_FILE_EXT)) {
        generatedSegments.add(
            file.substring(file.lastIndexOf(File.separator) + 1, file.indexOf(Constants.TAR_GZ_FILE_EXT)));
      }
    }
    return generatedSegments;
  }

  private String createSegmentLineageEntry(String tableNameWithType, String uploadURL,
      List<String> segmentsToBackfill, List<String> generatedSegments,
      AuthProvider authProvider) throws Exception {
    LOGGER.info("Start replacing segments by creating a lineage entry.");
    String segmentLineageEntryId = SegmentConversionUtils.startSegmentReplace(
        tableNameWithType, uploadURL, new StartReplaceSegmentsRequest(segmentsToBackfill, generatedSegments),
        authProvider, false);
    LOGGER.info("Created segment lineage entry: [{}]", segmentLineageEntryId);
    return segmentLineageEntryId;
  }

  private void pushSegmentsAndEndReplace(SegmentGenerationJobSpec spec, String tableNameWithType,
      String uploadURL, String segmentLineageEntryId,
      AuthProvider authProvider, URI controllerURI) throws Exception {
    try {
      spec.setJobType(String.valueOf(PinotIngestionJobType.SegmentTarPush));
      IngestionJobLauncher.runIngestionJob(spec);

      LOGGER.info("End replacing segments by updating lineage entry: [{}]", segmentLineageEntryId);
      SegmentConversionUtils.endSegmentReplace(tableNameWithType, uploadURL, segmentLineageEntryId,
          HttpClient.DEFAULT_SOCKET_TIMEOUT_MS, authProvider);
      LOGGER.info("New segments are pushed successfully and now serving queries.");
    } catch (Exception e) {
      LOGGER.error("Failed to upload segments, reverting lineage entry.", e);
      Map<URI, String> uriToLineageEntryIdMap = Collections.singletonMap(controllerURI, segmentLineageEntryId);
      ConsistentDataPushUtils.handleUploadException(spec, uriToLineageEntryIdMap, null);
      LOGGER.info("Reverted lineage entry [{}]. Any newly uploaded segments should have been deleted.",
          segmentLineageEntryId);
      throw e;
    }
  }

  @Override
  public String getName() {
    return "LaunchBackfillIngestionJob";
  }

  @Override
  public String description() {
    return "Launch a backfill ingestion job.";
  }

  private long getMillis(String backfillDate) {
    return LocalDate.parse(backfillDate).atStartOfDay(ZoneId.of("UTC")).toInstant().toEpochMilli();
  }
}
