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
package org.apache.pinot.plugin.segmentuploader;

import com.google.common.base.Preconditions;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.segment.local.utils.IngestionUtils;
import org.apache.pinot.spi.auth.AuthProvider;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.apache.pinot.spi.ingestion.batch.BatchConfig;
import org.apache.pinot.spi.ingestion.batch.BatchConfigProperties;
import org.apache.pinot.spi.ingestion.batch.spec.Constants;
import org.apache.pinot.spi.ingestion.segment.uploader.SegmentUploader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Default implementation of {@link SegmentUploader} with support for all push modes
 * The configs for push are fetched from batchConfigMaps of tableConfig
 */
public class SegmentUploaderDefault implements SegmentUploader {

  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentUploaderDefault.class);

  private String _tableNameWithType;
  private BatchConfig _batchConfig;

  @Override
  public void init(TableConfig tableConfig) throws Exception {
    init(tableConfig, Collections.emptyMap());
  }

  @Override
  public void init(TableConfig tableConfig, Map<String, String> batchConfigOverride)
      throws Exception {
    _tableNameWithType = tableConfig.getTableName();

    Preconditions.checkState(
        tableConfig.getIngestionConfig() != null && tableConfig.getIngestionConfig().getBatchIngestionConfig() != null
            && CollectionUtils
            .isNotEmpty(tableConfig.getIngestionConfig().getBatchIngestionConfig().getBatchConfigMaps()),
        "Must provide ingestionConfig->batchIngestionConfig->batchConfigMaps in tableConfig for table: %s",
        _tableNameWithType);
    Preconditions
        .checkState(tableConfig.getIngestionConfig().getBatchIngestionConfig().getBatchConfigMaps().size() == 1,
            "batchConfigMaps must contain only 1 BatchConfig for table: %s", _tableNameWithType);

    // apply config override provided by user.
    Map<String, String> batchConfigMap = new HashMap<>(
        tableConfig.getIngestionConfig().getBatchIngestionConfig().getBatchConfigMaps().get(0));
    batchConfigMap.putAll(batchConfigOverride);

    _batchConfig = new BatchConfig(_tableNameWithType, batchConfigMap);

    Preconditions.checkState(StringUtils.isNotBlank(_batchConfig.getPushControllerURI()),
        "Must provide: %s in batchConfigs for table: %s", BatchConfigProperties.PUSH_CONTROLLER_URI,
        _tableNameWithType);

    LOGGER.info("Initialized {} for table: {}", SegmentUploaderDefault.class.getName(), _tableNameWithType);
  }

  @Override
  public void uploadSegment(URI segmentTarURI, @Nullable AuthProvider authProvider)
      throws Exception {
    IngestionUtils
        .uploadSegment(_tableNameWithType, _batchConfig, Collections.singletonList(segmentTarURI), authProvider);
    LOGGER.info("Successfully uploaded segment: {} to table: {}", segmentTarURI, _tableNameWithType);
  }

  @Override
  public void uploadSegmentsFromDir(URI segmentDir, @Nullable AuthProvider authProvider)
      throws Exception {

    List<URI> segmentTarURIs = new ArrayList<>();
    PinotFS outputPinotFS = IngestionUtils.getOutputPinotFS(_batchConfig, segmentDir);
    String[] filePaths = outputPinotFS.listFiles(segmentDir, true);
    for (String filePath : filePaths) {
      URI uri = URI.create(filePath);
      if (!outputPinotFS.isDirectory(uri) && filePath.endsWith(Constants.TAR_GZ_FILE_EXT)) {
        segmentTarURIs.add(uri);
      }
    }
    IngestionUtils.uploadSegment(_tableNameWithType, _batchConfig, segmentTarURIs, authProvider);
    LOGGER.info("Successfully uploaded segments: {} to table: {}", segmentTarURIs, _tableNameWithType);
  }
}
