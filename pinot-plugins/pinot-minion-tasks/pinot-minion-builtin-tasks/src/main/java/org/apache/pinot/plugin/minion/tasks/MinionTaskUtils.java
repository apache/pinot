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
package org.apache.pinot.plugin.minion.tasks;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import org.apache.helix.HelixAdmin;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.common.utils.config.InstanceUtils;
import org.apache.pinot.controller.helix.core.minion.ClusterInfoAccessor;
import org.apache.pinot.controller.util.ServerSegmentMetadataReader;
import org.apache.pinot.minion.MinionContext;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.filesystem.LocalPinotFS;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.apache.pinot.spi.filesystem.PinotFSFactory;
import org.apache.pinot.spi.ingestion.batch.BatchConfigProperties;
import org.apache.pinot.spi.plugin.PluginManager;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.IngestionConfigUtils;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MinionTaskUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(MinionTaskUtils.class);

  private static final String DEFAULT_DIR_PATH_TERMINATOR = "/";

  private MinionTaskUtils() {
  }

  public static PinotFS getInputPinotFS(Map<String, String> taskConfigs, URI fileURI)
      throws Exception {
    String fileURIScheme = fileURI.getScheme();
    if (fileURIScheme == null) {
      return new LocalPinotFS();
    }
    // Try to create PinotFS using given Input FileSystem config always
    String fsClass = taskConfigs.get(BatchConfigProperties.INPUT_FS_CLASS);
    if (fsClass != null) {
      PinotFS pinotFS = PluginManager.get().createInstance(fsClass);
      PinotConfiguration fsProps = IngestionConfigUtils.getInputFsProps(taskConfigs);
      pinotFS.init(fsProps);
      return pinotFS;
    }
    return PinotFSFactory.create(fileURIScheme);
  }

  public static PinotFS getOutputPinotFS(Map<String, String> taskConfigs, URI fileURI)
      throws Exception {
    String fileURIScheme = (fileURI == null) ? null : fileURI.getScheme();
    if (fileURIScheme == null) {
      return new LocalPinotFS();
    }
    // Try to create PinotFS using given Input FileSystem config always
    String fsClass = taskConfigs.get(BatchConfigProperties.OUTPUT_FS_CLASS);
    if (fsClass != null) {
      PinotFS pinotFS = PluginManager.get().createInstance(fsClass);
      PinotConfiguration fsProps = IngestionConfigUtils.getOutputFsProps(taskConfigs);
      pinotFS.init(fsProps);
      return pinotFS;
    }
    return PinotFSFactory.create(fileURIScheme);
  }

  public static Map<String, String> getPushTaskConfig(String tableName, Map<String, String> taskConfigs,
      ClusterInfoAccessor clusterInfoAccessor) {
    try {
      String pushMode = IngestionConfigUtils.getPushMode(taskConfigs);

      Map<String, String> singleFileGenerationTaskConfig = new HashMap<>(taskConfigs);
      if (pushMode == null
          || pushMode.toUpperCase().contentEquals(BatchConfigProperties.SegmentPushType.TAR.toString())) {
        singleFileGenerationTaskConfig.put(BatchConfigProperties.PUSH_MODE,
            BatchConfigProperties.SegmentPushType.TAR.toString());
      } else {
        URI outputDirURI = URI.create(
            normalizeDirectoryURI(clusterInfoAccessor.getDataDir()) + TableNameBuilder.extractRawTableName(tableName));
        String outputDirURIScheme = outputDirURI.getScheme();

        if (!isLocalOutputDir(outputDirURIScheme)) {
          singleFileGenerationTaskConfig.put(BatchConfigProperties.OUTPUT_SEGMENT_DIR_URI, outputDirURI.toString());
          if (pushMode.toUpperCase().contentEquals(BatchConfigProperties.SegmentPushType.URI.toString())) {
            LOGGER.warn("URI push type is not supported in this task. Switching to METADATA push");
            pushMode = BatchConfigProperties.SegmentPushType.METADATA.toString();
          }
          singleFileGenerationTaskConfig.put(BatchConfigProperties.PUSH_MODE, pushMode);
        } else {
          LOGGER.warn("segment upload with METADATA push is not supported with local output dir: {}."
              + " Switching to TAR push.", outputDirURI);
          singleFileGenerationTaskConfig.put(BatchConfigProperties.PUSH_MODE,
              BatchConfigProperties.SegmentPushType.TAR.toString());
        }
      }
      singleFileGenerationTaskConfig.put(BatchConfigProperties.PUSH_CONTROLLER_URI, clusterInfoAccessor.getVipUrl());
      return singleFileGenerationTaskConfig;
    } catch (Exception e) {
      return taskConfigs;
    }
  }

  public static boolean isLocalOutputDir(String outputDirURIScheme) {
    return outputDirURIScheme == null || outputDirURIScheme.startsWith("file");
  }

  public static PinotFS getLocalPinotFs() {
    return new LocalPinotFS();
  }

  public static String normalizeDirectoryURI(URI dirURI) {
    return normalizeDirectoryURI(dirURI.toString());
  }

  public static String normalizeDirectoryURI(String dirInStr) {
    if (!dirInStr.endsWith(DEFAULT_DIR_PATH_TERMINATOR)) {
      return dirInStr + DEFAULT_DIR_PATH_TERMINATOR;
    }
    return dirInStr;
  }

  public static RoaringBitmap getValidDocIds(String tableNameWithType, String segmentName, Map<String, String> configs,
      MinionContext minionContext) {
    HelixAdmin helixAdmin = minionContext.getHelixManager().getClusterManagmentTool();
    String clusterName = minionContext.getHelixManager().getClusterName();

    String server = getServer(segmentName, tableNameWithType, helixAdmin, clusterName);
    InstanceConfig instanceConfig = helixAdmin.getInstanceConfig(clusterName, server);
    String endpoint = InstanceUtils.getServerAdminEndpoint(instanceConfig);

    // We only need aggregated table size and the total number of docs/rows. Skipping column related stats, by
    // passing an empty list.
    ServerSegmentMetadataReader serverSegmentMetadataReader = new ServerSegmentMetadataReader();
    return serverSegmentMetadataReader.getValidDocIdsFromServer(tableNameWithType, segmentName, endpoint, 60_000);
  }

  public static String getServer(String segmentName, String tableNameWithType, HelixAdmin helixAdmin,
      String clusterName) {
    ExternalView externalView = helixAdmin.getResourceExternalView(clusterName, tableNameWithType);
    if (externalView == null) {
      throw new IllegalStateException("External view does not exist for table: " + tableNameWithType);
    }
    Map<String, String> instanceStateMap = externalView.getStateMap(segmentName);
    if (instanceStateMap == null) {
      throw new IllegalStateException("Failed to find segment: " + segmentName);
    }
    for (Map.Entry<String, String> entry : instanceStateMap.entrySet()) {
      if (entry.getValue().equals(CommonConstants.Helix.StateModel.SegmentStateModel.ONLINE)) {
        return entry.getKey();
      }
    }
    throw new IllegalStateException("Failed to find ONLINE server for segment: " + segmentName);
  }
}
