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
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import javax.annotation.Nullable;
import org.apache.helix.HelixAdmin;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.common.restlet.resources.ValidDocIdsBitmapResponse;
import org.apache.pinot.common.utils.RoaringBitmapUtils;
import org.apache.pinot.common.utils.config.InstanceUtils;
import org.apache.pinot.controller.helix.core.minion.ClusterInfoAccessor;
import org.apache.pinot.controller.util.ServerSegmentMetadataReader;
import org.apache.pinot.minion.MinionContext;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableTaskConfig;
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

  public static final String DATETIME_PATTERN = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
  public static final String UTC = "UTC";

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
      if (pushMode == null || pushMode.toUpperCase()
          .contentEquals(BatchConfigProperties.SegmentPushType.TAR.toString())) {
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

  public static List<String> getServers(String segmentName, String tableNameWithType, HelixAdmin helixAdmin,
      String clusterName) {
    ExternalView externalView = helixAdmin.getResourceExternalView(clusterName, tableNameWithType);
    if (externalView == null) {
      throw new IllegalStateException("External view does not exist for table: " + tableNameWithType);
    }
    Map<String, String> instanceStateMap = externalView.getStateMap(segmentName);
    if (instanceStateMap == null) {
      throw new IllegalStateException("Failed to find segment: " + segmentName);
    }
    ArrayList<String> servers = new ArrayList<>();
    for (Map.Entry<String, String> entry : instanceStateMap.entrySet()) {
      if (entry.getValue().equals(CommonConstants.Helix.StateModel.SegmentStateModel.ONLINE)) {
        servers.add(entry.getKey());
      }
    }
    if (servers.isEmpty()) {
      throw new IllegalStateException("Failed to find any ONLINE servers for segment: " + segmentName);
    }
    return servers;
  }

  /**
   * Extract allowDownloadFromServer config from table task config
   */
  public static boolean extractMinionAllowDownloadFromServer(TableConfig tableConfig, String taskType,
      boolean defaultValue) {
    TableTaskConfig tableTaskConfig = tableConfig.getTaskConfig();
    if (tableTaskConfig != null) {
      Map<String, String> configs = tableTaskConfig.getConfigsForTaskType(taskType);
      if (configs != null && !configs.isEmpty()) {
        return Boolean.parseBoolean(
            configs.getOrDefault(TableTaskConfig.MINION_ALLOW_DOWNLOAD_FROM_SERVER, String.valueOf(defaultValue)));
      }
    }
    return defaultValue;
  }

  /**
   * Returns the validDocID bitmap from the server whose local segment crc matches both crc of ZK metadata and
   * deepstore copy (expectedCrc).
   */
  @Nullable
  public static RoaringBitmap getValidDocIdFromServerMatchingCrc(String tableNameWithType, String segmentName,
      String validDocIdsType, MinionContext minionContext, String expectedCrc) {
    String clusterName = minionContext.getHelixManager().getClusterName();
    HelixAdmin helixAdmin = minionContext.getHelixManager().getClusterManagmentTool();
    RoaringBitmap validDocIds = null;
    List<String> servers = getServers(segmentName, tableNameWithType, helixAdmin, clusterName);
    for (String server : servers) {
      InstanceConfig instanceConfig = helixAdmin.getInstanceConfig(clusterName, server);
      String endpoint = InstanceUtils.getServerAdminEndpoint(instanceConfig);

      // We only need aggregated table size and the total number of docs/rows. Skipping column related stats, by
      // passing an empty list.
      ServerSegmentMetadataReader serverSegmentMetadataReader = new ServerSegmentMetadataReader();
      ValidDocIdsBitmapResponse validDocIdsBitmapResponse;
      try {
        validDocIdsBitmapResponse =
            serverSegmentMetadataReader.getValidDocIdsBitmapFromServer(tableNameWithType, segmentName, endpoint,
                validDocIdsType, 60_000);
      } catch (Exception e) {
        LOGGER.warn("Unable to retrieve validDocIds bitmap for segment: " + segmentName + " from endpoint: "
            + endpoint, e);
        continue;
      }

      // Check crc from the downloaded segment against the crc returned from the server along with the valid doc id
      // bitmap. If this doesn't match, this means that we are hitting the race condition where the segment has been
      // uploaded successfully while the server is still reloading the segment. Reloading can take a while when the
      // offheap upsert is used because we will need to delete & add all primary keys.
      // `BaseSingleSegmentConversionExecutor.executeTask()` already checks for the crc from the task generator
      // against the crc from the current segment zk metadata, so we don't need to check that here.
      String crcFromValidDocIdsBitmap = validDocIdsBitmapResponse.getSegmentCrc();
      if (!expectedCrc.equals(crcFromValidDocIdsBitmap)) {
        // In this scenario, we are hitting the other replica of the segment which did not commit to ZK or deepstore.
        // We will skip processing this bitmap to query other server to confirm if there is a valid matching CRC.
        String message = "CRC mismatch for segment: " + segmentName + ", expected value based on task generator: "
            + expectedCrc + ", actual crc from validDocIdsBitmapResponse from endpoint " + endpoint + ": "
            + crcFromValidDocIdsBitmap;
        LOGGER.warn(message);
        continue;
      }
      validDocIds = RoaringBitmapUtils.deserialize(validDocIdsBitmapResponse.getBitmap());
      break;
    }
    return validDocIds;
  }

  public static String toUTCString(long epochMillis) {
    Date date = new Date(epochMillis);
    SimpleDateFormat isoFormat = new SimpleDateFormat(DATETIME_PATTERN);
    isoFormat.setTimeZone(TimeZone.getTimeZone(UTC));
    return isoFormat.format(date);
  }

  public static long fromUTCString(String utcString) {
    return Instant.parse(utcString).toEpochMilli();
  }
}
