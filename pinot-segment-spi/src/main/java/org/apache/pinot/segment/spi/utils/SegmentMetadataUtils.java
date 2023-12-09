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
package org.apache.pinot.segment.spi.utils;

import com.google.common.base.Preconditions;
import java.io.File;
import java.util.Map;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.segment.spi.store.SegmentDirectoryPaths;
import org.apache.pinot.spi.env.CommonsConfigurationUtils;


public class SegmentMetadataUtils {
  private SegmentMetadataUtils() {
  }

  public static PropertiesConfiguration getPropertiesConfiguration(File indexDir)
      throws ConfigurationException {
    File metadataFile = SegmentDirectoryPaths.findMetadataFile(indexDir);
    Preconditions.checkNotNull(metadataFile, "Cannot find segment metadata file under directory: %s", indexDir);
    return CommonsConfigurationUtils.fromFile(metadataFile);
  }

  public static PropertiesConfiguration getPropertiesConfiguration(SegmentMetadata segmentMetadata)
      throws ConfigurationException {
    File indexDir = segmentMetadata.getIndexDir();
    Preconditions.checkState(indexDir != null, "Cannot get PropertiesConfiguration from in-memory segment: %s",
        segmentMetadata.getName());
    return getPropertiesConfiguration(indexDir);
  }

  public static void savePropertiesConfiguration(PropertiesConfiguration propertiesConfiguration, File indexDir) {
    File metadataFile = SegmentDirectoryPaths.findMetadataFile(indexDir);
    Preconditions.checkNotNull(metadataFile, "Cannot find segment metadata file under directory: %s", indexDir);
    CommonsConfigurationUtils.saveToFile(propertiesConfiguration, metadataFile);
  }

  public static SegmentMetadata updateMetadataProperties(SegmentDirectory segmentDirectory,
      Map<String, String> metadataProperties)
      throws Exception {
    SegmentMetadata segmentMetadata = segmentDirectory.getSegmentMetadata();
    PropertiesConfiguration propertiesConfiguration = SegmentMetadataUtils.getPropertiesConfiguration(segmentMetadata);
    for (Map.Entry<String, String> entry : metadataProperties.entrySet()) {
      propertiesConfiguration.setProperty(entry.getKey(), entry.getValue());
    }
    savePropertiesConfiguration(propertiesConfiguration, segmentMetadata.getIndexDir());
    segmentDirectory.reloadMetadata();
    return segmentDirectory.getSegmentMetadata();
  }
}
