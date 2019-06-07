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
package org.apache.pinot.common.metadata.dataset;

import org.apache.helix.AccessOption;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.pinot.common.metadata.ZKMetadataProvider;


/**
 * Utility class for Dataset Metadata
 */
public class DatasetMetadataUtil {

  public static DatasetMetadata fetchDatasetMetadata(ZkHelixPropertyStore<ZNRecord> propertyStore,
      String datasetNameWithType) {
    String path =
        ZKMetadataProvider.constructPropertyStorePathForDatasetMetadata(datasetNameWithType);

    // Fetch the instance partitions from property store if exists
    ZNRecord znRecord = propertyStore.get(path, null, AccessOption.PERSISTENT);
    if (znRecord != null) {
      return DatasetMetadata.fromZNRecord(znRecord);
    }
    return new DatasetMetadata(datasetNameWithType);
  }

  public static boolean persistDatasetMetadata(ZkHelixPropertyStore<ZNRecord> propertyStore,
      DatasetMetadata datasetMetadata) {
    String path =
        ZKMetadataProvider.constructPropertyStorePathForDatasetMetadata(datasetMetadata.getDatasetNameWithType());
    return propertyStore.set(path, datasetMetadata.toZNRecord(), AccessOption.PERSISTENT);
  }
}
