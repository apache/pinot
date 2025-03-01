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
package org.apache.pinot.common.utils.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.spi.config.workload.EnforcementProfile;
import org.apache.pinot.spi.config.workload.WorkloadConfig;


public class WorkloadConfigUtils {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private WorkloadConfigUtils() {
  }

  /**
   * Converts a ZNRecord into a WorkloadConfig object by extracting mapFields.
   *
   * @param znRecord The ZNRecord containing workload config data.
   * @return A WorkloadConfig object.
   */
  public static WorkloadConfig fromZNRecord(ZNRecord znRecord) throws Exception {
    Preconditions.checkNotNull(znRecord, "ZNRecord cannot be null");
    String workloadId = znRecord.getId();
    String enforcementProfileJson = znRecord.getSimpleField("enforcementProfile");
    Preconditions.checkArgument(enforcementProfileJson != null, "enforcementProfile field is missing");
    EnforcementProfile enforcementProfile = OBJECT_MAPPER.readValue(enforcementProfileJson, EnforcementProfile.class);
    return new WorkloadConfig(workloadId, enforcementProfile);
  }

  /**
   * Updates a ZNRecord with the fields from a WorkloadConfig object.
   *
   * @param workloadConfig The WorkloadConfig object to convert.
   * @param znRecord The ZNRecord to update.
   */
  public static void updateZNRecordWithWorkloadConfig(ZNRecord znRecord, WorkloadConfig workloadConfig)
      throws Exception {
    Preconditions.checkNotNull(workloadConfig, "WorkloadConfig cannot be null");
    Preconditions.checkNotNull(znRecord, "ZNRecord cannot be null");
    String enforcementProfileJson = OBJECT_MAPPER.writeValueAsString(workloadConfig.getEnforcementProfile());
    znRecord.setSimpleField("enforcementProfile", enforcementProfileJson);
  }
}
