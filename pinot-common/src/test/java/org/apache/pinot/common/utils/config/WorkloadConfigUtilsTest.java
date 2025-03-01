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

import java.util.ArrayList;
import java.util.List;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.spi.config.workload.EnforcementProfile;
import org.apache.pinot.spi.config.workload.WorkloadConfig;
import org.apache.pinot.spi.config.workload.WorkloadCost;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class WorkloadConfigUtilsTest {

  @Test(dataProvider = "fromZNRecordDataProvider")
  public void testFromZNRecord(ZNRecord znRecord, WorkloadConfig expectedWorkloadConfig, boolean shouldFail) {
    try {
      WorkloadConfig actualWorkloadConfig = WorkloadConfigUtils.fromZNRecord(znRecord);
      if (shouldFail) {
        Assert.fail("Expected an exception but none was thrown");
      }
      Assert.assertEquals(actualWorkloadConfig, expectedWorkloadConfig);
    } catch (Exception e) {
      if (!shouldFail) {
        Assert.fail("Caught unexpected exception: " + e.getMessage(), e);
      }
    }
  }

  @DataProvider(name = "fromZNRecordDataProvider")
  public Object[][] fromZNRecordDataProvider() {
    List<Object[]> data = new ArrayList<>();

    WorkloadCost validWorkloadCost = new WorkloadCost(100.0, 200.0);
    EnforcementProfile validEnforcementProfile = new EnforcementProfile(validWorkloadCost, validWorkloadCost);
    WorkloadConfig validWorkloadConfig = new WorkloadConfig("workloadId", validEnforcementProfile);

    // Valid case
    ZNRecord validZnRecord = new ZNRecord("workloadId");
    validZnRecord.setSimpleField("enforcementProfile", validEnforcementProfile.toJsonString());
    data.add(new Object[]{validZnRecord, validWorkloadConfig, false});

    // Case: Missing `enforcementProfile`
    ZNRecord missingEnforcementProfile = new ZNRecord("workloadId");
    data.add(new Object[]{missingEnforcementProfile, null, true});

    // Case: Invalid JSON in `enforcementProfile`
    ZNRecord invalidJsonRecord = new ZNRecord("workloadId");
    invalidJsonRecord.setSimpleField("enforcementProfile", "{invalid_json}");
    data.add(new Object[]{invalidJsonRecord, null, true});

    // Case: Null ZNRecord
    data.add(new Object[]{null, null, true});

    // Case: Empty ZNRecord
    ZNRecord emptyZnRecord = new ZNRecord("");
    data.add(new Object[]{emptyZnRecord, null, true});

    // Case: Zero workload costs
    WorkloadCost zeroWorkloadCost = new WorkloadCost(0.0, 0.0);
    EnforcementProfile zeroEnforcementProfile = new EnforcementProfile(zeroWorkloadCost, zeroWorkloadCost);
    WorkloadConfig zeroWorkloadConfig = new WorkloadConfig("zeroWorkload", zeroEnforcementProfile);

    ZNRecord zeroZnRecord = new ZNRecord("zeroWorkload");
    zeroZnRecord.setSimpleField("enforcementProfile", zeroEnforcementProfile.toJsonString());
    data.add(new Object[]{zeroZnRecord, zeroWorkloadConfig, false});

    // Case: Unexpected additional fields
    ZNRecord extraFieldsRecord = new ZNRecord("workloadId");
    extraFieldsRecord.setSimpleField("enforcementProfile", validEnforcementProfile.toJsonString());
    extraFieldsRecord.setSimpleField("extraField", "extraValue");
    data.add(new Object[]{extraFieldsRecord, validWorkloadConfig, false});

    return data.toArray(new Object[0][]);
  }

  @Test(dataProvider = "updateZNRecordDataProvider")
  public void testUpdateZNRecordWithWorkloadConfig(WorkloadConfig workloadConfig, ZNRecord znRecord,
      ZNRecord expectedZnRecord, boolean shouldFail) {
    try {
      WorkloadConfigUtils.updateZNRecordWithWorkloadConfig(znRecord, workloadConfig);
      if (shouldFail) {
        Assert.fail("Expected an exception but none was thrown");
      }
      Assert.assertEquals(znRecord, expectedZnRecord);
    } catch (Exception e) {
      if (!shouldFail) {
        Assert.fail("Caught unexpected exception: " + e.getMessage(), e);
      }
    }
  }

  @DataProvider(name = "updateZNRecordDataProvider")
  public Object[][] updateZNRecordDataProvider() {
    List<Object[]> data = new ArrayList<>();

    // Valid WorkloadConfig with positive workload costs
    WorkloadCost validWorkloadCost = new WorkloadCost(100.0, 200.0);
    EnforcementProfile validEnforcementProfile = new EnforcementProfile(validWorkloadCost, validWorkloadCost);
    WorkloadConfig validWorkloadConfig = new WorkloadConfig("workloadId", validEnforcementProfile);

    ZNRecord validZnRecord = new ZNRecord("workloadId");
    ZNRecord expectedValidZnRecord = new ZNRecord("workloadId");
    expectedValidZnRecord.setSimpleField("enforcementProfile", validEnforcementProfile.toJsonString());

    data.add(new Object[]{validWorkloadConfig, validZnRecord, expectedValidZnRecord, false}); // should not fail

    // Case: WorkloadConfig with zero workload costs
    WorkloadCost zeroWorkloadCost = new WorkloadCost(0.0, 0.0);
    EnforcementProfile zeroEnforcementProfile = new EnforcementProfile(zeroWorkloadCost, zeroWorkloadCost);
    WorkloadConfig zeroWorkloadConfig = new WorkloadConfig("zeroWorkload", zeroEnforcementProfile);

    ZNRecord zeroZnRecord = new ZNRecord("zeroWorkload");
    ZNRecord expectedZeroZnRecord = new ZNRecord("zeroWorkload");
    expectedZeroZnRecord.setSimpleField("enforcementProfile", zeroEnforcementProfile.toJsonString());

    data.add(new Object[]{zeroWorkloadConfig, zeroZnRecord, expectedZeroZnRecord, false}); // should not fail

    // Case: Null WorkloadConfig (should fail)
    ZNRecord znRecordWithNullWorkload = new ZNRecord("nullWorkload");
    data.add(new Object[]{null, znRecordWithNullWorkload, null, true}); // should fail

    // Case: Null ZNRecord (should fail)
    data.add(new Object[]{validWorkloadConfig, null, null, true}); // should fail

    // Case: Empty WorkloadConfig
    WorkloadConfig emptyWorkloadConfig = new WorkloadConfig("emptyWorkload", null);
    ZNRecord emptyZnRecord = new ZNRecord("emptyWorkload");
    data.add(new Object[]{emptyWorkloadConfig, emptyZnRecord, null, true}); // should fail

    return data.toArray(new Object[0][]);
  }
}
