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
package org.apache.pinot.controller.validation;

import java.util.List;
import java.util.Properties;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.metrics.ValidationMetrics;
import org.apache.pinot.common.restlet.resources.PauseStatusDetails;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.realtime.PinotLLCRealtimeSegmentManager;
import org.apache.pinot.core.realtime.impl.fakestream.FakeStreamConfigUtils;
import org.apache.pinot.segment.local.utils.SegmentReplacementUtils;
import org.apache.pinot.spi.config.table.PauseState;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.mockito.MockedStatic;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;
import static org.testng.Assert.expectThrows;


/// Replacement collection must reuse the metadata already fetched for validation, including failures and empty tables.
public class SegmentReplacementValidationTest {
  @DataProvider
  public Object[][] tableTypes() {
    return new Object[][]{{TableType.OFFLINE}, {TableType.REALTIME}};
  }

  @Test(dataProvider = "tableTypes")
  public void testCleanupReusesValidationSnapshot(TableType type) {
    checkValidation(type, false);
  }

  @Test(dataProvider = "tableTypes")
  public void testFailedMetadataReadDoesNotRunCleanup(TableType type) {
    checkValidation(type, true);
  }

  private void checkValidation(TableType type, boolean failRead) {
    String tableName = "test_" + type;
    PinotHelixResourceManager resourceManager = mock(PinotHelixResourceManager.class);
    ControllerConf config = new ControllerConf();
    config.setDataDir("s3://controller/deep-store");
    TableConfigBuilder builder = new TableConfigBuilder(type).setTableName("test");
    if (type == TableType.REALTIME) {
      builder.setStreamConfigs(FakeStreamConfigUtils.getDefaultLowLevelStreamConfigs().getStreamConfigsMap());
    }
    TableConfig table = builder.build();
    when(resourceManager.getTableConfig(tableName)).thenReturn(table);
    List<SegmentZKMetadata> snapshot = List.of();
    if (failRead) {
      when(resourceManager.getSegmentsZKMetadata(tableName))
          .thenThrow(new IllegalStateException("metadata unavailable"));
    } else {
      when(resourceManager.getSegmentsZKMetadata(tableName)).thenReturn(snapshot);
    }
    Runnable validate;
    if (type == TableType.OFFLINE) {
      OfflineSegmentValidationManager manager = new OfflineSegmentValidationManager(config, resourceManager, null,
          mock(ValidationMetrics.class), mock(ControllerMetrics.class), mock(ResourceUtilizationManager.class));
      validate = () -> manager.processTable(tableName, manager.preprocess(new Properties()));
    } else {
      PinotLLCRealtimeSegmentManager llcManager = mock(PinotLLCRealtimeSegmentManager.class);
      when(llcManager.getPauseStatusDetails(tableName))
          .thenReturn(new PauseStatusDetails(true, null, PauseState.ReasonCode.ADMINISTRATIVE, null, null));
      RealtimeSegmentValidationManager manager = new RealtimeSegmentValidationManager(config, resourceManager, null,
          llcManager, mock(ValidationMetrics.class), mock(ControllerMetrics.class), mock(StorageQuotaChecker.class),
          mock(ResourceUtilizationManager.class));
      validate = () -> manager.processTable(tableName, manager.preprocess(new Properties()));
    }
    try (MockedStatic<SegmentReplacementUtils> cleanup = mockStatic(SegmentReplacementUtils.class)) {
      if (failRead) {
        expectThrows(IllegalStateException.class, validate::run);
        cleanup.verifyNoInteractions();
      } else {
        validate.run();
        cleanup.verify(() -> SegmentReplacementUtils.cleanup(config.getDataDir(), tableName, snapshot));
      }
      verify(resourceManager).getSegmentsZKMetadata(tableName);
      verify(resourceManager, never()).getSegmentZKMetadata(anyString(), anyString());
    }
  }
}
