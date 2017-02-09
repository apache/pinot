package com.linkedin.thirdeye.anomaly.detection;

import com.linkedin.thirdeye.client.DAORegistry;
import com.linkedin.thirdeye.datalayer.bao.DataCompletenessConfigManager;
import com.linkedin.thirdeye.datalayer.dto.AnomalyFunctionDTO;
import com.linkedin.thirdeye.datalayer.dto.DataCompletenessConfigDTO;
import java.util.Collections;
import org.joda.time.DateTime;
import org.mockito.Mockito;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;


public class TestDetectionTaskRunner {

  static final DAORegistry DAO_REGISTRY = DAORegistry.getInstance();

  static final DateTime NOW = DateTime.now();
  static final DateTime LATER = NOW.plusDays(1);
  static final DetectionTaskInfo TASK_INFO = new DetectionTaskInfo(0, NOW, LATER, new AnomalyFunctionDTO(), "dataset");

  DetectionTaskRunner runner;

  @BeforeTest
  void before() {
    runner = new DetectionTaskRunner();

    DataCompletenessConfigManager mock_dccm = Mockito.mock(DataCompletenessConfigManager.class);
    Mockito.when(mock_dccm.findAllByDatasetAndInTimeRangeAndStatus("dataset0", NOW.getMillis(), LATER.getMillis(), true)).thenReturn(
        Collections.emptyList());
    Mockito.when(mock_dccm.findAllByDatasetAndInTimeRangeAndStatus("dataset1", NOW.getMillis(), LATER.getMillis(), true)).thenReturn(
        Collections.singletonList(new DataCompletenessConfigDTO()));
    DAO_REGISTRY.setDataCompletenessConfigDAO(mock_dccm);

  }

  @Test
  void testCompletenessCheckAssertPass() {
    runner.assertCompletenessCheck("dataset1");
  }

  @Test(expectedExceptions = IllegalStateException.class)
  void testCompletenessCheckAssertFail() {
    runner.assertCompletenessCheck("dataset0");
  }

}
