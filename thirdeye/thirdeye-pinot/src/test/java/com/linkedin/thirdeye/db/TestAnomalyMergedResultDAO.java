package com.linkedin.thirdeye.db;

import com.linkedin.thirdeye.anomaly.merge.AnomalyMergeConfig;
import com.linkedin.thirdeye.anomaly.merge.AnomalySummaryGenerator;
import com.linkedin.thirdeye.db.dao.AbstractDbTestBase;
import com.linkedin.thirdeye.db.entity.AnomalyFunctionSpec;
import com.linkedin.thirdeye.db.entity.AnomalyMergedResult;
import com.linkedin.thirdeye.db.entity.AnomalyResult;
import java.util.ArrayList;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestAnomalyMergedResultDAO extends AbstractDbTestBase {
  Long mergedResultId;
  Long anomalyResultId;
  AnomalyFunctionSpec spec = getTestFunctionSpec("metric", "dataset");

  @Test
  public void testMergedResultCRUD() {
    anomalyFunctionDAO.save(spec);
    Assert.assertNotNull(spec.getId());

    // create anomaly result
    AnomalyResult result = getAnomalyResult();
    result.setFunction(spec);
    anomalyResultDAO.save(result);

    AnomalyResult resultRet = anomalyResultDAO.findById(result.getId());
    Assert.assertEquals(resultRet.getFunction(), spec);

    anomalyResultId = result.getId();

    // Let's create merged result

    List<AnomalyResult> rawResults = new ArrayList<>();
    rawResults.add(result);

    AnomalyMergeConfig mergeConfig = new AnomalyMergeConfig();

    List<AnomalyMergedResult> mergedResults = AnomalySummaryGenerator.mergeAnomalies(rawResults, mergeConfig);
    Assert.assertEquals(mergedResults.get(0).getStartTime(),result.getStartTimeUtc());
    Assert.assertEquals(mergedResults.get(0).getEndTime(),result.getEndTimeUtc());
    Assert.assertEquals(mergedResults.get(0).getAnomalyResults().get(0), result);

    // Lets persist the merged result
    mergedResultDAO.save(mergedResults.get(0));
    mergedResultId = mergedResults.get(0).getId();
    Assert.assertNotNull(mergedResultId);
  }
}
