package org.apache.pinot.thirdeye.dashboard.resources;

import org.apache.pinot.thirdeye.datalayer.dto.MetricConfigDTO;
import org.testng.Assert;
import org.testng.annotations.Test;


public class SummaryResourceTest {
  @Test
  public void testIsSimpleRatioMetric() {
    // False: null metric config
    Assert.assertFalse(SummaryResource.isSimpleRatioMetric(null));
    // False: empty metric config
    Assert.assertFalse(SummaryResource.isSimpleRatioMetric(new MetricConfigDTO()));

    MetricConfigDTO ratioMetricConfigDTO = new MetricConfigDTO();
    // True: ratio metric
    ratioMetricConfigDTO.setDerivedMetricExpression("id123/id456");
    Assert.assertTrue(SummaryResource.isSimpleRatioMetric(ratioMetricConfigDTO));

    // False: derived metric with empty expression
    ratioMetricConfigDTO.setDerivedMetricExpression("");
    Assert.assertFalse(SummaryResource.isSimpleRatioMetric(ratioMetricConfigDTO));

    // False: derived metric with null expression
    ratioMetricConfigDTO.setDerivedMetricExpression(null);
    Assert.assertFalse(SummaryResource.isSimpleRatioMetric(ratioMetricConfigDTO));

    // False: percentage metric
    ratioMetricConfigDTO.setDerivedMetricExpression("id123*100/id456");
    Assert.assertFalse(SummaryResource.isSimpleRatioMetric(ratioMetricConfigDTO));

    // False: complex derived metric
    ratioMetricConfigDTO.setDerivedMetricExpression("id123/id456/id789");
    Assert.assertFalse(SummaryResource.isSimpleRatioMetric(ratioMetricConfigDTO));
  }

  @Test
  public void testParseNumeratorDenominatorId() {
    // Fail: empty metric expression
    SummaryResource.MatchedRatioMetricsResult matchedRatioMetricsResult =
        SummaryResource.parseNumeratorDenominatorId("");
    Assert.assertFalse(matchedRatioMetricsResult.hasFound);

    // Fail: null metric expression
    matchedRatioMetricsResult = SummaryResource.parseNumeratorDenominatorId(null);
    Assert.assertFalse(matchedRatioMetricsResult.hasFound);

    // The other failure cases should be covered by testIsSimpleRatioMetric() since they use the same regex expression.

    // Success: simple ratio metric expression
    matchedRatioMetricsResult = SummaryResource.parseNumeratorDenominatorId("id123/id456");
    Assert.assertTrue(matchedRatioMetricsResult.hasFound);
    Assert.assertEquals(matchedRatioMetricsResult.numeratorId, 123);
    Assert.assertEquals(matchedRatioMetricsResult.denominatorId, 456);
  }
}
