package org.apache.pinot.thirdeye.detection.performance;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.pinot.thirdeye.dashboard.resources.v2.pojo.AnomalyClassificationType;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class PerformanceMetricsTest {
  private List<String> noResponses;
  private List<String> oneTruePos;
  private List<String> allTruePos;
  private List<String> allFalsePos;
  private List<String> mixedPos;
  private List<String> posAndNeg;
  private final String NONE = AnomalyClassificationType.NONE.toString();
  private final String TRUE_POS = AnomalyClassificationType.TRUE_POSITIVE.toString();
  private final String FALSE_POS = AnomalyClassificationType.FALSE_POSITIVE.toString();
  private final String FALSE_NEG = AnomalyClassificationType.FALSE_NEGATIVE.toString();

  private double calculateResponseRate (long respondedAnomalies, long numAnomalies) {
    return (double) respondedAnomalies / numAnomalies * 100;
  }

  private double calculatePrecision (long truePos, long falsePos) {
    return (double) truePos / (truePos + falsePos) * 100;
  }

  private double calculateRecall (long truePos, long falseNeg) {
    return (double) truePos / (truePos + falseNeg) * 100;
  }

  @BeforeMethod
  public void setUp() {
    this.noResponses = Stream.of(NONE, NONE, NONE, NONE).collect(Collectors.toList());
    this.oneTruePos = Stream.of(TRUE_POS, NONE, NONE, NONE).collect(Collectors.toList());
    this.allTruePos = Stream.of(TRUE_POS, TRUE_POS, TRUE_POS, TRUE_POS).collect(Collectors.toList());
    this.allFalsePos = Stream.of(FALSE_POS, FALSE_POS, FALSE_POS, FALSE_POS).collect(Collectors.toList());
    this.mixedPos = Stream.of(FALSE_POS, TRUE_POS, TRUE_POS, TRUE_POS).collect(Collectors.toList());
    this.posAndNeg = Stream.of(FALSE_NEG, TRUE_POS, TRUE_POS, NONE).collect(Collectors.toList());
  }

  @Test
  public void testBuildTotalAnomalies() {
    final long numAnomalies = 4L;
    PerformanceMetric totalAnomalies = new PerformanceMetric();

    totalAnomalies.setValue((double) numAnomalies);
    totalAnomalies.setType(PerformanceMetricType.COUNT);

    PerformanceMetrics pm = new PerformanceMetrics.Builder(this.noResponses)
        .addTotalAnomalies()
        .build();

    Assert.assertEquals(pm.getTotalAnomalies().getValue(), totalAnomalies.getValue());
    Assert.assertEquals(pm.getTotalAnomalies().getType(), totalAnomalies.getType());
  }

  @Test
  public void testBuildResponseRate() {
    final long numAnomalies = 4L;
    final long zeroRespondedAnomalies = 0L;
    final long oneRespondedAnomalies = 1L;
    PerformanceMetric noneResponded = new PerformanceMetric();
    PerformanceMetric oneResponded = new PerformanceMetric();

    noneResponded.setValue(calculateResponseRate(zeroRespondedAnomalies, numAnomalies));
    noneResponded.setType(PerformanceMetricType.PERCENT);

    oneResponded.setValue(calculateResponseRate(oneRespondedAnomalies, numAnomalies));
    oneResponded.setType(PerformanceMetricType.PERCENT);

    PerformanceMetrics pmNone = new PerformanceMetrics.Builder(this.noResponses)
        .addResponseRate()
        .build();
    PerformanceMetrics pmOne = new PerformanceMetrics.Builder(this.oneTruePos)
        .addResponseRate()
        .build();

    Assert.assertEquals(pmNone.getResponseRate().getValue(), noneResponded.getValue());
    Assert.assertEquals(pmOne.getResponseRate().getValue(), oneResponded.getValue());
    Assert.assertEquals(pmNone.getResponseRate().getType(), noneResponded.getType());
    Assert.assertEquals(pmOne.getResponseRate().getType(), oneResponded.getType());
  }

  @Test
  public void testBuildPrecision() {
    final long zeroTruePos = 0L;
    final long zeroFalsePos = 0L;
    final long fourTruePos = 4L;
    final long fourFalsePos = 4L;
    final long oneFalsePos = 1L;
    final long threeTruePos = 3L;
    PerformanceMetric noneResponded = new PerformanceMetric();
    PerformanceMetric allTrue = new PerformanceMetric();
    PerformanceMetric allFalse = new PerformanceMetric();
    PerformanceMetric mixed = new PerformanceMetric();

    noneResponded.setValue(calculatePrecision(zeroTruePos, zeroFalsePos));
    noneResponded.setType(PerformanceMetricType.PERCENT);

    allTrue.setValue(calculatePrecision(fourTruePos, zeroFalsePos));
    allTrue.setType(PerformanceMetricType.PERCENT);

    allFalse.setValue(calculatePrecision(zeroTruePos, fourFalsePos));
    allFalse.setType(PerformanceMetricType.PERCENT);

    mixed.setValue(calculatePrecision(threeTruePos, oneFalsePos));
    mixed.setType(PerformanceMetricType.PERCENT);

    PerformanceMetrics pmNone = new PerformanceMetrics.Builder(this.noResponses)
        .addPrecision()
        .build();
    PerformanceMetrics pmAllTrue = new PerformanceMetrics.Builder(this.allTruePos)
        .addPrecision()
        .build();
    PerformanceMetrics pmAllFalse = new PerformanceMetrics.Builder(this.allFalsePos)
        .addPrecision()
        .build();
    PerformanceMetrics pmMixed = new PerformanceMetrics.Builder(this.mixedPos)
        .addPrecision()
        .build();

    Assert.assertEquals(pmNone.getPrecision().getValue(), noneResponded.getValue());
    Assert.assertEquals(pmAllTrue.getPrecision().getValue(), allTrue.getValue());
    Assert.assertEquals(pmAllFalse.getPrecision().getValue(), allFalse.getValue());
    Assert.assertEquals(pmMixed.getPrecision().getValue(), mixed.getValue());
    Assert.assertEquals(pmNone.getPrecision().getType(), noneResponded.getType());
    Assert.assertEquals(pmAllTrue.getPrecision().getType(), allTrue.getType());
    Assert.assertEquals(pmAllFalse.getPrecision().getType(), allFalse.getType());
    Assert.assertEquals(pmMixed.getPrecision().getType(), mixed.getType());
  }

  @Test
  public void testBuildRecall() {
    final long zeroTruePos = 0L;
    final long zeroFalseNeg = 0L;
    final long fourTruePos = 4L;
    final long oneFalseNeg = 1L;
    final long twoTruePos = 2L;
    PerformanceMetric noneResponded = new PerformanceMetric();
    PerformanceMetric allTrue = new PerformanceMetric();
    PerformanceMetric mixed = new PerformanceMetric();

    noneResponded.setValue(calculateRecall(zeroTruePos, zeroFalseNeg));
    noneResponded.setType(PerformanceMetricType.PERCENT);

    allTrue.setValue(calculateRecall(fourTruePos, zeroFalseNeg));
    allTrue.setType(PerformanceMetricType.PERCENT);

    mixed.setValue(calculateRecall(twoTruePos, oneFalseNeg));
    mixed.setType(PerformanceMetricType.PERCENT);

    PerformanceMetrics pmNone = new PerformanceMetrics.Builder(this.noResponses)
        .addRecall()
        .build();
    PerformanceMetrics pmAllTrue = new PerformanceMetrics.Builder(this.allTruePos)
        .addRecall()
        .build();
    PerformanceMetrics pmMixed = new PerformanceMetrics.Builder(this.posAndNeg)
        .addRecall()
        .build();

    Assert.assertEquals(pmNone.getRecall().getValue(), noneResponded.getValue());
    Assert.assertEquals(pmAllTrue.getRecall().getValue(), allTrue.getValue());
    Assert.assertEquals(pmMixed.getRecall().getValue(), mixed.getValue());
    Assert.assertEquals(pmNone.getRecall().getType(), noneResponded.getType());
    Assert.assertEquals(pmAllTrue.getRecall().getType(), allTrue.getType());
    Assert.assertEquals(pmMixed.getRecall().getType(), mixed.getType());
  }
}
