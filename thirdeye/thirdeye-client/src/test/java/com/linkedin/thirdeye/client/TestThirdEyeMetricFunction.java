package com.linkedin.thirdeye.client;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.linkedin.thirdeye.api.TimeGranularity;
import com.linkedin.thirdeye.client.ThirdEyeMetricFunction.Expression;

public class TestThirdEyeMetricFunction {

  private static final TimeGranularity DEFAULT_GRANULARITY = new TimeGranularity(1, TimeUnit.HOURS);
  private static final List<String> DEFAULT_METRICS = Arrays.asList("A", "C", "RATIO(A,B)", "B");

  @Test(dataProvider = "fromStrProvider")
  public void fromStr(String test, List<String> expectedRawMetrics, int expectedExpressionCount,
      TimeGranularity expectedGranularity) {
    ThirdEyeMetricFunction func = ThirdEyeMetricFunction.fromStr(test);
    List<String> rawMetricNames = func.getRawMetricNames();
    Assert.assertEquals(rawMetricNames, expectedRawMetrics);

    List<Expression> metricExpressions = func.getMetricExpressions();
    Assert.assertEquals(metricExpressions.size(), expectedExpressionCount);
    for (Expression expression : metricExpressions) {
      Assert.assertEquals(expression.isAtomic(), !expression.getName().contains("RATIO"));
      if (!expression.isAtomic()) {
        Assert.assertEquals(expression.getArguments().size(), 2);
      }
    }

    TimeGranularity timeGranularity = func.getTimeGranularity();
    Assert.assertEquals(timeGranularity, expectedGranularity);

    String sqlFunction = func.getSqlFunction();
    Assert.assertEquals(sqlFunction.toLowerCase(), test.replaceAll("'", "").toLowerCase());
  }

  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Metric function must match pattern.*")
  public void fromStrInvalidFunction() {
    ThirdEyeMetricFunction.fromStr("MOVING_AVERAGE_1_HOURS(m1)");
  }

  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Metric function must match pattern.*")
  public void fromStrInvalidAggSize() {
    ThirdEyeMetricFunction.fromStr("AGGREGATE_ONE_HOURS(m1)");
  }

  @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "No enum constant java.util.concurrent.TimeUnit.WEAK")
  public void fromStrInvalidUnit() {
    ThirdEyeMetricFunction.fromStr("AGGREGATE_1_WEAK(m1)");
    Assert.fail("Need to expect the right exception here");
  }

  @Test
  public void getRawMetrics() {
    ThirdEyeMetricFunction function =
        new ThirdEyeMetricFunction(DEFAULT_GRANULARITY, DEFAULT_METRICS);

    List<String> rawMetricNames = function.getRawMetricNames();
    Assert.assertEquals(Arrays.asList("A", "B", "C"), rawMetricNames);
  }

  @Test
  public void getMetrics() {
    ThirdEyeMetricFunction function =
        new ThirdEyeMetricFunction(DEFAULT_GRANULARITY, DEFAULT_METRICS);
    List<Expression> metricExpressions = function.getMetricExpressions();
    Assert.assertEquals(DEFAULT_METRICS.size(), metricExpressions.size());
    for (int i = 0; i < DEFAULT_METRICS.size(); i++) {
      String defaultMetric = DEFAULT_METRICS.get(i);
      Expression expression = metricExpressions.get(i);
      if (defaultMetric.startsWith(ThirdEyeMetricFunction.Expression.RATIO)) {
        Assert.assertFalse(expression.isAtomic());
        Assert.assertEquals("RATIO", expression.getFunctionName());
        List<String> args = expression.getArguments();
        Assert.assertEquals(2, args.size());
        Assert.assertEquals(defaultMetric, String.format("RATIO(%s,%s)", args.get(0), args.get(1)));
      } else {
        Assert.assertTrue(expression.isAtomic());
        Assert.assertEquals(defaultMetric, expression.getAtomicValue());
      }

    }
  }

  @Test(dataProvider = "ratioExpressionProvider")
  public void ratioExpression(String ratioStr, String expectedFunctionName, List<String> metrics) {
    Expression expression = new Expression(ratioStr);
    Assert.assertFalse(expression.isAtomic());
    Assert.assertEquals(expression.getFunctionName(), expectedFunctionName);
    Assert.assertEquals(expression.getArguments(), metrics);
  }

  @Test(dataProvider = "atomicExpressionProvider")
  public void atomicExpression(String atomicStr) {
    Expression expression = new Expression(atomicStr);
    Assert.assertTrue(expression.isAtomic());
    Assert.assertEquals(expression.getAtomicValue(), atomicStr);
  }

  @DataProvider(name = "fromStrProvider")
  public static Object[][] fromStrProvider() {
    List<String> expectedRawMetrics = Arrays.asList("impressions", "submits");
    int expressionCount = 3;
    TimeGranularity timeGranularity = new TimeGranularity(1, TimeUnit.HOURS);
    String noQuotes = "AGGREGATE_1_HOURS(submits,impressions,RATIO(submits,impressions))";
    String ratioQuotes = "AGGREGATE_1_HOURS(submits,impressions,RATIO('submits','impressions'))";
    String baseQuotes = "AGGREGATE_1_HOURS('submits','impressions',RATIO(submits,impressions))";
    String lowercaseAgg = "aggregate_1_hours(submits,impressions,RATIO(submits,impressions))";
    String[] testStrs = new String[] {
        noQuotes, ratioQuotes, baseQuotes, lowercaseAgg
    };
    Object[][] result = new Object[testStrs.length][];
    for (int i = 0; i < testStrs.length; i++) {
      String string = testStrs[i];
      result[i] = new Object[] {
          string, expectedRawMetrics, expressionCount, timeGranularity
      };
    }
    return result;
  }

  @DataProvider(name = "ratioExpressionProvider")
  public static Object[][] ratioExpressionProvider() {
    // resulting expression should be identical.

    List<String> metricsM1M2 = Arrays.asList("m1", "m2");
    String standard = "RATIO(m1,m2)"; // normal, uppercase
    String metricSpace1 = "RATIO(m1,m2)"; // with a space just before metric
    String metricSpace2 = "RATIO(m1 ,m2)"; // with a space just after metrics
    String parenSpace = "RATIO( m1,m2 )"; // spaces before/after parens
    String quotedMetrics = "RATIO('m1','m2')"; // legacy behavior: quoted metrics
    String lowercaseRatio = "ratio(m1,m2)"; // handle lowercase RATIO
    String[] testStrs = new String[] {
        standard, metricSpace1, metricSpace2, parenSpace, quotedMetrics, lowercaseRatio
    };
    Object[][] result = new Object[testStrs.length][];
    for (int i = 0; i < testStrs.length; i++) {
      String string = testStrs[i];
      result[i] = new Object[] {
          string, ThirdEyeMetricFunction.Expression.RATIO, metricsM1M2
      };
    }
    return result;
  }

  @DataProvider(name = "atomicExpressionProvider")
  public static Object[][] atomicExpressionProvider() {
    String alphabetical = "abc";
    String numerical = "123";
    String alphanumeric = "abc123";
    String nonRatio = "UNIQUE(m1)"; // TODO anything non-regex is considered atomic. Maybe there
                                    // should be a check for function calls?

    String[] testStrs = new String[] {
        alphabetical, numerical, alphabetical, nonRatio
    };
    Object[][] result = new Object[testStrs.length][];
    for (int i = 0; i < testStrs.length; i++) {
      String string = testStrs[i];
      result[i] = new Object[] {
          string
      };
    }
    return result;
  }
}
