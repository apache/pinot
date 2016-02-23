package com.linkedin.thirdeye.client;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Joiner;
import com.linkedin.thirdeye.api.TimeGranularity;

/**
 * Represents an ThirdEye metric function. Currently the only valid function is an aggregate call,
 * eg "AGGREGATE_1_HOURS(m1,m2)"
 * @author jteoh
 */
public class ThirdEyeMetricFunction {
  // TODO support other functions in ThirdEyeQueryParser.
  private static final Joiner COMMA = Joiner.on(",");
  private static final Pattern METRIC_FUNCTION_AGG_REGEX =
      Pattern.compile("AGGREGATE_(\\d+)_(\\w+)\\((.*)\\)", Pattern.CASE_INSENSITIVE);

  private final TimeGranularity timeGranularity;
  private final List<Expression> metricExpressions;

  public ThirdEyeMetricFunction(TimeGranularity timeGranularity, List<String> metrics) {
    if (timeGranularity == null || metrics == null || metrics.isEmpty()) {
      throw new IllegalArgumentException("Time granularity and metrics must not be null or empty");
    }
    this.timeGranularity = timeGranularity;
    metrics = trimQuotes(metrics);
    this.metricExpressions = Expression.fromExpressionStr(metrics);
  }

  public static ThirdEyeMetricFunction fromStr(String metricFunction) {
    Matcher matcher = METRIC_FUNCTION_AGG_REGEX.matcher(metricFunction);
    if (matcher.matches()) {
      String size = matcher.group(1);
      String unit = matcher.group(2).toUpperCase();
      String metrics = matcher.group(3);
      TimeGranularity timeGranularity =
          new TimeGranularity(Integer.valueOf(size), TimeUnit.valueOf(unit));
      List<String> expressionStrs = splitMetrics(metrics);
      return new ThirdEyeMetricFunction(timeGranularity, expressionStrs);
    } else {
      throw new IllegalArgumentException(
          "Metric function must match pattern " + METRIC_FUNCTION_AGG_REGEX);
    }
  }

  public TimeGranularity getTimeGranularity() {
    return timeGranularity;
  }

  public List<Expression> getMetricExpressions() {
    return metricExpressions;
  }

  /** Returns request metrics. For raw metrics, see {@link #getRawMetricNames()} */
  @JsonIgnore
  public List<String> getMetricNames() {
    ArrayList<String> names = new ArrayList<>(metricExpressions.size());
    for (Expression expression : metricExpressions) {
      names.add(expression.getName());
    }
    return names;
  }

  /**
   * Returns all unique raw metrics used in this function in sorted order. No duplicates are
   * returned (eg "A", "RATIO(B,A)" will not return "A" twice).
   */
  @JsonIgnore
  public List<String> getRawMetricNames() {
    Set<String> rawNameSet = new TreeSet<>();
    for (Expression expression : metricExpressions) {
      if (expression.isAtomic()) {
        rawNameSet.add(expression.getAtomicValue());
      } else {
        for (String arg : expression.getArguments()) {
          rawNameSet.add(arg);
        }
      }
    }
    return new ArrayList<>(rawNameSet);
  }

  public String getSqlFunction() {
    return String.format("AGGREGATE_%d_%s(%s)", timeGranularity.getSize(),
        timeGranularity.getUnit(), COMMA.join(metricExpressions));
  }

  @Override
  public String toString() {
    return getSqlFunction();
  }

  @Override
  public int hashCode() {
    return Objects.hash(timeGranularity, metricExpressions);
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof ThirdEyeMetricFunction)) {
      return false;
    }
    ThirdEyeMetricFunction other = (ThirdEyeMetricFunction) obj;
    return Objects.equals(other.timeGranularity, this.timeGranularity)
        && Objects.equals(other.metricExpressions, this.metricExpressions);
  }

  private static List<String> splitMetrics(String metrics) {
    if (metrics == null) {
      return null;
    }
    int parenDepth = 0;
    StringBuilder sb = new StringBuilder();
    ArrayList<String> metricList = new ArrayList<>();
    for (int i = 0; i < metrics.toCharArray().length; i++) {
      char c = metrics.toCharArray()[i];
      if (c == '(') {
        parenDepth++;
      } else if (c == ')') {
        parenDepth--;
      }

      if (c == ',' && parenDepth == 0) {
        // EXTRACT STRING
        metricList.add(sb.toString());
        sb.setLength(0);
      } else {
        sb.append(c);
      }
    }
    if (parenDepth != 0) {
      throw new IllegalArgumentException("Invalid parenthesis depth: " + metrics);
    }
    if (sb.length() > 0) {
      metricList.add(sb.toString());
    }
    return metricList;
  }

  private static List<String> trimQuotes(List<String> quotedStrings) {
    if (quotedStrings == null) {
      return null;
    }
    List<String> results = new ArrayList<>(quotedStrings.size());
    for (String string : quotedStrings) {
      if (string != null) {
        results.add(string.replaceAll("^['\"]|['\"]$", "").trim());
      }
    }
    return results;
  }

  /**
   * Represents an expression within a ThirdEye function call. At the moment, the only valid
   * expressions are literals and RATIO calls.
   * @author jteoh
   */
  public static class Expression {
    public static final String RATIO = "RATIO";
    // case-insensitive, with optional argument single quotes and whitespace adjacent to comma or
    // parens.
    private static final Pattern RATIO_REGEX = Pattern.compile(
        RATIO + "\\(\\s*?'?(\\w+)'?\\s*?,\\s*?'?(\\w+)'?\\s*?\\)", Pattern.CASE_INSENSITIVE);

    private final String singularValue; // reused for function name if not atomic
    private final List<String> arguments;

    Expression(String expr) {
      Matcher matcher = RATIO_REGEX.matcher(expr);
      if (matcher.matches()) {
        String first = matcher.group(1);
        String second = matcher.group(2);
        this.singularValue = RATIO;
        this.arguments = trimQuotes(Arrays.asList(first, second));
      } else {
        // anything not matching the ratio regex is considered atomic.
        this.singularValue = expr;
        this.arguments = null;
      }
    }

    static List<Expression> fromExpressionStr(List<String> expressionStrs) {
      ArrayList<Expression> expressions = new ArrayList<>(expressionStrs.size());
      for (String exprStr : expressionStrs) {
        expressions.add(new Expression(exprStr));
      }
      return expressions;
    }

    public boolean isAtomic() {
      return singularValue != null && arguments == null;
    }

    public String getAtomicValue() {
      if (isAtomic()) {
        return singularValue;
      } else {
        throw new UnsupportedOperationException("Non-atomic expression has no atomic value");
      }
    }

    public String getName() {
      if (isAtomic()) {
        return singularValue;
      } else {
        return String.format("%s(%s)", singularValue, COMMA.join(arguments));
      }
    }

    public String getFunctionName() {
      if (!isAtomic()) {
        return singularValue;
      } else {
        throw new UnsupportedOperationException("Atomic expression has no function call");
      }
    }

    public List<String> getArguments() {
      if (!isAtomic()) {
        return arguments;
      } else {
        throw new UnsupportedOperationException("Atomic expression has no arguments");
      }
    }

    @Override
    public String toString() {
      return getName();
    }

    @Override
    public int hashCode() {
      return Objects.hash(singularValue, arguments);
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof Expression)) {
        return false;
      }
      Expression other = (Expression) obj;
      return Objects.equals(other.singularValue, this.singularValue)
          && Objects.equals(other.arguments, this.arguments);
    }

  }
}
