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
package org.apache.pinot.core.query.optimizer.filter;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.common.function.DateTimeUtils;
import org.apache.pinot.common.request.Expression;
import org.apache.pinot.common.request.ExpressionType;
import org.apache.pinot.common.request.Function;
import org.apache.pinot.common.request.Literal;
import org.apache.pinot.common.utils.request.RequestUtils;
import org.apache.pinot.core.operator.transform.function.DateTimeConversionTransformFunction;
import org.apache.pinot.core.operator.transform.function.DateTruncTransformFunction;
import org.apache.pinot.core.operator.transform.function.TimeConversionTransformFunction;
import org.apache.pinot.spi.data.DateTimeFieldSpec.TimeFormat;
import org.apache.pinot.spi.data.DateTimeFormatSpec;
import org.apache.pinot.spi.data.DateTimeGranularitySpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.TimeUtils;
import org.apache.pinot.sql.FilterKind;
import org.joda.time.chrono.ISOChronology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The {@code TimePredicateFilterOptimizer} optimizes the time related predicates:
 * <ul>
 *   <li>
 *     Optimizes TIME_CONVERT/DATE_TIME_CONVERT function with range/equality predicate to directly apply the predicate
 *     to the inner expression.
 *     <p>E.g. "dateTimeConvert(col, '1:SECONDS:EPOCH', '1:MINUTES:EPOCH', '30:MINUTES') > 27013846" will be optimized
 *     to "col >= 1620831600".
 *     <p>NOTE: Other predicates such as NOT_EQUALS, IN, NOT_IN are not supported for now because these predicates are
 *     not common on time column, and they cannot be optimized to a single range predicate.
 *   </li>
 *   <li>
 *     Optimizes DATE_TRUNC function with range/equality predicates by either rounding up or down to closest granularity
 *     step
 *     <p>E.g. "dateTrunc('DAY', col, 'MILLISECONDS') > 1620777600000" will be optimized
 *     to "col > 1620863999999" as 1620863999999 is the largest value that can be truncated to 1620777600000
 *     <p>E.g. "datetrunc('DAY', col, 'MILLISECONDS') <= 1620777600010" will be optimized
 *     to col <= 1620863999999 as the next granularity step lower than 1620777600010 is 1620777600000 and 1620863999999
 *     is the largest value that truncates to be lower than the specified literal.
 *     <p>NOTE: Other predicates such as NOT_EQUALS, IN, NOT_IN are not supported for now because these predicates are
 *     not common on time column, and they cannot be optimized to a single range predicate.
 *     <p>NOTE: Timezones are not yet supported for the datetrunc optimizer</p>
 *   </li>
 * </ul>
 *
 * NOTE: This optimizer is followed by the {@link MergeRangeFilterOptimizer}, which can merge the generated ranges.
 */
public class TimePredicateFilterOptimizer implements FilterOptimizer {
  private static final Logger LOGGER = LoggerFactory.getLogger(TimePredicateFilterOptimizer.class);

  @Override
  public Expression optimize(Expression filterExpression, @Nullable Schema schema) {
    return filterExpression.getType() == ExpressionType.FUNCTION ? optimize(filterExpression) : filterExpression;
  }

  @VisibleForTesting
  static Expression optimize(Expression filterExpression) {
    Function filterFunction = filterExpression.getFunctionCall();
    FilterKind filterKind = FilterKind.valueOf(filterFunction.getOperator());
    List<Expression> operands = filterFunction.getOperands();
    if (filterKind == FilterKind.AND || filterKind == FilterKind.OR || filterKind == FilterKind.NOT) {
      // NOTE: We don't need to replace the children because all the changes are applied in-place
      for (Expression operand : operands) {
        optimize(operand);
      }
    } else if (filterKind.isRange() || filterKind == FilterKind.EQUALS) {
      Expression expression = operands.get(0);
      if (expression.getType() == ExpressionType.FUNCTION) {
        Function expressionFunction = expression.getFunctionCall();
        String functionName = StringUtils.remove(expressionFunction.getOperator(), '_');
        if (functionName.equalsIgnoreCase(TimeConversionTransformFunction.FUNCTION_NAME)) {
          optimizeTimeConvert(filterFunction, filterKind);
        } else if (functionName.equalsIgnoreCase(DateTimeConversionTransformFunction.FUNCTION_NAME)) {
          optimizeDateTimeConvert(filterFunction, filterKind);
        } else if (functionName.equalsIgnoreCase(DateTruncTransformFunction.FUNCTION_NAME)) {
          optimizeDateTrunc(filterFunction, filterKind);
        }
      }
    }
    return filterExpression;
  }

  /**
   * Helper method to optimize TIME_CONVERT function with range/equality predicate to directly apply the predicate to
   * the inner expression. Changes are applied in-place of the filter function.
   */
  private static void optimizeTimeConvert(Function filterFunction, FilterKind filterKind) {
    List<Expression> filterOperands = filterFunction.getOperands();
    List<Expression> timeConvertOperands = filterOperands.get(0).getFunctionCall().getOperands();
    Preconditions.checkArgument(timeConvertOperands.size() == 3,
        "Exactly 3 arguments are required for TIME_CONVERT transform function");
    Preconditions.checkArgument(
        isStringLiteral(timeConvertOperands.get(1)) && isStringLiteral(timeConvertOperands.get(2)),
        "The 2nd and 3rd argument for TIME_CONVERT transform function must be string literal");

    try {
      TimeUnit inputTimeUnit = TimeUnit.valueOf(timeConvertOperands.get(1).getLiteral().getStringValue().toUpperCase());
      TimeUnit outputTimeUnit =
          TimeUnit.valueOf(timeConvertOperands.get(2).getLiteral().getStringValue().toUpperCase());

      // For the same input and output time unit, directly remove the TIME_CONVERT function
      if (inputTimeUnit == outputTimeUnit) {
        filterOperands.set(0, timeConvertOperands.get(0));
        return;
      }

      // Step 1: Convert output range to millis range
      Long lowerMillis = null;
      Long upperMillis = null;
      switch (filterKind) {
        case GREATER_THAN: {
          // millisToFormat(millis) > n
          // -> millisToFormat(millis) >= n + 1
          // -> millis >= formatToMillis(n + 1)
          //
          // E.g.
          // millisToSeconds(millis) > 0
          // -> millisToSeconds(millis) >= 1
          // -> millis >= 1000
          //
          // Note that 'millisToSeconds(millis) > 0' is not equivalent to 'millis > 0'
          long lowerValue = getLongValue(filterOperands.get(1));
          lowerMillis = outputTimeUnit.toMillis(lowerValue + 1);
          Preconditions.checkState(TimeUtils.timeValueInValidRange(lowerMillis), "Invalid lower bound in millis: %s",
              lowerMillis);
          break;
        }
        case GREATER_THAN_OR_EQUAL: {
          // millisToFormat(millis) >= n
          // -> millis >= formatToMillis(n)
          long lowerValue = getLongValue(filterOperands.get(1));
          lowerMillis = outputTimeUnit.toMillis(lowerValue);
          Preconditions.checkState(TimeUtils.timeValueInValidRange(lowerMillis), "Invalid lower bound in millis: %s",
              lowerMillis);
          break;
        }
        case LESS_THAN: {
          // millisToFormat(millis) < n
          // -> millis < formatToMillis(n)
          long upperValue = getLongValue(filterOperands.get(1));
          upperMillis = outputTimeUnit.toMillis(upperValue);
          Preconditions.checkState(TimeUtils.timeValueInValidRange(upperMillis), "Invalid upper bound in millis: %s",
              upperMillis);
          break;
        }
        case LESS_THAN_OR_EQUAL: {
          // millisToFormat(millis) <= n
          // -> millisToFormat(millis) < n + 1
          // -> millis < formatToMillis(n + 1)
          //
          // E.g.
          // millisToSeconds(millis) <= 0
          // -> millisToSeconds(millis) < 1
          // -> millis < 1000
          //
          // Note that 'millisToSeconds(millis) <= 0' is not equivalent to 'millis <= 0'
          long upperValue = getLongValue(filterOperands.get(1));
          upperMillis = outputTimeUnit.toMillis(upperValue + 1);
          Preconditions.checkState(TimeUtils.timeValueInValidRange(upperMillis), "Invalid upper bound in millis: %s",
              upperMillis);
          break;
        }
        case BETWEEN: {
          // Combine GREATER_THAN_OR_EQUAL and LESS_THAN_OR_EQUAL
          long lowerValue = getLongValue(filterOperands.get(1));
          lowerMillis = outputTimeUnit.toMillis(lowerValue);
          Preconditions.checkState(TimeUtils.timeValueInValidRange(lowerMillis), "Invalid lower bound in millis: %s",
              lowerMillis);
          long upperValue = getLongValue(filterOperands.get(2));
          upperMillis = outputTimeUnit.toMillis(upperValue + 1);
          Preconditions.checkState(TimeUtils.timeValueInValidRange(upperMillis), "Invalid upper bound in millis: %s",
              upperMillis);
          break;
        }
        case EQUALS: {
          // Combine GREATER_THAN_OR_EQUAL and LESS_THAN_OR_EQUAL
          long value = getLongValue(filterOperands.get(1));
          lowerMillis = outputTimeUnit.toMillis(value);
          Preconditions.checkState(TimeUtils.timeValueInValidRange(lowerMillis), "Invalid lower bound in millis: %s",
              lowerMillis);
          upperMillis = outputTimeUnit.toMillis(value + 1);
          Preconditions.checkState(TimeUtils.timeValueInValidRange(upperMillis), "Invalid upper bound in millis: %s",
              upperMillis);
          break;
        }
        default:
          throw new IllegalStateException();
      }

      // Step 2: Convert millis range to input range
      Long lowerValue = null;
      boolean lowerInclusive = false;
      if (lowerMillis != null) {
        // formatToMillis(col) >= millis
        // - if (formatToMillis(millisToFormat(millis)) == millis)
        //   -> col >= millisToFormat(millis)
        // - else (formatToMillis(millisToFormat(millis)) < millis)
        //   -> col > millisToFormat(millis)
        //
        // E.g.
        // secondsToMillis(seconds) >= 123
        // -> seconds > 0
        // secondsToMillis(seconds) >= 0
        // -> seconds >= 0
        lowerValue = inputTimeUnit.convert(lowerMillis, TimeUnit.MILLISECONDS);
        lowerInclusive = inputTimeUnit.toMillis(lowerValue) == lowerMillis;
      }
      Long upperValue = null;
      boolean upperInclusive = false;
      if (upperMillis != null) {
        // formatToMillis(col) < millis
        // - if (formatToMillis(millisToFormat(millis)) == millis)
        //   -> col < millisToFormat(millis)
        // - else (formatToMillis(millisToFormat(millis)) < millis)
        //   -> col <= millisToFormat(millis)
        //
        // E.g.
        // secondsToMillis(seconds) < 123
        // -> seconds <= 0
        // secondsToMillis(seconds) < 0
        // -> seconds < 0
        upperValue = inputTimeUnit.convert(upperMillis, TimeUnit.MILLISECONDS);
        upperInclusive = inputTimeUnit.toMillis(upperValue) != upperMillis;
      }

      // Step 3: Rewrite the filter function
      String rangeString = new Range(lowerValue, lowerInclusive, upperValue, upperInclusive).getRangeString();
      rewriteToRange(filterFunction, timeConvertOperands.get(0), rangeString);
    } catch (Exception e) {
      LOGGER.warn("Caught exception while optimizing TIME_CONVERT predicate: {}, skipping the optimization",
          filterFunction, e);
    }
  }

  /**
   * Helper method to optimize DATE_TIME_CONVERT function with range/equality predicate to directly apply the predicate
   * to the inner expression. Changes are applied in-place of the filter function.
   */
  private static void optimizeDateTimeConvert(Function filterFunction, FilterKind filterKind) {
    List<Expression> filterOperands = filterFunction.getOperands();
    List<Expression> dateTimeConvertOperands = filterOperands.get(0).getFunctionCall().getOperands();

    // dateTimeConvert with bucketing time zone is not optimized yet
    if (dateTimeConvertOperands.size() == 5) {
      return;
    }

    Preconditions.checkArgument(dateTimeConvertOperands.size() == 4,
        "Exactly 4 arguments are required for DATE_TIME_CONVERT transform function");
    Preconditions.checkArgument(
        isStringLiteral(dateTimeConvertOperands.get(1)) && isStringLiteral(dateTimeConvertOperands.get(2))
            && isStringLiteral(dateTimeConvertOperands.get(3)),
        "The 2nd to 4th arguments for DATE_TIME_CONVERT transform function must be string literal");

    try {
      DateTimeFormatSpec inputFormat =
          new DateTimeFormatSpec(dateTimeConvertOperands.get(1).getLiteral().getStringValue());
      DateTimeFormatSpec outputFormat =
          new DateTimeFormatSpec(dateTimeConvertOperands.get(2).getLiteral().getStringValue());
      // SDF output format is not supported because:
      // 1. No easy way to get the next time value (instead of simply +1 for EPOCH format)
      // 2. Hard to calculate the bucket boundary (need to consider time zone)
      // TODO: Support SDF output format
      if (outputFormat.getTimeFormat() == TimeFormat.SIMPLE_DATE_FORMAT) {
        return;
      }
      long granularityMillis = new DateTimeGranularitySpec(
          dateTimeConvertOperands.get(3).getLiteral().getStringValue()).granularityToMillis();

      // Step 1: Convert output range to millis range
      Long lowerMillis = null;
      Long upperMillis = null;
      switch (filterKind) {
        case GREATER_THAN: {
          // millisToFormat(floor(millis, granularity)) > n
          // -> millisToFormat(floor(millis, granularity)) >= n + 1
          // -> floor(millis, granularity) >= formatToMillis(n + 1)
          // -> millis >= ceil(formatToMillis(n + 1), granularity)
          //
          // E.g.
          // millisToSeconds(floor(millis, 1 minute)) > 0
          // -> millisToSeconds(floor(millis, 1 minute)) >= 1
          // -> floor(millis, 1 minute) >= 1000
          // -> millis >= 60000
          //
          // Note that 'millisToSeconds(floor(millis, 1 minute)) > 0' is not equivalent to 'millis > 0'
          long lowerValue = getLongValue(filterOperands.get(1));
          lowerMillis = ceil(outputFormat.fromFormatToMillis(lowerValue + 1), granularityMillis);
          Preconditions.checkState(TimeUtils.timeValueInValidRange(lowerMillis), "Invalid lower bound in millis: %s",
              lowerMillis);
          break;
        }
        case GREATER_THAN_OR_EQUAL: {
          // millisToFormat(floor(millis, granularity)) >= n
          // -> floor(millis, granularity) >= formatToMillis(n)
          // -> millis >= ceil(formatToMillis(n), granularity)
          long lowerValue = getLongValue(filterOperands.get(1));
          lowerMillis = ceil(outputFormat.fromFormatToMillis(lowerValue), granularityMillis);
          Preconditions.checkState(TimeUtils.timeValueInValidRange(lowerMillis), "Invalid lower bound in millis: %s",
              lowerMillis);
          break;
        }
        case LESS_THAN: {
          // millisToFormat(floor(millis, granularity)) < n
          // -> floor(millis, granularity) < formatToMillis(n)
          // -> millis < ceil(formatToMillis(n), granularity)
          long upperValue = getLongValue(filterOperands.get(1));
          upperMillis = ceil(outputFormat.fromFormatToMillis(upperValue), granularityMillis);
          Preconditions.checkState(TimeUtils.timeValueInValidRange(upperMillis), "Invalid upper bound in millis: %s",
              upperMillis);
          break;
        }
        case LESS_THAN_OR_EQUAL: {
          // millisToFormat(floor(millis, granularity)) <= n
          // -> millisToFormat(floor(millis, granularity)) < n + 1
          // -> floor(millis, granularity) < formatToMillis(n + 1)
          // -> millis < ceil(formatToMillis(n + 1), granularity)
          //
          // E.g.
          // millisToSeconds(floor(millis, 1 minute)) <= 0
          // -> millisToSeconds(floor(millis, 1 minute)) < 1
          // -> floor(millis, 1 minute) < 1000
          // -> millis < 60000
          //
          // Note that 'millisToSeconds(floor(millis, 1 minute)) <= 0' is not equivalent to 'millis <= 0'
          long upperValue = getLongValue(filterOperands.get(1));
          upperMillis = ceil(outputFormat.fromFormatToMillis(upperValue + 1), granularityMillis);
          Preconditions.checkState(TimeUtils.timeValueInValidRange(upperMillis), "Invalid upper bound in millis: %s",
              upperMillis);
          break;
        }
        case BETWEEN: {
          // Combine GREATER_THAN_OR_EQUAL and LESS_THAN_OR_EQUAL
          long lowerValue = getLongValue(filterOperands.get(1));
          lowerMillis = ceil(outputFormat.fromFormatToMillis(lowerValue), granularityMillis);
          Preconditions.checkState(TimeUtils.timeValueInValidRange(lowerMillis), "Invalid lower bound in millis: %s",
              lowerMillis);
          long upperValue = getLongValue(filterOperands.get(2));
          upperMillis = ceil(outputFormat.fromFormatToMillis(upperValue + 1), granularityMillis);
          Preconditions.checkState(TimeUtils.timeValueInValidRange(upperMillis), "Invalid upper bound in millis: %s",
              upperMillis);
          break;
        }
        case EQUALS: {
          // Combine GREATER_THAN_OR_EQUAL and LESS_THAN_OR_EQUAL
          long value = getLongValue(filterOperands.get(1));
          lowerMillis = ceil(outputFormat.fromFormatToMillis(value), granularityMillis);
          Preconditions.checkState(TimeUtils.timeValueInValidRange(lowerMillis), "Invalid lower bound in millis: %s",
              lowerMillis);
          upperMillis = ceil(outputFormat.fromFormatToMillis(value + 1), granularityMillis);
          Preconditions.checkState(TimeUtils.timeValueInValidRange(upperMillis), "Invalid upper bound in millis: %s",
              upperMillis);
          break;
        }
        default:
          throw new IllegalStateException();
      }

      // Step 2: Convert millis range to input range
      String lowerValue = null;
      boolean lowerInclusive = false;
      if (lowerMillis != null) {
        // formatToMillis(col) >= millis
        // - if (formatToMillis(millisToFormat(millis)) == millis)
        //   -> col >= millisToFormat(millis)
        // - else (formatToMillis(millisToFormat(millis)) < millis)
        //   -> col > millisToFormat(millis)
        //
        // E.g.
        // secondsToMillis(seconds) >= 123
        // -> seconds > 0
        // secondsToMillis(seconds) >= 0
        // -> seconds >= 0
        lowerValue = inputFormat.fromMillisToFormat(lowerMillis);
        lowerInclusive = inputFormat.fromFormatToMillis(lowerValue) == lowerMillis;
      }
      String upperValue = null;
      boolean upperInclusive = false;
      if (upperMillis != null) {
        // formatToMillis(col) < millis
        // - if (formatToMillis(millisToFormat(millis)) == millis)
        //   -> col < millisToFormat(millis)
        // - else (formatToMillis(millisToFormat(millis)) < millis)
        //   -> col <= millisToFormat(millis)
        //
        // E.g.
        // secondsToMillis(seconds) < 123
        // -> seconds <= 0
        // secondsToMillis(seconds) < 0
        // -> seconds < 0
        upperValue = inputFormat.fromMillisToFormat(upperMillis);
        upperInclusive = inputFormat.fromFormatToMillis(upperValue) != upperMillis;
      }

      // Step 3: Rewrite the filter function
      String rangeString = new Range(lowerValue, lowerInclusive, upperValue, upperInclusive).getRangeString();
      rewriteToRange(filterFunction, dateTimeConvertOperands.get(0), rangeString);
    } catch (Exception e) {
      LOGGER.warn("Caught exception while optimizing DATE_TIME_CONVERT predicate: {}, skipping the optimization",
          filterFunction, e);
    }
  }

  private static void optimizeDateTrunc(Function filterFunction, FilterKind filterKind) {
    List<Expression> filterOperands = filterFunction.getOperands();
    List<Expression> dateTruncOperands = filterOperands.get(0).getFunctionCall().getOperands();

    long lowerMillis;
    long upperMillis;
    boolean lowerInclusive = true;
    boolean upperInclusive = true;
    String unit = dateTruncOperands.get(0).getLiteral().getStringValue();
    String inputTimeUnit = (dateTruncOperands.size() >= 3) ? dateTruncOperands.get(2).getLiteral().getStringValue()
        : TimeUnit.MILLISECONDS.name();
    if (dateTruncOperands.size() >= 4) {
      if (!dateTruncOperands.get(3).getLiteral().getStringValue().equals("UTC")) {
        // Leave query unoptimized if working with non-UTC time zones
        return;
      }
    }
    ISOChronology chronology = ISOChronology.getInstanceUTC();
    String outputTimeUnit = (dateTruncOperands.size() == 5) ? dateTruncOperands.get(4).getLiteral().getStringValue()
        : TimeUnit.MILLISECONDS.name();

    switch (filterKind) {
      case EQUALS: {
        long timeValue = getLongValue(filterOperands.get(1));
        upperMillis = dateTruncCeil(unit, timeValue, outputTimeUnit);
        lowerMillis = dateTruncFloor(timeValue, outputTimeUnit);
        // If the roundFloor of the lowerMillis does not equal lowerMillis, this implies that lowerMillis (the literal
        // comparative in the filter) is not aligned with the interval step of the unit. Therefore, the filter is
        // impossible and is converted to an impossible filter that is optimized away downstream
        if (lowerMillis != DateTimeUtils.getTimestampField(chronology, unit).roundFloor(lowerMillis)) {
          lowerMillis = Long.MAX_VALUE;
          upperMillis = Long.MIN_VALUE;
          String rangeString = new Range(lowerMillis, lowerInclusive, upperMillis, upperInclusive).getRangeString();
          rewriteToRange(filterFunction, dateTruncOperands.get(1), rangeString);
          return;
        }
        break;
      }
      case GREATER_THAN: {
        long timeValue = getLongValue(filterOperands.get(1));
        lowerMillis = dateTruncCeil(unit, timeValue, outputTimeUnit);
        lowerInclusive = false;
        upperMillis = Long.MAX_VALUE;
        break;
      }
      case GREATER_THAN_OR_EQUAL: {
        long timeValue = getLongValue(filterOperands.get(1));
        lowerMillis = dateTruncFloor(timeValue, outputTimeUnit);
        upperMillis = Long.MAX_VALUE;
        // If the roundFloor of the lowerMillis does not equal lowerMillis, this implies that lowerMillis (the literal
        // comparative in the filter) is not aligned with the interval step of the unit. Therefore, the equality portion
        // of the filter is impossible and is converted to a regular greater than filter
        if (lowerMillis != DateTimeUtils.getTimestampField(chronology, unit).roundFloor(lowerMillis)) {
          lowerInclusive = false;
          lowerMillis = dateTruncCeil(unit, timeValue, outputTimeUnit);
        }
        break;
      }
      case LESS_THAN: {
        long timeValue = getLongValue(filterOperands.get(1));
        lowerMillis = Long.MIN_VALUE;
        upperInclusive = false;
        upperMillis = dateTruncFloor(timeValue, outputTimeUnit);
        // If the roundFloor of the upperMillis does not equal upperMillis, this implies that upperMillis (the literal
        // comparative in the filter) is not aligned with the interval step of the unit. Therefore, the maximum value
        // that will truncate to a value lower than the literal will be equal to the ceiling of the inverse
        if (upperMillis != DateTimeUtils.getTimestampField(chronology, unit).roundFloor(upperMillis)) {
          upperInclusive = true;
          upperMillis = dateTruncCeil(unit, timeValue, outputTimeUnit);
        }
        break;
      }
      case LESS_THAN_OR_EQUAL: {
        long timeValue = getLongValue(filterOperands.get(1));
        lowerMillis = Long.MIN_VALUE;
        upperMillis = dateTruncCeil(unit, timeValue, outputTimeUnit);
        break;
      }
      case BETWEEN: {
        long timeValue = getLongValue(filterOperands.get(1));
        lowerMillis = dateTruncFloor(timeValue, outputTimeUnit);
        // If the roundFloor of the lowerMillis does not equal lowerMillis, this implies that lowerMillis (the literal
        // comparative in the filter) is not aligned with the interval step of the unit. Therefore, the lowerMillis must
        // be equal to the ceiling (non-inclusive) of the inverse
        if (TimeUnit.valueOf(outputTimeUnit).convert(lowerMillis, TimeUnit.MILLISECONDS)
            != getLongValue(filterOperands.get(1))) {
          lowerInclusive = false;
          lowerMillis = dateTruncCeil(unit, timeValue, outputTimeUnit);
        }
        timeValue = getLongValue(filterOperands.get(2));
        upperMillis = dateTruncCeil(unit, timeValue, outputTimeUnit);
        break;
      }
      default:
        throw new IllegalStateException();
    }

    // Convert values back into columnar unit
    lowerMillis = TimeUnit.valueOf(inputTimeUnit).convert(lowerMillis, TimeUnit.MILLISECONDS);
    upperMillis = TimeUnit.valueOf(inputTimeUnit).convert(upperMillis, TimeUnit.MILLISECONDS);
    String rangeString = new Range(lowerMillis, lowerInclusive, upperMillis, upperInclusive).getRangeString();
    rewriteToRange(filterFunction, dateTruncOperands.get(1), rangeString);
  }

  private static boolean isStringLiteral(Expression expression) {
    Literal literal = expression.getLiteral();
    return literal != null && literal.isSetStringValue();
  }

  private static long getLongValue(Expression expression) {
    Literal literal = expression.getLiteral();
    Preconditions.checkArgument(literal != null, "Got non-literal expression: %s", expression);
    switch (literal.getSetField()) {
      case INT_VALUE:
        return literal.getIntValue();
      case LONG_VALUE:
        return literal.getLongValue();
      case STRING_VALUE:
        return Long.parseLong(literal.getStringValue());
      default:
        throw new IllegalStateException("Unsupported literal type: " + literal.getSetField() + " as long value");
    }
  }

  /**
   * Helper method to round up the given value based on the granularity.
   */
  private static long ceil(long millisValue, long granularityMillis) {
    return (millisValue + granularityMillis - 1) / granularityMillis * granularityMillis;
  }

  private static void rewriteToRange(Function filterFunction, Expression expression, String rangeString) {
    filterFunction.setOperator(FilterKind.RANGE.name());
    // NOTE: Create an ArrayList because we might need to modify the list later
    List<Expression> newOperands = new ArrayList<>(2);
    newOperands.add(expression);
    newOperands.add(RequestUtils.getLiteralExpression(rangeString));
    filterFunction.setOperands(newOperands);
  }

  /**
   * Helper function to find the floor of acceptable values truncating to a specified value
   * Computes floor inverse of date trunc function
   */
  private static long dateTruncFloor(long timeValue, String outputTimeUnit) {
    return TimeUnit.MILLISECONDS.convert(timeValue, TimeUnit.valueOf(outputTimeUnit.toUpperCase()));
  }

  /**
   * Helper function that finds the maximum value (ceiling) that truncates to specified value
   * Computes ceiling inverse of date trunc function
   */
  private static long dateTruncCeil(String unit, long timeValue, String outputTimeUnit) {
    long floorMillis = dateTruncFloor(timeValue, outputTimeUnit);
    return DateTimeUtils.getTimestampField(ISOChronology.getInstanceUTC(), unit).roundFloor(floorMillis)
        + DateTimeUtils.getTimestampField(ISOChronology.getInstanceUTC(), unit).roundCeiling(1) - 1;
  }
}
