/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.thirdeye.rootcause.util;

import java.util.concurrent.TimeUnit;
import org.joda.time.Period;


/**
 * Utility for scoring entities
 */
public class ScoreUtils {
  public enum StrategyType {
    LINEAR,
    TRIANGULAR,
    QUADRATIC,
    HYPERBOLA
  }

  private ScoreUtils() {
    // left blank
  }

  /**
   * Converts a string representation of a period into millis.
   * <br/><b>NOTE:</b> parses weeks, days, hours, minutes, and seconds - e.g.
   * {@code "1w 2d 3h 45m 6s"}.
   *
   * @param period period string
   * @return period in millis
   */
  public static long parsePeriod(String period) {
    // NOTE: JodaTime Parser/Formatter does not support optional sections

    Period p = Period.ZERO;

    final String[] parts = period.split("\\s+");
    if (parts.length == 1) {
      try {
        return Long.parseLong(parts[0]);
      } catch(NumberFormatException ignore) {
        // ignore, parse individual parts
      }
    }

    for (String part : parts) {
      final String prefix = part.substring(0, part.length() - 1);
      if (part.endsWith("w")) {
        p = p.withWeeks(Integer.parseInt(prefix));
      } else if(part.endsWith("d")) {
        p = p.withDays(Integer.parseInt(prefix));
      } else if(part.endsWith("h")) {
        p = p.withHours(Integer.parseInt(prefix));
      } else if(part.endsWith("m")) {
        p = p.withMinutes(Integer.parseInt(prefix));
      } else if(part.endsWith("s")) {
        p = p.withSeconds(Integer.parseInt(prefix));
      } else {
        throw new IllegalArgumentException(String.format("Invalid token '%s'", part));
      }
    }

    return p.toStandardSeconds().getSeconds() * 1000L;
  }

  /**
   * Converts a string representation of a scoring strategy to a {@code StrategyType}.
   *
   * @param strategyType strategy type
   * @return StrategyType
   */
  public static StrategyType parseStrategy(String strategyType) {
    return StrategyType.valueOf(strategyType);
  }

  /**
   * Returns a scorer based on the string name representation and arguments.
   *
   * @param type score type
   * @param lookback lookback in millis
   * @param start window start in millis
   * @param end window end in millis
   * @return scorer instance
   */
  public static TimeRangeStrategy build(StrategyType type, long lookback, long start, long end) {
    switch(type) {
      case LINEAR:
        return new LinearStartTimeStrategy(start, end);
      case TRIANGULAR:
        return new TriangularStartTimeStrategy(lookback, start, end);
      case QUADRATIC:
        return new QuadraticTriangularStartTimeStrategy(lookback, start, end);
      case HYPERBOLA:
        return new HyperbolaStrategy(start, end);
      default:
        throw new IllegalArgumentException(String.format("Unknown score type '%s'", type));
    }
  }

  /**
   * Determines a score between {@code [0.0, 1.0]} based on an entity's time range, described
   * by start and end timestamps.
   */
  public interface TimeRangeStrategy {

    /**
     * Returns an entity's score based on start and end timestamps.
     *
     * @param start start time in millis
     * @param end end time in millis
     * @return score between {@code [0.0, 1.0]}
     */
    double score(long start, long end);
  }

  /**
   * Determines a score between {@code [0.0, 1.0]} based on the entity's start time. The closer
   * an entity's start time is to the start time of the interval, the higher the score. If the
   * entity's start time lies outside the window, the score is zero.
   */
  public static final class LinearStartTimeStrategy implements TimeRangeStrategy {
    private final long start;
    private final long duration;

    public LinearStartTimeStrategy(long start, long end) {
      if (start > end)
        throw new IllegalArgumentException("Requires start <= end");
      this.start = start;
      this.duration = end - start;
    }

    @Override
    public double score(long start, long end) {
      if (start < this.start)
        return 0;
      long offset = start - this.start;
      return Math.min(Math.max(1.0d - offset / (double) this.duration, 0), 1.0);
    }
  }

  /**
   * Determines a score between {@code [0.0, 1.0]} based on the entity's start time. The closer
   * an entity's start time is to the start time of the interval, the higher the score. This
   * scorer allows for a lookback period before the start of the actual interval and provides
   * an increasing score between {@code lookback} an {@code start}, and a decreasing score
   * between {@code start} and {@code end}.
   */
  public static final class TriangularStartTimeStrategy implements TimeRangeStrategy {
    private final long lookback;
    private final long start;
    private final long end;

    public TriangularStartTimeStrategy(long lookback, long start, long end) {
      if (lookback > start)
        throw new IllegalArgumentException("Requires lookback <= start");
      if (start > end)
        throw new IllegalArgumentException("Requires start <= end");

      this.lookback = lookback;
      this.start = start;
      this.end = end;
    }

    @Override
    public double score(long start, long end) {
      if (start < this.lookback)
        return 0;
      if (start >= this.end)
        return 0;

      if (start < this.start) {
        // in lookback
        long duration = this.start - this.lookback;
        long offset = start - this.lookback;
        return offset / (double) duration;
      }

      // in time range
      long duration = this.end - this.start;
      long offset = start - this.start;
      return 1.0 - offset / (double) duration;
    }
  }

  /**
   * Determines a score between {@code [0.0, 1.0]} based on the entity's start time. Similar to
   * {@code TriangularStartTimeStrategy} in function, squaring the result score.
   *
   * @see TriangularStartTimeStrategy
   */
  public static final class QuadraticTriangularStartTimeStrategy implements TimeRangeStrategy {
    private final TriangularStartTimeStrategy delegate;

    public QuadraticTriangularStartTimeStrategy(long lookback, long start, long end) {
      this.delegate = new TriangularStartTimeStrategy(lookback, start, end);
    }

    @Override
    public double score(long start, long end) {
      return Math.pow(this.delegate.score(start, end), 2);
    }
  }

  /**
   * Determines a score between {@code [0.0, 1.0]} based on the entity's start time. The score
   * is proportional to the absolute distance from the anomaly region start and truncated to
   * {@code 0.0} after the midpoint of this region.
   */
  public static final class HyperbolaStrategy implements TimeRangeStrategy {
    private static final double COEFFICIENT = 1.0d / TimeUnit.HOURS.toMillis(1);

    private final long start;
    private final long end;

    public HyperbolaStrategy(long start, long end) {
      this.start = start;
      this.end = end;
    }

    @Override
    public double score(long start, long end) {
      if (start >= (this.start + this.end) / 2)
        return 0;
      return 1.0d / (COEFFICIENT * Math.abs(start - this.start) + 1.0);
    }
  }
}
