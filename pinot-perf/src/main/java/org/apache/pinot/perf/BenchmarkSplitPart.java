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
package org.apache.pinot.perf;

import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.common.function.scalar.StringFunctions;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.OptionsBuilder;


@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(1)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 10, time = 1)
@State(Scope.Benchmark)
public class BenchmarkSplitPart {

  @Param({
      "small_index", "large_index",
      "negative_index", "large_negative_index",
      "adjacent_delimiters", "many_fields",
      "multi_char_delim", "large_multi_char_delim"
  })
  public String _scenario;

  private String _input;
  private String _delimiter;
  private int _index;

  @Setup
  public void setUp() {
    switch (_scenario) {
      case "small_index":
        // Extract 2nd element from 100-field string
        _delimiter = ",";
        _input = buildString(100, _delimiter);
        _index = 1;
        break;
      case "large_index":
        // Extract 80th element from 100-field string
        _delimiter = ",";
        _input = buildString(10000, _delimiter);
        _index = 790;
        break;
      case "negative_index":
        // Extract last element from 100-field string
        _delimiter = ",";
        _input = buildString(100, _delimiter);
        _index = -1;
        break;
      case "large_negative_index":
        // Extract last element from 100-field string
        _delimiter = ",";
        _input = buildString(10000, _delimiter);
        _index = -7242;
        break;
      case "adjacent_delimiters":
        // String with many consecutive delimiters
        _input = "field0+++field1+++field2+++field3";
        _delimiter = "+";
        _index = 1;
        break;
      case "many_fields":
        // Extract 5th element from 1000-field string
        _delimiter = ",";
        _input = buildString(10000, _delimiter);
        _index = 4;
        break;
      case "multi_char_delim":
        // Multi-character delimiter
        _delimiter = ":";
        _input = buildString(50, repeatStr(_delimiter, 10));
        _index = 10;
        break;
      case "large_multi_char_delim":
        // Multi-character delimiter
        _delimiter = repeatStr(":", 4242);
        _input = buildString(10000, _delimiter);
        _index = 10;
        break;
      default:
        throw new IllegalArgumentException("Unknown scenario: " + _scenario);
    }
  }

  private String repeatStr(String str, int length) {
    return String.valueOf(str).repeat(Math.max(0, length));
  }

  private String buildString(int fields, String delimiter) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < fields; i++) {
      if (i > 0) {
        sb.append(delimiter);
      }
      sb.append("field").append(i);
    }
    return sb.toString();
  }

  public static void main(String[] args)
      throws Exception {
    ChainedOptionsBuilder opt = new OptionsBuilder().include(BenchmarkSplitPart.class.getSimpleName());
    new Runner(opt.build()).run();
  }

  @Benchmark
  public String testSplitPartOld() {
    return splitPartOld(_input, _delimiter, _index);
  }

  @Benchmark
  public String testSplitPartNew() {
    return StringFunctions.splitPart(_input, _delimiter, _index);
  }

  /**
   * Original implementation that allocates full String array
   */
  public static String splitPartOld(String input, String delimiter, int index) {
    String[] splitString = StringUtils.splitByWholeSeparator(input, delimiter);
    if (index >= 0 && index < splitString.length) {
      return splitString[index];
    } else if (index < 0 && index >= -splitString.length) {
      return splitString[splitString.length + index];
    } else {
      return "null";
    }
  }
}
