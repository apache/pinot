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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.pinot.common.function.scalar.regexp.RegexpReplaceConstFunctions;
import org.apache.pinot.spi.annotations.ScalarFunction;
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
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.OptionsBuilder;


@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(1)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 3, time = 1)
@State(Scope.Benchmark)
public class BenchmarkRegexpReplace {

  private ArrayList<String> _input = new ArrayList<>();

  @Param({"q.[aeiou]c.*", ".*a", "b.*", ".*", ".*ated", ".*ba.*"})
  public String _regex;

  @Setup
  public void setUp()
      throws IOException {
    try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(
        Objects.requireNonNull(getClass().getClassLoader().getResourceAsStream("data/words.txt"))))) {
      String currentWord;
      while ((currentWord = bufferedReader.readLine()) != null) {
        _input.add(currentWord);
      }
    }
  }

  public static void main(String[] args)
      throws Exception {
    ChainedOptionsBuilder opt = new OptionsBuilder().include(BenchmarkRegexpReplace.class.getSimpleName());
    new Runner(opt.build()).run();
  }

  @Benchmark
  public void testRegexpReplaceOld(Blackhole blackhole) {
    for (int i = 0, n = _input.size(); i < n; i++) {
      blackhole.consume(regexpReplaceOld(_input.get(i), _regex, ""));
    }
  }

  @Benchmark
  public void testRegexpReplaceConst(Blackhole blackhole) {
    RegexpReplaceConstFunctions function = new RegexpReplaceConstFunctions();
    for (int i = 0, n = _input.size(); i < n; i++) {
      blackhole.consume(function.regexpReplaceConst(_input.get(i), _regex, ""));
    }
  }

  //old regexp_replace implementation
  public static String regexpReplaceOld(String inputStr, String matchStr, String replaceStr, int matchStartPos,
      int occurence, String flag) {
    Integer patternFlag;

    switch (flag) {
      case "i":
        patternFlag = Pattern.CASE_INSENSITIVE;
        break;
      default:
        patternFlag = null;
        break;
    }

    Pattern p;
    if (patternFlag != null) {
      p = Pattern.compile(matchStr, patternFlag);
    } else {
      p = Pattern.compile(matchStr);
    }

    Matcher matcher = p.matcher(inputStr).region(matchStartPos, inputStr.length());
    StringBuffer sb;

    if (occurence >= 0) {
      sb = new StringBuffer(inputStr);
      while (occurence >= 0 && matcher.find()) {
        if (occurence == 0) {
          sb.replace(matcher.start(), matcher.end(), replaceStr);
          break;
        }
        occurence--;
      }
    } else {
      sb = new StringBuffer();
      while (matcher.find()) {
        matcher.appendReplacement(sb, replaceStr);
      }
      matcher.appendTail(sb);
    }

    return sb.toString();
  }

  @ScalarFunction
  public static String regexpReplaceOld(String inputStr, String matchStr, String replaceStr) {
    return regexpReplaceOld(inputStr, matchStr, replaceStr, 0, -1, "");
  }
}
