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
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import org.apache.pinot.segment.local.utils.nativefst.FST;
import org.apache.pinot.segment.local.utils.nativefst.builders.FSTBuilder;
import org.apache.pinot.segment.local.utils.nativefst.utils.RegexpMatcher;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;


/**
 * This benchmark uses COCACorpus which constitutes of 51 million words and 1.5 million unique
 * words. The benchmark runs a set of queries on Lucene FST and native FST and publishes numbers.
 */
public class FSTBenchmarkTest {

  public static void main(String[] args)
      throws Exception {
    org.openjdk.jmh.Main.main(args);
  }

  @Benchmark
  @Fork(value = 1)
  @Warmup(iterations = 2)
  @Measurement(iterations = 5)
  @BenchmarkMode(Mode.AverageTime)
  public void testNativeRegex1(FSTStore fstStore, Blackhole blackhole) {
    regexQueryNrHits("q.[aeiou]c.*", FSTBenchmarkTest.FSTStore._nativeFST, blackhole);
  }

  @Benchmark
  @Fork(value = 1)
  @Warmup(iterations = 2)
  @Measurement(iterations = 5)
  @BenchmarkMode(Mode.AverageTime)
  public void testNativeRegex2(FSTStore fstStore, Blackhole blackhole) {
    regexQueryNrHits(".*a", FSTBenchmarkTest.FSTStore._nativeFST, blackhole);
  }

  @Benchmark
  @Fork(value = 1)
  @Warmup(iterations = 2)
  @Measurement(iterations = 5)
  @BenchmarkMode(Mode.AverageTime)
  public void testNativeRegex3(FSTStore fstStore, Blackhole blackhole) {
    regexQueryNrHits("b.*", FSTBenchmarkTest.FSTStore._nativeFST, blackhole);
  }

  @Benchmark
  @Fork(value = 1)
  @Warmup(iterations = 2)
  @Measurement(iterations = 5)
  @BenchmarkMode(Mode.AverageTime)
  public void testNativeRegex4(FSTStore fstStore, Blackhole blackhole) {
    regexQueryNrHits(".*", FSTBenchmarkTest.FSTStore._nativeFST, blackhole);
  }

  @Benchmark
  @Fork(value = 1)
  @Warmup(iterations = 2)
  @Measurement(iterations = 5)
  @BenchmarkMode(Mode.AverageTime)
  public void testNativeRegex5(FSTStore fstStore, Blackhole blackhole) {
    regexQueryNrHits(".*ated", FSTBenchmarkTest.FSTStore._nativeFST, blackhole);
  }

  @Benchmark
  @Fork(value = 1)
  @Warmup(iterations = 2)
  @Measurement(iterations = 5)
  @BenchmarkMode(Mode.AverageTime)
  public void testNativeRegex6(FSTStore fstStore, Blackhole blackhole) {
    regexQueryNrHits(".*ba.*", FSTBenchmarkTest.FSTStore._nativeFST, blackhole);
  }

  @Benchmark
  @Fork(value = 1)
  @Warmup(iterations = 2)
  @Measurement(iterations = 5)
  @BenchmarkMode(Mode.AverageTime)
  public void testLuceneRegex1(FSTStore fstStore, Blackhole blackhole) {
    regexQueryNrHits("q.[aeiou]c.*", FSTBenchmarkTest.FSTStore._fst, blackhole);
  }

  @Benchmark
  @Fork(value = 1)
  @Warmup(iterations = 2)
  @Measurement(iterations = 5)
  @BenchmarkMode(Mode.AverageTime)
  public void testLuceneRegex2(FSTStore fstStore, Blackhole blackhole) {
    regexQueryNrHits(".*a", FSTBenchmarkTest.FSTStore._fst, blackhole);
  }

  @Benchmark
  @Fork(value = 1)
  @Warmup(iterations = 2)
  @Measurement(iterations = 5)
  @BenchmarkMode(Mode.AverageTime)
  public void testLuceneRegex3(FSTStore fstStore, Blackhole blackhole) {
    regexQueryNrHits("b.*", FSTBenchmarkTest.FSTStore._fst, blackhole);
  }

  @Benchmark
  @Fork(value = 1)
  @Warmup(iterations = 2)
  @Measurement(iterations = 5)
  @BenchmarkMode(Mode.AverageTime)
  public void testLuceneRegex4(FSTStore fstStore, Blackhole blackhole) {
    regexQueryNrHits(".*", FSTBenchmarkTest.FSTStore._fst, blackhole);
  }

  @Benchmark
  @Fork(value = 1)
  @Warmup(iterations = 2)
  @Measurement(iterations = 5)
  @BenchmarkMode(Mode.AverageTime)
  public void testLuceneRegex5(FSTStore fstStore, Blackhole blackhole) {
    regexQueryNrHits(".*ated", FSTBenchmarkTest.FSTStore._fst, blackhole);
  }

  @Benchmark
  @Fork(value = 1)
  @Warmup(iterations = 2)
  @Measurement(iterations = 5)
  @BenchmarkMode(Mode.AverageTime)
  public void testLuceneRegex6(FSTStore fstStore, Blackhole blackhole) {
    regexQueryNrHits(".*ba.*", FSTBenchmarkTest.FSTStore._fst, blackhole);
  }

  private void regexQueryNrHits(String regex, FST fst, Blackhole blackhole) {
    List<Long> resultList = RegexpMatcher.regexMatch(regex, fst);

    blackhole.consume(resultList);
  }

  private void regexQueryNrHits(String regex, org.apache.lucene.util.fst.FST fst, Blackhole blackhole) {
    try {
      List<Long> resultList = org.apache.pinot.segment.local.utils.fst.RegexpMatcher.regexMatch(regex, fst);

      blackhole.consume(resultList);
    } catch (IOException e) {
      throw new RuntimeException(e.getMessage());
    }
  }

  @State(Scope.Benchmark)
  public static class FSTStore {
    public static FST _nativeFST;
    public static org.apache.lucene.util.fst.FST _fst;
    public static boolean _initialized;

    public FSTStore() {

      if (_initialized) {
        return;
      }

      SortedMap<String, Integer> inputStrings = new TreeMap<>();
      InputStream fileInputStream;
      InputStreamReader inputStreamReader;
      BufferedReader bufferedReader;

      try {
        File file = new File("pinot-perf/src/main/resources/words.txt");

        fileInputStream = new FileInputStream(file);
        inputStreamReader = new InputStreamReader(fileInputStream, StandardCharsets.UTF_8);
        bufferedReader = new BufferedReader(inputStreamReader);

        String currentWord;
        int i = 0;
        while ((currentWord = bufferedReader.readLine()) != null) {
          inputStrings.put(currentWord, i);
          i++;
        }

        _nativeFST = FSTBuilder.buildFST(inputStrings);
        _fst = org.apache.pinot.segment.local.utils.fst.FSTBuilder.buildFST(inputStrings);

        _initialized = true;
      } catch (IOException e) {
        throw new RuntimeException(e.getMessage());
      }
    }
  }
}
