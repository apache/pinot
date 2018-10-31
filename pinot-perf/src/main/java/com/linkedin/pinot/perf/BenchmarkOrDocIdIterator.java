/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.perf;

import com.linkedin.pinot.core.common.BlockDocIdIterator;
import com.linkedin.pinot.core.common.Constants;
import com.linkedin.pinot.core.operator.dociditerators.OrDocIdIterator;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;


@State(Scope.Benchmark)
public class BenchmarkOrDocIdIterator {
  private static final int MAX_DOC_ID = 100000;

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public int arrayBased2() {
    OrDocIdIterator iterator = setUpArrayBased(2);
    int ret = 0;
    int docId;
    while ((docId = iterator.next()) != Constants.EOF) {
      ret += docId;
    }
    return ret;
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public int arrayBased5() {
    OrDocIdIterator iterator = setUpArrayBased(5);
    int ret = 0;
    int docId;
    while ((docId = iterator.next()) != Constants.EOF) {
      ret += docId;
    }
    return ret;
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public int arrayBased10() {
    OrDocIdIterator iterator = setUpArrayBased(10);
    int ret = 0;
    int docId;
    while ((docId = iterator.next()) != Constants.EOF) {
      ret += docId;
    }
    return ret;
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public int arrayBased20() {
    OrDocIdIterator iterator = setUpArrayBased(20);
    int ret = 0;
    int docId;
    while ((docId = iterator.next()) != Constants.EOF) {
      ret += docId;
    }
    return ret;
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public int arrayBased50() {
    OrDocIdIterator iterator = setUpArrayBased(50);
    int ret = 0;
    int docId;
    while ((docId = iterator.next()) != Constants.EOF) {
      ret += docId;
    }
    return ret;
  }

  private OrDocIdIterator setUpArrayBased(int numIterators) {
    BlockDocIdIterator[] iterators = new BlockDocIdIterator[numIterators];
    for (int i = 0; i < numIterators; i++) {
      iterators[i] = new FixedStepsDocIdIterator(MAX_DOC_ID, i + 1);
    }
    return new OrDocIdIterator(iterators, 0, MAX_DOC_ID);
  }

  private class FixedStepsDocIdIterator implements BlockDocIdIterator {
    private final int _maxDocId;
    private final int _steps;

    private int _currentDocId = -1;

    public FixedStepsDocIdIterator(int maxDocId, int steps) {
      _maxDocId = maxDocId;
      _steps = steps;
    }

    @Override
    public int next() {
      if (_currentDocId == Constants.EOF) {
        return Constants.EOF;
      }
      int nextDocId = (_currentDocId + _steps) / _steps * _steps;
      if (nextDocId > _maxDocId) {
        _currentDocId = Constants.EOF;
      } else {
        _currentDocId = nextDocId;
      }
      return _currentDocId;
    }

    @Override
    public int advance(int targetDocId) {
      int nextDocId = (targetDocId + _steps - 1) / _steps * _steps;
      if (nextDocId > _maxDocId) {
        _currentDocId = Constants.EOF;
      } else {
        _currentDocId = nextDocId;
      }
      return _currentDocId;
    }

    @Override
    public int currentDocId() {
      return _currentDocId;
    }
  }

  public static void main(String[] args) throws Exception {
    Options opt = new OptionsBuilder().include(BenchmarkOrDocIdIterator.class.getSimpleName())
        .warmupTime(TimeValue.seconds(5))
        .warmupIterations(2)
        .measurementTime(TimeValue.seconds(5))
        .measurementIterations(3)
        .forks(1)
        .build();

    new Runner(opt).run();
  }
}
