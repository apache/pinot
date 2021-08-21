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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.segment.local.segment.creator.impl.inv.OffHeapBitmapInvertedIndexCreator;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.OptionsBuilder;


@State(Scope.Benchmark)
public class BenchmarkOffheapBitmapInvertedIndexCreator {

  public enum Assignment {
    ROUND_ROBIN {
      @Override
      void assign(OffHeapBitmapInvertedIndexCreator creator, int docs, int cardinality) {
        for (int i = 0; i < docs; ++i) {
          creator.add(i % cardinality);
        }
      }
    }, SORTED_UNIFORM {
      @Override
      void assign(OffHeapBitmapInvertedIndexCreator creator, int docs, int cardinality) {
        for (int i = 0; i < cardinality; ++i) {
          for (int j = 0; j < docs / cardinality; ++j) {
            creator.add(i);
          }
        }
      }
    };

    abstract void assign(OffHeapBitmapInvertedIndexCreator creator, int docs, int cardinality);
  }

  private Path _indexDir;
  @Param({"10", "1000", "10000"})
  int _cardinality;

  @Param({"1000000", "10000000", "100000000"})
  int _numDocs;

  @Param
  Assignment _assignment;

  private OffHeapBitmapInvertedIndexCreator _creator;

  @Setup(Level.Invocation)
  public void setup()
      throws IOException {
    _indexDir = Files.createTempDirectory("index");
    _creator = new OffHeapBitmapInvertedIndexCreator(
        _indexDir.toFile(), new DimensionFieldSpec("foo", FieldSpec.DataType.STRING, true),
        _cardinality, _numDocs, -1);
    _assignment.assign(_creator, _numDocs, _cardinality);
  }

  @TearDown(Level.Invocation)
  public void tearDown()
      throws IOException {
    if (null != _indexDir) {
      FileUtils.deleteDirectory(_indexDir.toFile());
    }
    _creator.close();
  }

  @Benchmark
  public Object seal()
      throws IOException {
    _creator.seal();
    return _creator;
  }

  public static void main(String[] args)
      throws Exception {
    ChainedOptionsBuilder opt =
        new OptionsBuilder().include(BenchmarkOffheapBitmapInvertedIndexCreator.class.getSimpleName())
            .mode(Mode.SingleShotTime).warmupIterations(8).measurementIterations(8).forks(5);

    new Runner(opt.build()).run();
  }
}
