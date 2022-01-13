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

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.pinot.segment.local.recordtransformer.DataTypeTransformer;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.CompilerControl;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;


@State(Scope.Benchmark)
public class BenchmarkDataTypeTransformer {

  @Param({"10", "20", "30", "40", "50"})
  int _columnCount;

  private GenericRow _genericRow;
  private DataTypeTransformer _dataTypeTransformer;

  @Setup(Level.Trial)
  public void setup() {
    _genericRow = new GenericRow();
    List<FieldSpec> fieldSpecs = fieldSpecs(_columnCount);
    _dataTypeTransformer = new DataTypeTransformer(fieldSpecs);
    for (FieldSpec fieldSpec : fieldSpecs) {
      switch (fieldSpec.getDataType()) {
        case STRING:
          _genericRow.putValue(fieldSpec.getName(), UUID.randomUUID().toString());
          break;
        case DOUBLE:
          _genericRow.putValue(fieldSpec.getName(), ThreadLocalRandom.current().nextDouble());
          break;
        default:
          throw new RuntimeException("not measuring type" + fieldSpec.getDataType());
      }
    }
  }

  @CompilerControl(CompilerControl.Mode.DONT_INLINE)
  List<FieldSpec> fieldSpecs(int size) {
    return IntStream.range(0, size)
        .mapToObj(i -> i % 2 == 0 ? new DimensionFieldSpec("column" + i, FieldSpec.DataType.STRING, true)
            : new DimensionFieldSpec("column" + i, FieldSpec.DataType.DOUBLE, true))
        .collect(Collectors.toList());
  }

  @CompilerControl(CompilerControl.Mode.DONT_INLINE)
  @Benchmark
  public GenericRow transform() {
    return _dataTypeTransformer.transform(_genericRow);
  }
}
