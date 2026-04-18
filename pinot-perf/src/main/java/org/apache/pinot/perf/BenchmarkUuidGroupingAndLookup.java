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

import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.query.runtime.operator.groupby.OneObjectKeyGroupIdGenerator;
import org.apache.pinot.query.runtime.operator.groupby.OneUuidKeyGroupIdGenerator;
import org.apache.pinot.query.runtime.operator.join.ObjectLookupTable;
import org.apache.pinot.query.runtime.operator.join.UuidLookupTable;
import org.apache.pinot.spi.utils.ByteArray;
import org.apache.pinot.spi.utils.UuidUtils;
import org.apache.pinot.spi.utils.UuidUtils.UuidKey;
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
import org.openjdk.jmh.runner.options.OptionsBuilder;


/**
 * Benchmarks UUID grouping and lookup hot paths using Pinot's current {@link ByteArray}-based representation versus a
 * two-long UUID key representation and {@link UUID}.
 *
 * <p>The benchmark is intentionally engine-adjacent:
 * <ul>
 *   <li>V1 grouping uses the same {@code Object2IntOpenHashMap<ByteArray>} shape as no-dictionary UUID group-by</li>
 *   <li>V2 grouping uses {@link OneUuidKeyGroupIdGenerator}</li>
 *   <li>Lookup uses {@link UuidLookupTable}, which is the UUID-specific path used by MSE hash join</li>
 * </ul>
 */
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(1)
@Warmup(iterations = 3, time = 5)
@Measurement(iterations = 5, time = 5)
@State(Scope.Benchmark)
public class BenchmarkUuidGroupingAndLookup {
  private static final long RANDOM_SEED = 677_280_899_123L;

  @Param({"262144"})
  public int _numRows;

  @Param({"65536"})
  public int _cardinality;

  @Param({"8192"})
  public int _numProbes;

  private byte[][] _rowUuidBytes;
  private ByteArray[] _rowByteArrays;
  private UuidKey[] _rowUuidKeys;
  private UUID[] _rowJavaUuids;
  private ByteArray[] _distinctByteArrays;
  private UuidKey[] _distinctUuidKeys;
  private UUID[] _distinctJavaUuids;
  private Object[][] _distinctByteArrayRows;
  private Object[][] _distinctJavaUuidRows;
  private ByteArray[] _probeByteArrays;
  private UUID[] _probeJavaUuids;

  @Setup
  public void setUp() {
    Random random = new Random(RANDOM_SEED);

    byte[][] distinctUuidBytes = new byte[_cardinality][];
    _distinctByteArrays = new ByteArray[_cardinality];
    _distinctUuidKeys = new UuidKey[_cardinality];
    _distinctJavaUuids = new UUID[_cardinality];
    _distinctByteArrayRows = new Object[_cardinality][];
    _distinctJavaUuidRows = new Object[_cardinality][];

    for (int i = 0; i < _cardinality; i++) {
      UUID uuid = new UUID(random.nextLong(), random.nextLong());
      byte[] uuidBytes = UuidUtils.toBytes(uuid);

      distinctUuidBytes[i] = uuidBytes;
      _distinctByteArrays[i] = new ByteArray(uuidBytes);
      _distinctUuidKeys[i] = UuidKey.fromLongs(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
      _distinctJavaUuids[i] = uuid;
      _distinctByteArrayRows[i] = new Object[]{i};
      _distinctJavaUuidRows[i] = new Object[]{i};
    }

    _rowUuidBytes = new byte[_numRows][];
    _rowByteArrays = new ByteArray[_numRows];
    _rowUuidKeys = new UuidKey[_numRows];
    _rowJavaUuids = new UUID[_numRows];

    for (int i = 0; i < _numRows; i++) {
      int dictId = random.nextInt(_cardinality);
      _rowUuidBytes[i] = distinctUuidBytes[dictId];
      _rowByteArrays[i] = _distinctByteArrays[dictId];
      _rowUuidKeys[i] = _distinctUuidKeys[dictId];
      _rowJavaUuids[i] = _distinctJavaUuids[dictId];
    }

    _probeByteArrays = new ByteArray[_numProbes];
    _probeJavaUuids = new UUID[_numProbes];
    for (int i = 0; i < _numProbes; i++) {
      int dictId = random.nextInt(_cardinality);
      _probeByteArrays[i] = _distinctByteArrays[dictId];
      _probeJavaUuids[i] = _distinctJavaUuids[dictId];
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  public int v1CurrentByteArrayGrouping() {
    Object2IntOpenHashMap<ByteArray> groupIdMap = new Object2IntOpenHashMap<>(_cardinality);
    groupIdMap.defaultReturnValue(-1);

    int checksum = 0;
    for (int i = 0; i < _numRows; i++) {
      checksum += getOrCreateGroupId(groupIdMap, new ByteArray(_rowUuidBytes[i]));
    }
    return checksum + groupIdMap.size();
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  public int v1UuidKeyGrouping() {
    Object2IntOpenHashMap<UuidKey> groupIdMap = new Object2IntOpenHashMap<>(_cardinality);
    groupIdMap.defaultReturnValue(-1);

    int checksum = 0;
    for (int i = 0; i < _numRows; i++) {
      checksum += getOrCreateGroupId(groupIdMap, _rowUuidKeys[i]);
    }
    return checksum + groupIdMap.size();
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  public int v1JavaUuidGrouping() {
    Object2IntOpenHashMap<UUID> groupIdMap = new Object2IntOpenHashMap<>(_cardinality);
    groupIdMap.defaultReturnValue(-1);

    int checksum = 0;
    for (int i = 0; i < _numRows; i++) {
      checksum += getOrCreateGroupId(groupIdMap, _rowJavaUuids[i]);
    }
    return checksum + groupIdMap.size();
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  public int v2CurrentByteArrayGroupIdGenerator() {
    OneObjectKeyGroupIdGenerator groupIdGenerator = new OneObjectKeyGroupIdGenerator(_cardinality, _cardinality);

    int checksum = 0;
    for (ByteArray rowByteArray : _rowByteArrays) {
      checksum += groupIdGenerator.getGroupId(rowByteArray);
    }
    return checksum + groupIdGenerator.getNumGroups();
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  public int v2UuidKeyGroupIdGenerator() {
    OneUuidKeyGroupIdGenerator groupIdGenerator = new OneUuidKeyGroupIdGenerator(_cardinality, _cardinality);

    int checksum = 0;
    for (UuidKey rowUuidKey : _rowUuidKeys) {
      checksum += groupIdGenerator.getGroupId(rowUuidKey);
    }
    return checksum + groupIdGenerator.getNumGroups();
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  public int v2JavaUuidGroupIdGenerator() {
    OneObjectKeyGroupIdGenerator groupIdGenerator = new OneObjectKeyGroupIdGenerator(_cardinality, _cardinality);

    int checksum = 0;
    for (UUID rowJavaUuid : _rowJavaUuids) {
      checksum += groupIdGenerator.getGroupId(rowJavaUuid);
    }
    return checksum + groupIdGenerator.getNumGroups();
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  public int currentObjectLookupTableBuildAndProbe() {
    ObjectLookupTable lookupTable = new ObjectLookupTable();
    for (int i = 0; i < _cardinality; i++) {
      lookupTable.addRow(_distinctByteArrays[i], _distinctByteArrayRows[i]);
    }
    lookupTable.finish();

    int checksum = lookupTable.size();
    for (ByteArray probeByteArray : _probeByteArrays) {
      Object[] row = (Object[]) lookupTable.lookup(probeByteArray);
      if (row != null) {
        checksum += (int) row[0];
      }
    }
    return checksum;
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  public int uuidLookupTableBuildAndProbe() {
    UuidLookupTable lookupTable = new UuidLookupTable();
    for (int i = 0; i < _cardinality; i++) {
      lookupTable.addRow(_distinctByteArrays[i], _distinctByteArrayRows[i]);
    }
    lookupTable.finish();

    int checksum = lookupTable.size();
    for (ByteArray probeByteArray : _probeByteArrays) {
      Object[] row = (Object[]) lookupTable.lookup(probeByteArray);
      if (row != null) {
        checksum += (int) row[0];
      }
    }
    return checksum;
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  public int javaUuidObjectLookupTableBuildAndProbe() {
    ObjectLookupTable lookupTable = new ObjectLookupTable();
    for (int i = 0; i < _cardinality; i++) {
      lookupTable.addRow(_distinctJavaUuids[i], _distinctJavaUuidRows[i]);
    }
    lookupTable.finish();

    int checksum = lookupTable.size();
    for (UUID probeJavaUuid : _probeJavaUuids) {
      Object[] row = (Object[]) lookupTable.lookup(probeJavaUuid);
      if (row != null) {
        checksum += (int) row[0];
      }
    }
    return checksum;
  }

  private <T> int getOrCreateGroupId(Object2IntOpenHashMap<T> groupIdMap, T key) {
    int numGroups = groupIdMap.size();
    if (numGroups < _cardinality) {
      return groupIdMap.computeIfAbsent(key, ignored -> numGroups);
    }
    return groupIdMap.getInt(key);
  }

  public static void main(String[] args)
      throws Exception {
    new Runner(new OptionsBuilder().include(BenchmarkUuidGroupingAndLookup.class.getSimpleName()).build()).run();
  }
}
