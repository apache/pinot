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

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;


/// Isolates the per-selected-segment instance-id map lookup in broker routing. Each invocation performs
/// `_selectedSegments` lookups, with no Pinot query execution, routing selection, or metadata updates.
/// Each worker owns its arrays and map; canonical server-id strings are immutable and shared through interning.
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 4, time = 1)
@Fork(2)
@State(Scope.Thread)
public class BenchmarkRoutingInstanceIdLookup {
  @Param({"100", "10000"})
  public int _selectedSegments;
  @Param({"100"})
  public int _servers;
  @Param({"baseline", "instance_config_intern"})
  public String _variant;

  private Map<String, Integer> _enabledServerInstanceMap;
  private String[] _candidateInstanceIds;

  @Setup(Level.Trial)
  public void setup() {
    if (_selectedSegments <= 0 || _servers <= 0) {
      throw new IllegalArgumentException("_selectedSegments and _servers must be positive");
    }
    boolean internConfigIds;
    if (_variant.equals("baseline")) {
      internConfigIds = false;
    } else if (_variant.equals("instance_config_intern")) {
      internConfigIds = true;
    } else {
      throw new IllegalArgumentException("Unknown _variant: " + _variant);
    }

    _enabledServerInstanceMap = new ConcurrentHashMap<>();
    String[] canonicalInstanceIds = new String[_servers];
    for (int i = 0; i < _servers; i++) {
      String canonicalId = String.format("Server_pinot-server-%04d.prod.example.com_8098", i).intern();
      canonicalInstanceIds[i] = canonicalId;
      // A separately decoded JSON string value has equal content but does not share the field-name object.
      String decodedConfigId = new String(canonicalId.getBytes(StandardCharsets.UTF_8), StandardCharsets.UTF_8);
      String storedId = internConfigIds ? decodedConfigId.intern() : decodedConfigId;
      if (!canonicalId.equals(storedId) || (canonicalId == storedId) != internConfigIds) {
        throw new IllegalStateException("Invalid server-id identity for _variant: " + _variant);
      }
      _enabledServerInstanceMap.put(storedId, i + 1);
      // Both variants model repeatedly queried routing state with already-computed string hashes.
      canonicalId.hashCode();
      if (_enabledServerInstanceMap.get(canonicalId) != i + 1) {
        throw new IllegalStateException("Server lookup changed with key identity");
      }
    }

    _candidateInstanceIds = new String[_selectedSegments];
    long expected = 0;
    for (int i = 0; i < _selectedSegments; i++) {
      int server = i % _servers;
      _candidateInstanceIds[i] = canonicalInstanceIds[server];
      expected += server + 1;
    }
    if (lookupSelectedSegments() != expected) {
      throw new IllegalStateException("Selected-segment lookups returned incorrect results");
    }
  }

  @Benchmark
  public long lookupSelectedSegments() {
    long sum = 0;
    for (String instanceId : _candidateInstanceIds) {
      sum += _enabledServerInstanceMap.get(instanceId);
    }
    return sum;
  }
}
