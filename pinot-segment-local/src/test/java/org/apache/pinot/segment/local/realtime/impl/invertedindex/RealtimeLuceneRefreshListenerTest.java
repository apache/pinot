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
package org.apache.pinot.segment.local.realtime.impl.invertedindex;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.function.Supplier;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;


public class RealtimeLuceneRefreshListenerTest {

  @BeforeClass
  public void setUp() {
    ServerMetrics.register(mock(ServerMetrics.class));
  }

  @Test
  public void testRefreshTrue() {
    MutableIntSupplier numDocsSupplier = new MutableIntSupplier(0);
    Clock clock = Clock.fixed(Instant.ofEpochMilli(0), ZoneId.systemDefault());
    RealtimeLuceneRefreshListener listener =
        new RealtimeLuceneRefreshListener("table1", "segment1", "column1", 1, numDocsSupplier, clock);
    Supplier<Integer> numDocsDelaySupplier = listener.getNumDocsDelaySupplier();
    Supplier<Long> timeMsDelaySupplier = listener.getTimeMsDelaySupplier();

    // initiate state
    assertEquals(numDocsDelaySupplier.get(), 0);
    assertEquals(timeMsDelaySupplier.get(), 0);

    // time passes, docs indexed. expect up to date delays
    incrementNumDocs(numDocsSupplier, 10);
    incrementClock(listener, 10);
    assertEquals(numDocsDelaySupplier.get(), 10);
    assertEquals(timeMsDelaySupplier.get(), 10);

    // try refresh
    listener.beforeRefresh();
    assertEquals(numDocsDelaySupplier.get(), 10);
    assertEquals(timeMsDelaySupplier.get(), 10);

    // refresh success
    incrementClock(listener, 10);
    listener.afterRefresh(true);
    assertEquals(numDocsDelaySupplier.get(), 0);
    assertEquals(timeMsDelaySupplier.get(), 0);
  }

  @Test
  public void testRefreshFalse() {
    MutableIntSupplier numDocsSupplier = new MutableIntSupplier(0);
    Clock clock = Clock.fixed(Instant.ofEpochMilli(0), ZoneId.systemDefault());
    RealtimeLuceneRefreshListener listener =
        new RealtimeLuceneRefreshListener("table1", "segment1", "column1", 1, numDocsSupplier, clock);
    Supplier<Integer> numDocsDelaySupplier = listener.getNumDocsDelaySupplier();
    Supplier<Long> timeMsDelaySupplier = listener.getTimeMsDelaySupplier();

    // initiate state
    assertEquals(numDocsDelaySupplier.get(), 0);
    assertEquals(timeMsDelaySupplier.get(), 0);

    // time passes, docs indexed. expect up to date delays
    incrementNumDocs(numDocsSupplier, 10);
    incrementClock(listener, 10);
    assertEquals(numDocsDelaySupplier.get(), 10);
    assertEquals(timeMsDelaySupplier.get(), 10);

    // try refresh
    listener.beforeRefresh();
    assertEquals(numDocsDelaySupplier.get(), 10);
    assertEquals(timeMsDelaySupplier.get(), 10);

    // refresh false
    incrementClock(listener, 10);
    listener.afterRefresh(false);
    assertEquals(numDocsDelaySupplier.get(), 10);
    assertEquals(timeMsDelaySupplier.get(), 20);
  }

  @Test
  public void testRefreshFalseWithNoDocsAdded() {
    MutableIntSupplier numDocsSupplier = new MutableIntSupplier(0);
    Clock clock = Clock.fixed(Instant.ofEpochMilli(0), ZoneId.systemDefault());
    RealtimeLuceneRefreshListener listener =
        new RealtimeLuceneRefreshListener("table1", "segment1", "column1", 1, numDocsSupplier, clock);
    Supplier<Integer> numDocsDelaySupplier = listener.getNumDocsDelaySupplier();
    Supplier<Long> timeMsDelaySupplier = listener.getTimeMsDelaySupplier();

    // initiate state
    assertEquals(numDocsDelaySupplier.get(), 0);
    assertEquals(timeMsDelaySupplier.get(), 0);

    // time passes, no more docs indexed, there should be no delay
    incrementClock(listener, 10);
    assertEquals(numDocsDelaySupplier.get(), 0);
    assertEquals(timeMsDelaySupplier.get(), 0);

    // try refresh
    listener.beforeRefresh();
    assertEquals(numDocsDelaySupplier.get(), 0);
    assertEquals(timeMsDelaySupplier.get(), 0);

    // refresh false
    incrementClock(listener, 10);
    listener.afterRefresh(false);
    assertEquals(numDocsDelaySupplier.get(), 0);
    assertEquals(timeMsDelaySupplier.get(), 0);
  }

  @Test
  public void testFirstRefresh() {
    // index creator is initialized with a pause before docs are indexed, so we must ensure the first refresh
    // does not report an excessive delay
    MutableIntSupplier numDocsSupplier = new MutableIntSupplier(0);
    Clock clock = Clock.fixed(Instant.ofEpochMilli(0), ZoneId.systemDefault());
    RealtimeLuceneRefreshListener listener =
        new RealtimeLuceneRefreshListener("table1", "segment1", "column1", 1, numDocsSupplier, clock);
    Supplier<Integer> numDocsDelaySupplier = listener.getNumDocsDelaySupplier();
    Supplier<Long> timeMsDelaySupplier = listener.getTimeMsDelaySupplier();

    // initiate state
    assertEquals(numDocsDelaySupplier.get(), 0);
    assertEquals(timeMsDelaySupplier.get(), 0);

    // time passes, no more docs indexed, there should be no delay
    incrementClock(listener, 10);
    assertEquals(numDocsDelaySupplier.get(), 0);
    assertEquals(timeMsDelaySupplier.get(), 0);

    // time passes, no more docs indexed, there should be no delay
    incrementClock(listener, 10);
    assertEquals(numDocsDelaySupplier.get(), 0);
    assertEquals(timeMsDelaySupplier.get(), 0);

    // more time passes, initial docs indexed after time has passed, therefore delay should be zero
    incrementNumDocs(numDocsSupplier, 10);
    assertEquals(numDocsDelaySupplier.get(), 10);
    assertEquals(timeMsDelaySupplier.get(), 0);

    // try refresh
    listener.beforeRefresh();
    incrementClock(listener, 10);
    assertEquals(numDocsDelaySupplier.get(), 10);
    assertEquals(timeMsDelaySupplier.get(), 10);

    // refresh true
    incrementClock(listener, 10);
    listener.afterRefresh(true);
    assertEquals(numDocsDelaySupplier.get(), 0);
    assertEquals(timeMsDelaySupplier.get(), 0);
  }

  private void incrementClock(RealtimeLuceneRefreshListener listener, long millis) {
    Clock offsetClock = Clock.offset(listener.getClock(), Duration.ofMillis(millis));
    listener.setClock(offsetClock);
  }

  private void incrementNumDocs(MutableIntSupplier mutableIntSupplier, int docs) {
    mutableIntSupplier.increment(docs);
  }

  // Helper for simulating increasing doc count
  private static class MutableIntSupplier implements Supplier<Integer> {
    private int _value;

    public MutableIntSupplier(int initialValue) {
      _value = initialValue;
    }

    @Override
    public Integer get() {
      return _value;
    }

    public void increment(int i) {
      _value += i;
    }
  }
}
