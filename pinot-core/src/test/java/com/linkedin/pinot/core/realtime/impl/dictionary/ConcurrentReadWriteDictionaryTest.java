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
package com.linkedin.pinot.core.realtime.impl.dictionary;

import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Tests for concurrent read and write against REALTIME dictionary.
 * <p>For now just test against {@link IntOnHeapMutableDictionary}. Index contiguous integers from 1 so that the index
 * for each value is deterministic.
 */
public class ConcurrentReadWriteDictionaryTest {
  private static final int NUM_ENTRIES = 1_000_000;
  private static final int NUM_READERS = 5;
  private static final ExecutorService EXECUTOR_SERVICE = Executors.newFixedThreadPool(NUM_READERS + 1);
  private static final Random RANDOM = new Random();

  private IntOnHeapMutableDictionary _intOnHeapMutableDictionary;

  @BeforeMethod
  public void setUp() {
    _intOnHeapMutableDictionary = new IntOnHeapMutableDictionary();
  }

  @Test
  public void testSingleReaderSingleWriter()
      throws Exception {
    Future<Void> readerFuture = EXECUTOR_SERVICE.submit(new Reader());
    Future<Void> writerFuture = EXECUTOR_SERVICE.submit(new Writer());

    readerFuture.get();
    writerFuture.get();
  }

  @Test
  public void testMultiReadersSingleWriter()
      throws Exception {
    Future[] readerFutures = new Future[NUM_READERS];
    for (int i = 0; i < NUM_READERS; i++) {
      readerFutures[i] = EXECUTOR_SERVICE.submit(new Reader());
    }
    Future<Void> writerFuture = EXECUTOR_SERVICE.submit(new Writer());

    for (int i = 0; i < NUM_READERS; i++) {
      readerFutures[i].get();
    }
    writerFuture.get();
  }

  @AfterClass
  public void tearDown() {
    EXECUTOR_SERVICE.shutdown();
  }

  /**
   * Reader to read the index of each value after it's indexed into the dictionary, then get the value from the index.
   * <p>We can assume that we always first get the index of a value, then use the index to fetch the value.
   */
  private class Reader implements Callable<Void> {

    @Override
    public Void call()
        throws Exception {
      for (int i = 0; i < NUM_ENTRIES; i++) {
        int dictId;
        do {
          dictId = _intOnHeapMutableDictionary.indexOf(i + 1);
        } while (dictId < 0);
        Assert.assertEquals(dictId, i);
        Assert.assertEquals(_intOnHeapMutableDictionary.getIntValue(dictId), i + 1);

        // Fetch value by a random existing dictId
        int randomDictId = RANDOM.nextInt(i + 1);
        Assert.assertEquals(_intOnHeapMutableDictionary.getIntValue(randomDictId), randomDictId + 1);
      }
      return null;
    }
  }

  /**
   * Writer to index value into dictionary, then check the index of the value.
   */
  private class Writer implements Callable<Void> {

    @Override
    public Void call()
        throws Exception {
      for (int i = 0; i < NUM_ENTRIES; i++) {
        _intOnHeapMutableDictionary.index(i + 1);
        Assert.assertEquals(_intOnHeapMutableDictionary.indexOf(i + 1), i);

        // Index a random existing value
        int randomValue = RANDOM.nextInt(i + 1) + 1;
        _intOnHeapMutableDictionary.index(randomValue);
        Assert.assertEquals(_intOnHeapMutableDictionary.indexOf(randomValue), randomValue - 1);
      }
      return null;
    }
  }
}
