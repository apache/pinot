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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class NativeMutableTextIndexConcurrentTest {
  private ExecutorService _threadPool;
  private Set<String> _resultSet;

  @BeforeClass
  private void setup() {
    _threadPool = Executors.newFixedThreadPool(10);
    _resultSet = new ConcurrentSkipListSet<>();
  }

  @AfterClass
  private void shutDown() {
    _threadPool.shutdown();
  }

  @Test
  public void testConcurrentWriteAndRead()
      throws InterruptedException, IOException {
    CountDownLatch countDownLatch = new CountDownLatch(2);
    List<String> words = new ArrayList<>();
    words.add("ab");
    words.add("abba");
    words.add("aba");
    words.add("bab");
    words.add("cdd");
    words.add("efg");

    try (NativeMutableTextIndex textIndex = new NativeMutableTextIndex("testFSTColumn")) {
      _threadPool.submit(() -> {
        try {
          performReads(textIndex, words, 20, 200, countDownLatch);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      });

      _threadPool.submit(() -> {
        try {
          performWrites(textIndex, words, 10, countDownLatch);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      });

      countDownLatch.await();
    }

    assertEquals(_resultSet.size(), words.size());

    assertTrue(_resultSet.contains("ab"), "ab not found in result set");
    assertTrue(_resultSet.contains("abba"), "abba not found in result set");
    assertTrue(_resultSet.contains("aba"), "aba not found in result set");
    assertTrue(_resultSet.contains("bab"), "bab not found in result set");
    assertTrue(_resultSet.contains("cdd"), "cdd not found in result set");
    assertTrue(_resultSet.contains("efg"), "efg not found in result set");
  }

  @Test
  public void testConcurrentWriteWithMultipleThreads()
      throws InterruptedException, IOException {
    List<String> firstThreadWords = new ArrayList<>();
    List<String> secondThreadWords = new ArrayList<>();
    List<String> mergedThreadWords = new ArrayList<>();
    CountDownLatch countDownLatch = new CountDownLatch(3);
    firstThreadWords.add("ab");
    firstThreadWords.add("abba");
    firstThreadWords.add("aba");
    secondThreadWords.add("bab");
    secondThreadWords.add("cdd");
    secondThreadWords.add("efg");

    mergedThreadWords.addAll(firstThreadWords);
    mergedThreadWords.addAll(secondThreadWords);

    try (NativeMutableTextIndex textIndex = new NativeMutableTextIndex("testFSTColumn")) {
      _threadPool.submit(() -> {
        try {
          performReads(textIndex, mergedThreadWords, 20, 200, countDownLatch);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      });

      _threadPool.submit(() -> {
        try {
          performWrites(textIndex, firstThreadWords, 10, countDownLatch);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      });

      _threadPool.submit(() -> {
        try {
          performWrites(textIndex, secondThreadWords, 10, countDownLatch);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      });

      countDownLatch.await();
    }

    assertEquals(_resultSet.size(), mergedThreadWords.size());

    assertTrue(_resultSet.contains("ab"), "ab not found in result set");
    assertTrue(_resultSet.contains("abba"), "abba not found in result set");
    assertTrue(_resultSet.contains("aba"), "aba not found in result set");
    assertTrue(_resultSet.contains("bab"), "bab not found in result set");
    assertTrue(_resultSet.contains("cdd"), "cdd not found in result set");
    assertTrue(_resultSet.contains("efg"), "efg not found in result set");
  }

  @Test
  public void testMayhem()
      throws InterruptedException, IOException {
    List<String> firstThreadWords = new ArrayList<>();
    List<String> secondThreadWords = new ArrayList<>();
    List<String> mergedThreadWords = new ArrayList<>();
    CountDownLatch countDownLatch = new CountDownLatch(4);
    firstThreadWords.add("ab");
    firstThreadWords.add("abba");
    firstThreadWords.add("aba");
    secondThreadWords.add("bab");
    secondThreadWords.add("cdd");
    secondThreadWords.add("efg");

    mergedThreadWords.addAll(firstThreadWords);
    mergedThreadWords.addAll(secondThreadWords);

    try (NativeMutableTextIndex textIndex = new NativeMutableTextIndex("testFSTColumn")) {
      _threadPool.submit(() -> {
        try {
          performReads(textIndex, firstThreadWords, 20, 200, countDownLatch);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      });

      _threadPool.submit(() -> {
        try {
          performWrites(textIndex, secondThreadWords, 10, countDownLatch);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      });

      _threadPool.submit(() -> {
        try {
          performReads(textIndex, secondThreadWords, 20, 200, countDownLatch);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      });

      _threadPool.submit(() -> {
        try {
          performWrites(textIndex, firstThreadWords, 10, countDownLatch);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      });

      countDownLatch.await();
    }

    assertEquals(_resultSet.size(), mergedThreadWords.size());

    assertTrue(_resultSet.contains("ab"), "ab not found in result set");
    assertTrue(_resultSet.contains("abba"), "abba not found in result set");
    assertTrue(_resultSet.contains("aba"), "aba not found in result set");
    assertTrue(_resultSet.contains("bab"), "bab not found in result set");
    assertTrue(_resultSet.contains("cdd"), "cdd not found in result set");
    assertTrue(_resultSet.contains("efg"), "efg not found in result set");
  }

  private void performReads(NativeMutableTextIndex textIndex, List<String> words, int count, long sleepTime,
      CountDownLatch countDownLatch)
      throws InterruptedException {

    for (int i = 0; i < count; i++) {
      if (_resultSet.size() == words.size()) {
        break;
      }

      for (int j = 0; j < words.size(); j++) {
        String currentWord = words.get(j);

        if (_resultSet.contains(currentWord)) {
          continue;
        }

        if (textIndex.getDocIds(words.get(j)).getCardinality() == 1) {
          _resultSet.add(currentWord);
        }
      }

      Thread.sleep(sleepTime);
    }

    countDownLatch.countDown();
  }

  private void performWrites(NativeMutableTextIndex textIndex, List<String> words, long sleepTime,
      CountDownLatch countDownLatch)
      throws InterruptedException {

    for (int i = 0; i < words.size(); i++) {
      textIndex.add(words.get(i));

      Thread.sleep(sleepTime);
    }

    countDownLatch.countDown();
  }
}
