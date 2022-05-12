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
package org.apache.pinot.segment.local.utils.nativefst.mutablefst;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.pinot.segment.local.utils.nativefst.mutablefst.utils.MutableFSTUtils.regexQueryNrHitsForRealTimeFST;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;


public class MutableFSTConcurrentTest {
  private ExecutorService _threadPool;
  private Set<String> _resultSet;

  @BeforeClass
  private void setup() {
    _threadPool = Executors.newFixedThreadPool(2);
    _resultSet = new HashSet<>();
  }

  @AfterClass
  private void shutDown() {
    _threadPool.shutdown();
  }

  @Test
  public void testConcurrentWriteAndRead()
      throws InterruptedException {
    MutableFST mutableFST = new MutableFSTImpl();
    List<String> words = new ArrayList<>();
    CountDownLatch countDownLatch = new CountDownLatch(2);

    words.add("ab");
    words.add("abba");
    words.add("aba");
    words.add("bab");
    words.add("cdd");
    words.add("efg");

    List<Pair<String, Integer>> wordsWithMetadata = new ArrayList<>();
    int i = 1;

    for (String currentWord : words) {
      wordsWithMetadata.add(Pair.of(currentWord, i));
      i++;
    }

    _threadPool.submit(() -> {
      try {
        performReads(mutableFST, words, 10, 200, countDownLatch);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    });

    _threadPool.submit(() -> {
      try {
        performWrites(mutableFST, wordsWithMetadata, 10, countDownLatch);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    });

    countDownLatch.await();

    assertEquals(_resultSet.size(), words.size());

    assertTrue("ab not found in result set", _resultSet.contains("ab"));
    assertTrue("abba not found in result set", _resultSet.contains("abba"));
    assertTrue("aba not found in result set", _resultSet.contains("aba"));
    assertTrue("bab not found in result set", _resultSet.contains("bab"));
    assertTrue("cdd not found in result set", _resultSet.contains("cdd"));
    assertTrue("efg not found in result set", _resultSet.contains("efg"));
  }

  @Test
  public void testConcurrentLongWriteAndRead()
      throws InterruptedException {
    MutableFST mutableFST = new MutableFSTImpl();
    List<String> words = new ArrayList<>();
    CountDownLatch countDownLatch = new CountDownLatch(2);

    mutableFST.addPath("ab", 1);

    List<Pair<String, Integer>> wordsWithMetadata = new ArrayList<>();

    // Add some write pressure
    wordsWithMetadata.add(Pair.of("egegdgrbsbrsegzgzegzegegjntnmtj", 2));
    wordsWithMetadata.add(Pair.of("hrwbwefweg4wreghrtbrassregfesfefefefzew4ere", 2));
    wordsWithMetadata.add(Pair.of("easzegfegrertegbxzzez3erfezgzeddzdewstfefed", 2));
    wordsWithMetadata.add(Pair.of("tjntrhndsrsgezgrsxzetgteszetgezfzezedrefzdzdzdzdz", 2));
    wordsWithMetadata.add(Pair.of("abacxcvbnmlkjjhgfsaqwertyuioopzxcvbnmllkjshfgsfawieeiuefgeurfeoafa", 2));

    words.add("abacxcvbnmlkjjhgfsaqwertyuioopzxcvbnmllkjshfgsfawieeiuefgeurfeoafa");

    _threadPool.submit(() -> {
      try {
        performReads(mutableFST, words, 10, 10, countDownLatch);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    });

    _threadPool.submit(() -> {
      try {
        performWrites(mutableFST, wordsWithMetadata, 0, countDownLatch);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    });

    countDownLatch.await();

    assertEquals(_resultSet.size(), words.size());

    assertTrue("abacxcvbnmlkjjhgfsaqwertyuioopzxcvbnmllkjshfgsfawieeiuefgeurfeoafa not found in result set",
        _resultSet.contains("abacxcvbnmlkjjhgfsaqwertyuioopzxcvbnmllkjshfgsfawieeiuefgeurfeoafa"));

    _resultSet.clear();
  }

  private void performReads(MutableFST fst, List<String> words, int count, long sleepTime,
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

        if (regexQueryNrHitsForRealTimeFST(words.get(j), fst) == 1) {
          _resultSet.add(currentWord);
        }
      }

      Thread.sleep(sleepTime);
    }

    countDownLatch.countDown();
  }

  private void performWrites(MutableFST fst, List<Pair<String, Integer>> wordsAndMetadata, long sleepTime,
      CountDownLatch countDownLatch)
      throws InterruptedException {

    for (int i = 0; i < wordsAndMetadata.size(); i++) {
      Pair<String, Integer> currentPair = wordsAndMetadata.get(i);

      fst.addPath(currentPair.getLeft(), currentPair.getRight());

      Thread.sleep(sleepTime);
    }

    countDownLatch.countDown();
  }
}
