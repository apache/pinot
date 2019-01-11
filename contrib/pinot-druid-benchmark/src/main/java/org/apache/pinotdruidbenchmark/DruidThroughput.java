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
package org.apache.pinotdruidbenchmark;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;


/**
 * Test throughput for Druid.
 */
public class DruidThroughput {
  private DruidThroughput() {
  }

  private static final long RANDOM_SEED = 123456789L;
  private static final Random RANDOM = new Random(RANDOM_SEED);
  private static final char[] CHAR_BUFFER = new char[4096];

  private static final int MILLIS_PER_SECOND = 1000;
  private static final int REPORT_INTERVAL_MILLIS = 3000;

  @SuppressWarnings("InfiniteLoopStatement")
  public static void main(String[] args)
      throws Exception {
    if (args.length != 3 && args.length != 4) {
      System.err.println("3 or 4 arguments required: QUERY_DIR, RESOURCE_URL, NUM_CLIENTS, TEST_TIME (seconds).");
      return;
    }

    File queryDir = new File(args[0]);
    String resourceUrl = args[1];
    final int numClients = Integer.parseInt(args[2]);
    final long endTime;
    if (args.length == 3) {
      endTime = Long.MAX_VALUE;
    } else {
      endTime = System.currentTimeMillis() + Integer.parseInt(args[3]) * MILLIS_PER_SECOND;
    }

    File[] queryFiles = queryDir.listFiles();
    assert queryFiles != null;
    Arrays.sort(queryFiles);

    final int numQueries = queryFiles.length;
    final HttpPost[] httpPosts = new HttpPost[numQueries];
    for (int i = 0; i < numQueries; i++) {
      HttpPost httpPost = new HttpPost(resourceUrl);
      httpPost.addHeader("content-type", "application/json");
      StringBuilder stringBuilder = new StringBuilder();
      try (BufferedReader bufferedReader = new BufferedReader(new FileReader(queryFiles[i]))) {
        int length;
        while ((length = bufferedReader.read(CHAR_BUFFER)) > 0) {
          stringBuilder.append(new String(CHAR_BUFFER, 0, length));
        }
      }
      String query = stringBuilder.toString();
      httpPost.setEntity(new StringEntity(query));
      httpPosts[i] = httpPost;
    }

    final AtomicInteger counter = new AtomicInteger(0);
    final AtomicLong totalResponseTime = new AtomicLong(0L);
    final ExecutorService executorService = Executors.newFixedThreadPool(numClients);

    for (int i = 0; i < numClients; i++) {
      executorService.submit(new Runnable() {
        @Override
        public void run() {
          try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            while (System.currentTimeMillis() < endTime) {
              long startTime = System.currentTimeMillis();
              CloseableHttpResponse httpResponse = httpClient.execute(httpPosts[RANDOM.nextInt(numQueries)]);
              httpResponse.close();
              long responseTime = System.currentTimeMillis() - startTime;
              counter.getAndIncrement();
              totalResponseTime.getAndAdd(responseTime);
            }
          } catch (IOException e) {
            e.printStackTrace();
          }
        }
      });
    }
    executorService.shutdown();

    long startTime = System.currentTimeMillis();
    while (System.currentTimeMillis() < endTime) {
      Thread.sleep(REPORT_INTERVAL_MILLIS);
      double timePassedSeconds = ((double) (System.currentTimeMillis() - startTime)) / MILLIS_PER_SECOND;
      int count = counter.get();
      double avgResponseTime = ((double) totalResponseTime.get()) / count;
      System.out.println(
          "Time Passed: " + timePassedSeconds + "s, Query Executed: " + count + ", QPS: " + count / timePassedSeconds
              + ", Avg Response Time: " + avgResponseTime + "ms");
    }
  }
}
