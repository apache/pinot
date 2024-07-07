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

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.Arrays;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.io.entity.StringEntity;


/**
 * Test single query response time for Druid.
 */
public class DruidResponseTime {
  private DruidResponseTime() {
  }

  private static final byte[] BYTE_BUFFER = new byte[4096];
  private static final char[] CHAR_BUFFER = new char[4096];

  public static void main(String[] args)
      throws Exception {
    if (args.length != 4 && args.length != 5) {
      System.err.println(
          "4 or 5 arguments required: QUERY_DIR, RESOURCE_URL, WARM_UP_ROUNDS, TEST_ROUNDS, RESULT_DIR (optional).");
      return;
    }

    File queryDir = new File(args[0]);
    String resourceUrl = args[1];
    int warmUpRounds = Integer.parseInt(args[2]);
    int testRounds = Integer.parseInt(args[3]);
    File resultDir;
    if (args.length == 4) {
      resultDir = null;
    } else {
      resultDir = new File(args[4]);
      if (!resultDir.exists()) {
        if (!resultDir.mkdirs()) {
          throw new RuntimeException("Failed to create result directory: " + resultDir);
        }
      }
    }

    File[] queryFiles = queryDir.listFiles();
    assert queryFiles != null;
    Arrays.sort(queryFiles);

    try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
      HttpPost httpPost = new HttpPost(resourceUrl);
      httpPost.addHeader("content-type", "application/json");

      for (File queryFile : queryFiles) {
        StringBuilder stringBuilder = new StringBuilder();
        try (BufferedReader bufferedReader = new BufferedReader(new FileReader(queryFile))) {
          int length;
          while ((length = bufferedReader.read(CHAR_BUFFER)) > 0) {
            stringBuilder.append(new String(CHAR_BUFFER, 0, length));
          }
        }
        String query = stringBuilder.toString();
        httpPost.setEntity(new StringEntity(query));

        System.out.println("--------------------------------------------------------------------------------");
        System.out.println("Running query: " + query);
        System.out.println("--------------------------------------------------------------------------------");

        // Warm-up Rounds
        System.out.println("Run " + warmUpRounds + " times to warm up...");
        for (int i = 0; i < warmUpRounds; i++) {
          try (CloseableHttpResponse httpResponse = httpClient.execute(httpPost)) {
            // httpResponse will be auto closed
          }
          System.out.print('*');
        }
        System.out.println();

        // Test Rounds
        System.out.println("Run " + testRounds + " times to get response time statistics...");
        long[] responseTimes = new long[testRounds];
        long totalResponseTime = 0L;
        for (int i = 0; i < testRounds; i++) {
          long startTime = System.currentTimeMillis();
          try (CloseableHttpResponse httpResponse = httpClient.execute(httpPost)) {
              // httpResponse will be auto closed
          }
          long responseTime = System.currentTimeMillis() - startTime;
          responseTimes[i] = responseTime;
          totalResponseTime += responseTime;
          System.out.print(responseTime + "ms ");
        }
        System.out.println();

        // Store result.
        if (resultDir != null) {
          File resultFile = new File(resultDir, queryFile.getName() + ".result");
          try (CloseableHttpResponse httpResponse = httpClient.execute(httpPost)) {
            try (BufferedInputStream bufferedInputStream = new BufferedInputStream(
                httpResponse.getEntity().getContent());
                BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(resultFile))) {
              int length;
              while ((length = bufferedInputStream.read(BYTE_BUFFER)) > 0) {
                bufferedWriter.write(new String(BYTE_BUFFER, 0, length));
              }
            }
          }
        }

        // Process response times.
        double averageResponseTime = (double) totalResponseTime / testRounds;
        double temp = 0;
        for (long responseTime : responseTimes) {
          temp += (responseTime - averageResponseTime) * (responseTime - averageResponseTime);
        }
        double standardDeviation = Math.sqrt(temp / testRounds);
        System.out.println("Average response time: " + averageResponseTime + "ms");
        System.out.println("Standard deviation: " + standardDeviation);
      }
    }
  }
}
