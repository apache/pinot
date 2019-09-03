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
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;


/**
 * Test single query response time for Pinot.
 */
public class PinotResponseTime {
  private PinotResponseTime() {
  }

  private static final byte[] BYTE_BUFFER = new byte[4096];

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

      for (File queryFile : queryFiles) {
        String query = new BufferedReader(new FileReader(queryFile)).readLine();
        httpPost.setEntity(new StringEntity("{\"pql\":\"" + query + "\"}"));

        System.out.println("--------------------------------------------------------------------------------");
        System.out.println("Running query: " + query);
        System.out.println("--------------------------------------------------------------------------------");

        // Warm-up Rounds
        System.out.println("Run " + warmUpRounds + " times to warm up...");
        for (int i = 0; i < warmUpRounds; i++) {
          CloseableHttpResponse httpResponse = httpClient.execute(httpPost);
          httpResponse.close();
          System.out.print('*');
        }
        System.out.println();

        // Test Rounds
        System.out.println("Run " + testRounds + " times to get response time statistics...");
        long[] responseTimes = new long[testRounds];
        long totalResponseTime = 0L;
        for (int i = 0; i < testRounds; i++) {
          long startTime = System.currentTimeMillis();
          CloseableHttpResponse httpResponse = httpClient.execute(httpPost);
          httpResponse.close();
          long responseTime = System.currentTimeMillis() - startTime;
          responseTimes[i] = responseTime;
          totalResponseTime += responseTime;
          System.out.print(responseTime + "ms ");
        }
        System.out.println();

        // Store result.
        if (resultDir != null) {
          File resultFile = new File(resultDir, queryFile.getName() + ".result");
          CloseableHttpResponse httpResponse = httpClient.execute(httpPost);
          try (BufferedInputStream bufferedInputStream = new BufferedInputStream(httpResponse.getEntity().getContent());
              BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(resultFile))) {
            int length;
            while ((length = bufferedInputStream.read(BYTE_BUFFER)) > 0) {
              bufferedWriter.write(new String(BYTE_BUFFER, 0, length));
            }
          }
          httpResponse.close();
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
