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
import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;


public class DruidResponseTime {
  private DruidResponseTime() {
  }

  private static final String[] QUERIES =
      {
          "SELECT SUM(l_extendedprice), SUM(l_discount) FROM tpch_lineitem",
          "SELECT SUM(l_extendedprice) FROM tpch_lineitem WHERE l_returnflag = 'R'",
          "SELECT SUM(l_extendedprice) FROM tpch_lineitem WHERE l_shipdate BETWEEN '1996-12-01' AND '1996-12-31'",
          "SELECT SUM(l_extendedprice) FROM tpch_lineitem GROUP BY l_shipdate",
          "SELECT SUM(l_extendedprice), SUM(l_quantity) FROM tpch_lineitem GROUP BY l_shipdate",
          "SELECT SUM(l_extendedprice) FROM tpch_lineitem WHERE l_shipdate BETWEEN '1995-01-01' AND '1996-12-31' GROUP BY l_shipdate",
          "SELECT SUM(l_extendedprice) FROM tpch_lineitem WHERE l_shipmode in ('RAIL', 'FOB') AND l_receiptdate BETWEEN '1997-01-01' AND '1997-12-31' GROUP BY l_shipmode"
      };
  private static final String QUERY_FILE_DIR = "druid_queries";
  private static final String RESULT_DIR = "druid_results";
  private static final int RECORD_NUMBER = 6001215;
  private static final int WARMUP_ROUND = 10;
  private static final int TEST_ROUND = 20;
  private static final boolean STORE_RESULT = false;

  private static final byte[] BYTE_BUFFER = new byte[4096];
  private static final char[] CHAR_BUFFER = new char[4096];

  public static void main(String[] args) throws Exception {
    try (CloseableHttpClient client = HttpClients.createDefault()) {
      HttpPost post = new HttpPost("http://localhost:8082/druid/v2/?pretty");
      post.addHeader("content-type", "application/json");
      CloseableHttpResponse res;

      if (STORE_RESULT) {
        File dir = new File(RESULT_DIR);
        if (!dir.exists()) {
          dir.mkdirs();
        }
      }

      int length;

      // Make sure all segments online
      System.out.println("Test if number of records is " + RECORD_NUMBER);
      post.setEntity(new StringEntity("{" + "\"queryType\":\"timeseries\"," + "\"dataSource\":\"tpch_lineitem\"," + "\"intervals\":[\"1992-01-01/1999-01-01\"],"
          + "\"granularity\":\"all\"," + "\"aggregations\":[{\"type\":\"count\",\"name\":\"count\"}]}"));
      while (true) {
        System.out.print('*');
        res = client.execute(post);
        boolean valid;
        try (BufferedInputStream in = new BufferedInputStream(res.getEntity().getContent())) {
          length = in.read(BYTE_BUFFER);
          valid = new String(BYTE_BUFFER, 0, length, "UTF-8").contains("\"count\" : 6001215");
        }
        res.close();
        if (valid) {
          break;
        } else {
          Thread.sleep(5000);
        }
      }
      System.out.println("Number of Records Test Passed");

      for (int i = 0; i < QUERIES.length; i++) {
        System.out.println("--------------------------------------------------------------------------------");
        System.out.println("Start running query: " + QUERIES[i]);
        try (BufferedReader reader = new BufferedReader(
            new FileReader(QUERY_FILE_DIR + File.separator + i + ".json"))) {
          length = reader.read(CHAR_BUFFER);
          post.setEntity(new StringEntity(new String(CHAR_BUFFER, 0, length)));
        }

        // Warm-up Rounds
        System.out.println("Run " + WARMUP_ROUND + " times to warm up cache...");
        for (int j = 0; j < WARMUP_ROUND; j++) {
          res = client.execute(post);
          res.close();
          System.out.print('*');
        }
        System.out.println();

        // Test Rounds
        int[] time = new int[TEST_ROUND];
        int totalTime = 0;
        System.out.println("Run " + TEST_ROUND + " times to get average time...");
        for (int j = 0; j < TEST_ROUND; j++) {
          long startTime = System.currentTimeMillis();
          res = client.execute(post);
          long endTime = System.currentTimeMillis();
          if (STORE_RESULT && j == 0) {
            try (
                BufferedInputStream in = new BufferedInputStream(res.getEntity().getContent());
                BufferedWriter writer = new BufferedWriter(new FileWriter(RESULT_DIR + File.separator + i + ".json", false))
            ) {
              while ((length = in.read(BYTE_BUFFER)) > 0) {
                writer.write(new String(BYTE_BUFFER, 0, length, "UTF-8"));
              }
            }
          }
          res.close();
          time[j] = (int) (endTime - startTime);
          totalTime += time[j];
          System.out.print(time[j] + "ms ");
        }
        System.out.println();

        // Process Results
        double avgTime = (double) totalTime / TEST_ROUND;
        double stdDev = 0;
        for (int temp : time) {
          stdDev += (temp - avgTime) * (temp - avgTime) / TEST_ROUND;
        }
        stdDev = Math.sqrt(stdDev);
        System.out.println("The average response time for the query is: " + avgTime + "ms");
        System.out.println("The standard deviation is: " + stdDev);
      }
    }
  }
}
