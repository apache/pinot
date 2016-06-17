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
import java.io.IOException;
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


public class PinotThroughput {
  private PinotThroughput() {
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
  private static final int MILLIS_PER_SECOND = 1000;
  private static final long RANDOM_SEED = 123456789L;
  private static final int NUM_CLIENTS = 10;
  private static final int REPORT_INTERVAL_MILLIS = 3000;

  @SuppressWarnings("InfiniteLoopStatement")
  public static void main(String[] args) throws Exception {
    final int numQueries = QUERIES.length;
    final Random random = new Random(RANDOM_SEED);
    final AtomicInteger counter = new AtomicInteger(0);
    final AtomicLong totalResponseTime = new AtomicLong(0L);
    final ExecutorService executorService = Executors.newFixedThreadPool(NUM_CLIENTS);

    for (int i = 0; i < NUM_CLIENTS; i++) {
      executorService.submit(new Runnable() {
        @Override
        public void run() {
          try (CloseableHttpClient client = HttpClients.createDefault()) {
            HttpPost post = new HttpPost("http://localhost:8099/query");
            CloseableHttpResponse res;
            while (true) {
              String query = QUERIES[random.nextInt(numQueries)];
              post.setEntity(new StringEntity("{\"pql\":\"" + query + "\"}"));
              long start = System.currentTimeMillis();
              res = client.execute(post);
              res.close();
              counter.getAndIncrement();
              totalResponseTime.getAndAdd(System.currentTimeMillis() - start);
            }
          } catch (IOException e) {
            e.printStackTrace();
          }
        }
      });
    }

    long startTime = System.currentTimeMillis();
    while (true) {
      Thread.sleep(REPORT_INTERVAL_MILLIS);
      double timePassedSeconds = ((double) (System.currentTimeMillis() - startTime)) / MILLIS_PER_SECOND;
      int count = counter.get();
      double avgResponseTime = ((double) totalResponseTime.get()) / count;
      System.out.println("Time Passed: " + timePassedSeconds + "s, Query Executed: " + count + ", QPS: "
          + count / timePassedSeconds + ", Avg Response Time: " + avgResponseTime + "ms");
    }
  }
}
