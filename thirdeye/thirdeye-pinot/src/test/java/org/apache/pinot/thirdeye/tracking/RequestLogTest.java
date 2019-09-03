/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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

package org.apache.pinot.thirdeye.tracking;

import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class RequestLogTest {
  private RequestLog log;

  @BeforeMethod
  public void beforeMethod() {
    this.log = new RequestLog(5);
  }

  @Test
  public void testCapacityLimit() {
    this.log.success("a", "", "", 2, 12);
    this.log.success("b", "", "", 0, 10);
    this.log.success("c", "", "", 1, 11);
    this.log.success("d", "", "", 3, 13);
    this.log.success("e", "", "", 4, 14);

    this.log.success("f", "", "", 5, 15); // should discard first

    Assert.assertEquals(this.log.requestLogGauge.get(), 6);
    Assert.assertEquals(this.log.requestLog.size(), 5);
    Assert.assertEquals(this.log.requestLog.pop().datasource, "b");
    Assert.assertEquals(this.log.requestLog.pop().datasource, "c");
    Assert.assertEquals(this.log.requestLog.pop().datasource, "d");
    Assert.assertEquals(this.log.requestLog.pop().datasource, "e");
    Assert.assertEquals(this.log.requestLog.pop().datasource, "f");
  }

  @Test
  public void testTruncate() {
    this.log.success("a", "", "", 0, 10);
    this.log.success("b", "", "", 3, 13); // truncates here
    this.log.success("c", "", "", 1, 11);

    this.log.truncate(2);

    Assert.assertEquals(this.log.requestLogGauge.get(), 2);
    Assert.assertEquals(this.log.requestLog.size(), 2);
    Assert.assertEquals(this.log.requestLog.pop().datasource, "b");
    Assert.assertEquals(this.log.requestLog.pop().datasource, "c");
  }

  @Test
  public void testStatistics() {
    this.log.success("a", "A", "1", 0, 10);
    this.log.failure("a", "A", "2", 1, 11, new Exception());
    this.log.success("b", "B", "1", 2, 12); // stops here
    this.log.success("b", "B", "3", 3, 13);
    this.log.success("b", "B", "2", 2, 12);

    RequestStatistics stats = this.log.getStatistics(2);

    Assert.assertEquals(stats.requestsTotal, 3);
    Assert.assertEquals((long) stats.requestsPerDatasource.get("a"), 2);
    Assert.assertEquals((long) stats.requestsPerDatasource.get("b"), 1);
    Assert.assertEquals((long) stats.requestsPerDataset.get("A"), 2);
    Assert.assertEquals((long) stats.requestsPerDataset.get("B"), 1);
    Assert.assertEquals((long) stats.requestsPerMetric.get("A::1"), 1);
    Assert.assertEquals((long) stats.requestsPerMetric.get("A::2"), 1);
    Assert.assertEquals((long) stats.requestsPerMetric.get("B::1"), 1);
    Assert.assertEquals((long) stats.requestsPerPrincipal.get("no-auth-user"), 3);

    Assert.assertEquals(stats.successTotal, 2);
    Assert.assertEquals((long) stats.successPerDatasource.get("a"), 1);
    Assert.assertEquals((long) stats.successPerDatasource.get("b"), 1);
    Assert.assertEquals((long) stats.successPerDataset.get("A"), 1);
    Assert.assertEquals((long) stats.successPerDataset.get("B"), 1);
    Assert.assertEquals((long) stats.successPerMetric.get("A::1"), 1);
    Assert.assertEquals((long) stats.successPerMetric.get("B::1"), 1);
    Assert.assertEquals((long) stats.successPerPrincipal.get("no-auth-user"), 2);

    Assert.assertEquals(stats.failureTotal, 1);
    Assert.assertEquals((long) stats.failurePerDatasource.get("a"), 1);
    Assert.assertEquals((long) stats.failurePerDataset.get("A"), 1);
    Assert.assertEquals((long) stats.failurePerMetric.get("A::2"), 1);
    Assert.assertEquals((long) stats.failurePerPrincipal.get("no-auth-user"), 1);
  }
}
