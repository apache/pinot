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
package com.linkedin.pinot.query.pruner;

import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.utils.request.FilterQueryTree;
import com.linkedin.pinot.common.utils.request.RequestUtils;
import com.linkedin.pinot.core.query.pruner.TimeSegmentPruner;
import com.linkedin.pinot.pql.parsers.Pql2Compiler;
import junit.framework.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Unit test for {@link com.linkedin.pinot.core.query.pruner.TimeSegmentPruner} class.
 */
public class TimeSegmentPrunerTest {
  private static final String TIME_COLUMN_NAME = "time";
  Pql2Compiler _compiler;

  @BeforeClass
  public void setup() {
    _compiler = new Pql2Compiler();
  }

  @Test
  public void test() {
    // Query without time predicate
    String query = "select count(*) from table where foo = 'bar'";
    Assert.assertFalse(
        runPruner(query, new TimeSegmentPruner.TimeInterval(Long.MIN_VALUE, Long.MAX_VALUE), TIME_COLUMN_NAME));

    // Equality predicate
    query = "select count(*) from table where time = 10";
    Assert.assertFalse(runPruner(query, new TimeSegmentPruner.TimeInterval(0, 10), TIME_COLUMN_NAME));
    Assert.assertFalse(runPruner(query, new TimeSegmentPruner.TimeInterval(10, 20), TIME_COLUMN_NAME));
    Assert.assertFalse(runPruner(query, new TimeSegmentPruner.TimeInterval(10, 10), TIME_COLUMN_NAME));
    Assert.assertTrue(runPruner(query, new TimeSegmentPruner.TimeInterval(15, 20), TIME_COLUMN_NAME));
    Assert.assertTrue(runPruner(query, new TimeSegmentPruner.TimeInterval(15, 25), TIME_COLUMN_NAME));

    // Equality predicate with OR
    query = "select count(*) from table where time = 10 or time = 20";
    Assert.assertTrue(runPruner(query, new TimeSegmentPruner.TimeInterval(0, 9), TIME_COLUMN_NAME));
    Assert.assertTrue(runPruner(query, new TimeSegmentPruner.TimeInterval(11, 19), TIME_COLUMN_NAME));
    Assert.assertTrue(runPruner(query, new TimeSegmentPruner.TimeInterval(25, 35), TIME_COLUMN_NAME));
    Assert.assertFalse(runPruner(query, new TimeSegmentPruner.TimeInterval(0, 10), TIME_COLUMN_NAME));
    Assert.assertFalse(runPruner(query, new TimeSegmentPruner.TimeInterval(10, 20), TIME_COLUMN_NAME));
    Assert.assertFalse(runPruner(query, new TimeSegmentPruner.TimeInterval(10, 10), TIME_COLUMN_NAME));
    Assert.assertFalse(runPruner(query, new TimeSegmentPruner.TimeInterval(20, 20), TIME_COLUMN_NAME));
    Assert.assertFalse(runPruner(query, new TimeSegmentPruner.TimeInterval(15, 25), TIME_COLUMN_NAME));

    // Equality predicate OR'd with range
    query = "select count(*) from table where time = 10 or time between 20 and 30";
    Assert.assertTrue(runPruner(query, new TimeSegmentPruner.TimeInterval(0, 9), TIME_COLUMN_NAME));
    Assert.assertTrue(runPruner(query, new TimeSegmentPruner.TimeInterval(11, 19), TIME_COLUMN_NAME));
    Assert.assertTrue(runPruner(query, new TimeSegmentPruner.TimeInterval(31, 40), TIME_COLUMN_NAME));
    Assert.assertFalse(runPruner(query, new TimeSegmentPruner.TimeInterval(0, 10), TIME_COLUMN_NAME));
    Assert.assertFalse(runPruner(query, new TimeSegmentPruner.TimeInterval(10, 20), TIME_COLUMN_NAME));
    Assert.assertFalse(runPruner(query, new TimeSegmentPruner.TimeInterval(10, 10), TIME_COLUMN_NAME));
    Assert.assertFalse(runPruner(query, new TimeSegmentPruner.TimeInterval(20, 20), TIME_COLUMN_NAME));
    Assert.assertFalse(runPruner(query, new TimeSegmentPruner.TimeInterval(15, 25), TIME_COLUMN_NAME));

    query = "select count(*) from table where time > 10 and time < 20";
    Assert.assertTrue(runPruner(query, new TimeSegmentPruner.TimeInterval(0, 10), TIME_COLUMN_NAME));
    Assert.assertTrue(runPruner(query, new TimeSegmentPruner.TimeInterval(20, 30), TIME_COLUMN_NAME));
    Assert.assertFalse(runPruner(query, new TimeSegmentPruner.TimeInterval(11, 15), TIME_COLUMN_NAME));
    Assert.assertFalse(runPruner(query, new TimeSegmentPruner.TimeInterval(15, 20), TIME_COLUMN_NAME));
    Assert.assertFalse(runPruner(query, new TimeSegmentPruner.TimeInterval(15, 25), TIME_COLUMN_NAME));

    query = "select count(*) from table where time between 10 and 20";
    Assert.assertTrue(runPruner(query, new TimeSegmentPruner.TimeInterval(0, 9), TIME_COLUMN_NAME));
    Assert.assertTrue(runPruner(query, new TimeSegmentPruner.TimeInterval(21, 30), TIME_COLUMN_NAME));
    Assert.assertFalse(runPruner(query, new TimeSegmentPruner.TimeInterval(10, 15), TIME_COLUMN_NAME));
    Assert.assertFalse(runPruner(query, new TimeSegmentPruner.TimeInterval(15, 20), TIME_COLUMN_NAME));
    Assert.assertFalse(runPruner(query, new TimeSegmentPruner.TimeInterval(20, 25), TIME_COLUMN_NAME));

    query = "select count(*) from table where time between 10 and 20 or time between 15 and 25";
    Assert.assertTrue(runPruner(query, new TimeSegmentPruner.TimeInterval(0, 9), TIME_COLUMN_NAME));
    Assert.assertTrue(runPruner(query, new TimeSegmentPruner.TimeInterval(26, 30), TIME_COLUMN_NAME));
    Assert.assertFalse(runPruner(query, new TimeSegmentPruner.TimeInterval(10, 15), TIME_COLUMN_NAME));
    Assert.assertFalse(runPruner(query, new TimeSegmentPruner.TimeInterval(15, 20), TIME_COLUMN_NAME));
    Assert.assertFalse(runPruner(query, new TimeSegmentPruner.TimeInterval(20, 25), TIME_COLUMN_NAME));

    // Disjoint time intervals
    query = "select count(*) from table where time < 10 or time > 20";
    Assert.assertFalse(runPruner(query, new TimeSegmentPruner.TimeInterval(0, 10), TIME_COLUMN_NAME));
    Assert.assertFalse(runPruner(query, new TimeSegmentPruner.TimeInterval(20, 30), TIME_COLUMN_NAME));
    Assert.assertTrue(runPruner(query, new TimeSegmentPruner.TimeInterval(10, 20), TIME_COLUMN_NAME));

    // Time < 0 is not allowed, so it should always prune.
    query = "select count(*) from table where time < 0";
    Assert.assertTrue(
        runPruner(query, new TimeSegmentPruner.TimeInterval(Long.MIN_VALUE, Long.MAX_VALUE), TIME_COLUMN_NAME));

    // Time = 0 is not allowed, so it should always prune.
    query = "select count(*) from table where time between 0 and 0";
    Assert.assertTrue(runPruner(query, new TimeSegmentPruner.TimeInterval(-10, -1), TIME_COLUMN_NAME));
    Assert.assertTrue(runPruner(query, new TimeSegmentPruner.TimeInterval(1, 10), TIME_COLUMN_NAME));
    Assert.assertFalse(runPruner(query, new TimeSegmentPruner.TimeInterval(0, 10), TIME_COLUMN_NAME));
    Assert.assertFalse(runPruner(query, new TimeSegmentPruner.TimeInterval(0, 0), TIME_COLUMN_NAME));

    // Degenerate case of invalid range where start > end
    query = "select count(*) from table where time between 20 and 10";
    Assert.assertTrue(runPruner(query, new TimeSegmentPruner.TimeInterval(0, 9), TIME_COLUMN_NAME));
    Assert.assertTrue(runPruner(query, new TimeSegmentPruner.TimeInterval(20, 30), TIME_COLUMN_NAME));
    Assert.assertTrue(runPruner(query, new TimeSegmentPruner.TimeInterval(10, 15), TIME_COLUMN_NAME));
    Assert.assertTrue(runPruner(query, new TimeSegmentPruner.TimeInterval(15, 20), TIME_COLUMN_NAME));
    Assert.assertTrue(runPruner(query, new TimeSegmentPruner.TimeInterval(20, 25), TIME_COLUMN_NAME));

    // OR with non time column, should never prune.
    query = "select count(*) from table where time between 10 and 20 or foo = 'bar'";
    Assert.assertFalse(runPruner(query, new TimeSegmentPruner.TimeInterval(0, 9), TIME_COLUMN_NAME));
    Assert.assertFalse(runPruner(query, new TimeSegmentPruner.TimeInterval(21, 30), TIME_COLUMN_NAME));
    Assert.assertFalse(runPruner(query, new TimeSegmentPruner.TimeInterval(10, 15), TIME_COLUMN_NAME));
    Assert.assertFalse(runPruner(query, new TimeSegmentPruner.TimeInterval(15, 20), TIME_COLUMN_NAME));
    Assert.assertFalse(runPruner(query, new TimeSegmentPruner.TimeInterval(20, 25), TIME_COLUMN_NAME));
  }

  /**
   * Helper method to invoke the pruner for a given query and time interval.
   *
   * @param query Query for pruning
   * @param segmentTimeInterval Time interval of segment to be pruned.
   * @param timeColumn Time column name of segment to be pruned.
   * @return
   */
  private boolean runPruner(String query, TimeSegmentPruner.TimeInterval segmentTimeInterval, String timeColumn) {
    BrokerRequest brokerRequest = _compiler.compileToBrokerRequest(query);
    FilterQueryTree filterQueryTree = RequestUtils.generateFilterQueryTree(brokerRequest);
    return TimeSegmentPruner.pruneSegment(filterQueryTree, segmentTimeInterval, timeColumn);
  }
}
