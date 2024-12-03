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
package org.apache.pinot.tsdb.m3ql.time;

import java.time.Duration;
import java.util.Collection;
import org.apache.pinot.tsdb.spi.RangeTimeSeriesRequest;
import org.apache.pinot.tsdb.spi.TimeBuckets;
import org.apache.pinot.tsdb.spi.plan.BaseTimeSeriesPlanNode;
import org.apache.pinot.tsdb.spi.plan.LeafTimeSeriesPlanNode;


public class TimeBucketComputer {
  private TimeBucketComputer() {
  }

  public static TimeBuckets compute(BaseTimeSeriesPlanNode planNode, RangeTimeSeriesRequest request) {
    QueryTimeBoundaryConstraints constraints = process(planNode, request);
    long newStartTime = request.getStartSeconds() - constraints.getLeftExtensionSeconds();
    long newEndTime = request.getEndSeconds() + constraints.getRightExtensionSeconds();
    long lcmOfDivisors = lcm(constraints.getDivisors());
    long roundStartTimeDelta = (lcmOfDivisors - (newEndTime - newStartTime) % lcmOfDivisors) % lcmOfDivisors;
    newStartTime -= roundStartTimeDelta;
    int numItems = (int) ((newEndTime - newStartTime) / request.getStepSeconds());
    return TimeBuckets.ofSeconds(newStartTime, Duration.ofSeconds(request.getStepSeconds()), numItems);
  }

  public static QueryTimeBoundaryConstraints process(BaseTimeSeriesPlanNode planNode, RangeTimeSeriesRequest request) {
    if (planNode instanceof LeafTimeSeriesPlanNode) {
      QueryTimeBoundaryConstraints constraints = new QueryTimeBoundaryConstraints();
      constraints.getDivisors().add(request.getStepSeconds());
      return constraints;
    }
    QueryTimeBoundaryConstraints constraints = new QueryTimeBoundaryConstraints();
    for (BaseTimeSeriesPlanNode childNode : planNode.getInputs()) {
      QueryTimeBoundaryConstraints childConstraints = process(childNode, request);
      constraints = QueryTimeBoundaryConstraints.merge(constraints, childConstraints);
    }
    return constraints;
  }

  public static long lcm(Collection<Long> values) {
    long result = 1;
    for (long value : values) {
      result = lcm(result, value);
    }
    return result;
  }

  public static long lcm(long a, long b) {
    return a * b / gcd(a, b);
  }

  public static long gcd(long a, long b) {
    if (a < b) {
      return gcd(b, a);
    }
    if (b == 0) {
      return a;
    }
    return gcd(b, a % b);
  }
}
