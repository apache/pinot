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
package org.apache.pinot.broker.routing.intervalst;

import org.apache.pinot.broker.routing.segmentmetadata.Interval;
import org.testng.Assert;
import org.testng.annotations.Test;


public class IntervalTest {

  @Test
  public void testInterval() {
    Interval a = new Interval(1, 5);
    Interval b = new Interval(6, 8);
    Interval c = new Interval(1, 4);
    Interval d = new Interval(1, 6);
    Interval e = new Interval(1, 5);
    Interval f = new Interval(5, 5);

    // test compareTo
    Assert.assertTrue(a.compareTo(b) < 0);
    Assert.assertTrue(a.compareTo(c) > 0);
    Assert.assertTrue(a.compareTo(d) < 0);
    Assert.assertTrue(a.compareTo(e) == 0);

    // test intersects
    Assert.assertFalse(a.intersects(b));
    Assert.assertTrue(a.intersects(f));

    // test intersection
    Assert.assertEquals(Interval.getIntersection(a, b), null);
    Assert.assertEquals(Interval.getIntersection(a, c), c);
    Assert.assertEquals(Interval.getIntersection(b, d), new Interval(6, 6));

    // test union
    Assert.assertEquals(Interval.getUnion(a, b), null);
    Assert.assertEquals(Interval.getUnion(b, d), new Interval(1, 8));
    Assert.assertEquals(Interval.getUnion(a, d), d);
  }
}
