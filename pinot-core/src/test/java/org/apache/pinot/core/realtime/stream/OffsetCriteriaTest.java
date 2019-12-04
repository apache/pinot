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
package org.apache.pinot.core.realtime.stream;

import org.apache.pinot.spi.stream.OffsetCriteria;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Tests building of supported offset criteria
 */
public class OffsetCriteriaTest {

  @Test
  public void testOffsetCriteria() {
    // Build smallest offset criteria
    OffsetCriteria offsetCriteria = new OffsetCriteria.OffsetCriteriaBuilder().withOffsetSmallest();
    Assert.assertTrue(offsetCriteria.isSmallest());
    Assert.assertFalse(offsetCriteria.isLargest());
    Assert.assertFalse(offsetCriteria.isPeriod());
    Assert.assertFalse(offsetCriteria.isCustom());
    Assert.assertEquals(offsetCriteria.getOffsetString(), "smallest");

    // Build largest offset criteria
    offsetCriteria = new OffsetCriteria.OffsetCriteriaBuilder().withOffsetLargest();
    Assert.assertFalse(offsetCriteria.isSmallest());
    Assert.assertTrue(offsetCriteria.isLargest());
    Assert.assertFalse(offsetCriteria.isPeriod());
    Assert.assertFalse(offsetCriteria.isCustom());
    Assert.assertEquals(offsetCriteria.getOffsetString(), "largest");

    // Build period offset criteria
    offsetCriteria = new OffsetCriteria.OffsetCriteriaBuilder().withOffsetAsPeriod("5d");
    Assert.assertFalse(offsetCriteria.isSmallest());
    Assert.assertFalse(offsetCriteria.isLargest());
    Assert.assertTrue(offsetCriteria.isPeriod());
    Assert.assertFalse(offsetCriteria.isCustom());
    Assert.assertEquals(offsetCriteria.getOffsetString(), "5d");

    // Build custom offset criteria
    offsetCriteria = new OffsetCriteria.OffsetCriteriaBuilder().withOffsetCustom("custom criteria");
    Assert.assertFalse(offsetCriteria.isSmallest());
    Assert.assertFalse(offsetCriteria.isLargest());
    Assert.assertFalse(offsetCriteria.isPeriod());
    Assert.assertTrue(offsetCriteria.isCustom());
    Assert.assertEquals(offsetCriteria.getOffsetString(), "custom criteria");

    // Build smallest offset criteria with offset string
    offsetCriteria = new OffsetCriteria.OffsetCriteriaBuilder().withOffsetString("smallest");
    Assert.assertTrue(offsetCriteria.isSmallest());
    Assert.assertFalse(offsetCriteria.isLargest());
    Assert.assertFalse(offsetCriteria.isPeriod());
    Assert.assertFalse(offsetCriteria.isCustom());

    // Build largest offset criteria with offset string
    offsetCriteria = new OffsetCriteria.OffsetCriteriaBuilder().withOffsetString("LARGEST");
    Assert.assertFalse(offsetCriteria.isSmallest());
    Assert.assertTrue(offsetCriteria.isLargest());
    Assert.assertFalse(offsetCriteria.isPeriod());
    Assert.assertFalse(offsetCriteria.isCustom());

    // Build period offset criteria with offset string
    offsetCriteria = new OffsetCriteria.OffsetCriteriaBuilder().withOffsetString("6h");
    Assert.assertFalse(offsetCriteria.isSmallest());
    Assert.assertFalse(offsetCriteria.isLargest());
    Assert.assertTrue(offsetCriteria.isPeriod());
    Assert.assertFalse(offsetCriteria.isCustom());

    // Build incorrect period offset criteria with offset string
    offsetCriteria = new OffsetCriteria.OffsetCriteriaBuilder().withOffsetString("-6h");
    Assert.assertFalse(offsetCriteria.isSmallest());
    Assert.assertFalse(offsetCriteria.isLargest());
    Assert.assertFalse(offsetCriteria.isPeriod());
    Assert.assertTrue(offsetCriteria.isCustom());

    // Build custom offset criteria with offset string
    offsetCriteria = new OffsetCriteria.OffsetCriteriaBuilder().withOffsetString("myCriteria");
    Assert.assertFalse(offsetCriteria.isSmallest());
    Assert.assertFalse(offsetCriteria.isLargest());
    Assert.assertFalse(offsetCriteria.isPeriod());
    Assert.assertTrue(offsetCriteria.isCustom());
  }
}
