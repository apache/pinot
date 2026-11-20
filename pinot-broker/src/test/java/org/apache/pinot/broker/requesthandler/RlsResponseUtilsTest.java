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
package org.apache.pinot.broker.requesthandler;

import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.common.response.broker.BrokerResponseNativeV2;
import org.apache.pinot.common.response.broker.BrokerResponseNativeV2.StatKey;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class RlsResponseUtilsTest {

  /// The multi-stage broker hides the stats that show the layout of the whole table.
  @Test
  public void testHideTableLayoutStatsMultiStage() {
    BrokerResponseNativeV2 response = new BrokerResponseNativeV2();
    StatMap<StatKey> stats = new StatMap<>(StatKey.class)
        .merge(StatKey.TOTAL_DOCS, 1000L)
        .merge(StatKey.NUM_SEGMENTS_QUERIED, 10)
        .merge(StatKey.NUM_SEGMENTS_PROCESSED, 8)
        .merge(StatKey.NUM_SEGMENTS_MATCHED, 5)
        .merge(StatKey.NUM_CONSUMING_SEGMENTS_QUERIED, 2)
        .merge(StatKey.NUM_SEGMENTS_PRUNED_BY_BROKER, 3)
        .merge(StatKey.NUM_SEGMENTS_PRUNED_BY_SERVER, 2)
        .merge(StatKey.NUM_DOCS_SCANNED, 42L);
    response.addBrokerStats(stats);

    RlsResponseUtils.hideTableLayoutStats(response);

    assertEquals(response.getTotalDocs(), 0L);
    assertEquals(response.getNumSegmentsQueried(), 0L);
    assertEquals(response.getNumSegmentsProcessed(), 0L);
    assertEquals(response.getNumSegmentsMatched(), 0L);
    assertEquals(response.getNumConsumingSegmentsQueried(), 0L);
    assertEquals(response.getNumSegmentsPrunedByBroker(), 0L);
    assertEquals(response.getNumSegmentsPrunedByServer(), 0L);
    // numDocsScanned only counts the rows of the tenant, so it stays
    assertEquals(response.getNumDocsScanned(), 42L);
  }
}
