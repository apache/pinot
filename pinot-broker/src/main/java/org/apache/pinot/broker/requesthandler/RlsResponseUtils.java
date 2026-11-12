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

import java.util.List;
import org.apache.pinot.common.response.BrokerResponse;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.BrokerResponseNativeV2;
import org.apache.pinot.common.response.broker.BrokerResponseNativeV2.StatKey;


/// Response handling for queries with a row-level security (RLS) filter.
///
/// Stateless and thread-safe.
public class RlsResponseUtils {
  /// The multi-stage stats that show the layout of the whole table.
  private static final List<StatKey> TABLE_LAYOUT_STAT_KEYS = List.of(StatKey.TOTAL_DOCS,
      StatKey.NUM_SEGMENTS_QUERIED, StatKey.NUM_SEGMENTS_PROCESSED, StatKey.NUM_SEGMENTS_MATCHED,
      StatKey.NUM_CONSUMING_SEGMENTS_QUERIED, StatKey.NUM_CONSUMING_SEGMENTS_PROCESSED,
      StatKey.NUM_CONSUMING_SEGMENTS_MATCHED, StatKey.NUM_SEGMENTS_PRUNED_BY_BROKER,
      StatKey.NUM_SEGMENTS_PRUNED_BY_SERVER, StatKey.NUM_SEGMENTS_PRUNED_INVALID, StatKey.NUM_SEGMENTS_PRUNED_BY_LIMIT,
      StatKey.NUM_SEGMENTS_PRUNED_BY_VALUE);

  private RlsResponseUtils() {
  }

  /// Sets to 0 the stats that show the layout of the whole table: the total number of documents, and the number of
  /// segments queried, processed, matched and pruned. A table that holds only the rows of the tenant has a different
  /// layout, so these values cannot be emulated.
  ///
  /// A new server already leaves these stats out under RLS. The broker also hides them, for an old server.
  ///
  /// Call this after the broker logs the query, so that the broker query log keeps the real values.
  public static void hideTableLayoutStats(BrokerResponse brokerResponse) {
    if (brokerResponse instanceof BrokerResponseNative) {
      BrokerResponseNative response = (BrokerResponseNative) brokerResponse;
      response.setTotalDocs(0);
      response.setNumSegmentsQueried(0);
      response.setNumSegmentsProcessed(0);
      response.setNumSegmentsMatched(0);
      response.setNumConsumingSegmentsQueried(0);
      response.setNumConsumingSegmentsProcessed(0);
      response.setNumConsumingSegmentsMatched(0);
      response.setNumSegmentsPrunedByBroker(0);
      response.setNumSegmentsPrunedByServer(0);
      response.setNumSegmentsPrunedInvalid(0);
      response.setNumSegmentsPrunedByLimit(0);
      response.setNumSegmentsPrunedByValue(0);
    } else if (brokerResponse instanceof BrokerResponseNativeV2) {
      BrokerResponseNativeV2 response = (BrokerResponseNativeV2) brokerResponse;
      for (StatKey key : TABLE_LAYOUT_STAT_KEYS) {
        response.removeBrokerStat(key);
      }
    }
  }
}
