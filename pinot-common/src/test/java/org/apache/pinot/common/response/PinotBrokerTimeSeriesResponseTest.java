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
package org.apache.pinot.common.response;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.tsdb.spi.TimeBuckets;
import org.apache.pinot.tsdb.spi.series.TimeSeries;
import org.apache.pinot.tsdb.spi.series.TimeSeriesBlock;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


/// Tests the Prometheus range response rendered by the broker.
public class PinotBrokerTimeSeriesResponseTest {
  @Test
  public void testMatrixOmitsMissingSamples()
      throws Exception {
    TimeBuckets buckets = TimeBuckets.ofSeconds(10L, Duration.ofSeconds(10L), 3);
    List<String> tags = List.of("__name__");
    Object[] labels = {"requests"};
    TimeSeries empty = new TimeSeries("1", null, buckets, new Double[3], tags, labels);
    TimeSeries sparse = new TimeSeries("1", null, buckets, new Double[]{0.0, null, Double.NaN}, tags, labels);
    TimeSeries dense = new TimeSeries("1", null, buckets, new Double[]{1.0, 2.0, 3.0}, tags, labels);
    TimeSeriesBlock block = new TimeSeriesBlock(buckets, Map.of(1L, List.of(empty, sparse, dense)));

    String response = PinotBrokerTimeSeriesResponse.fromTimeSeriesBlock(block).serialize();

    String expected = "{\"status\":\"success\",\"data\":{\"resultType\":\"matrix\",\"result\":["
        + "{\"metric\":{\"__name__\":\"requests\"},\"values\":[[10,\"0.0\"],[30,\"NaN\"]]},"
        + "{\"metric\":{\"__name__\":\"requests\"},\"values\":[[10,\"1.0\"],[20,\"2.0\"],[30,\"3.0\"]]}]}}";
    assertEquals(JsonUtils.stringToJsonNode(response), JsonUtils.stringToJsonNode(expected));
  }
}
