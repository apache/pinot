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
package org.apache.pinot.query.service.dispatch;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.pinot.common.response.StreamingBrokerResponse;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.runtime.operator.MultiStageOperator;
import org.apache.pinot.query.runtime.operator.OpChain;
import org.apache.pinot.query.runtime.operator.OperatorTestUtil;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Unit tests for {@link LazyBrokerResponse}.
 *
 * <p>Constructs lightweight {@link LazyBrokerResponse} instances backed by mock operators (via
 * {@link OperatorTestUtil}) to avoid heavy multi-stage infrastructure.
 */
public class LazyBrokerResponseTest {

  private static final String OP = OperatorTestUtil.OP_1;
  private static final DataSchema SCHEMA = OperatorTestUtil.SIMPLE_KV_DATA_SCHEMA;

  /**
   * Task 5: Verify that a successful {@link LazyBrokerResponse#consumeData} call produces a metainfo
   * whose {@link StreamingBrokerResponse.Metainfo#asJson()} contains a {@code brokerReduceTimeMs} field
   * with a non-negative value.
   */
  @Test
  public void testSuccessfulConsumeMetainfoContainsBrokerReduceTimeMs()
      throws InterruptedException {
    MultiStageOperator operator = OperatorTestUtil.getOperator(OP);
    OpChain opChain = new OpChain(OperatorTestUtil.getNoTracingContext(), operator);

    try (LazyBrokerResponse response = new LazyBrokerResponse(SCHEMA, opChain)) {
      StreamingBrokerResponse.Metainfo metainfo = response.consumeData(data -> { });

      Assert.assertTrue(metainfo.getExceptions().isEmpty(),
          "Expected no exceptions in a successful response, got: " + metainfo.getExceptions());

      ObjectNode json = metainfo.asJson();
      Assert.assertTrue(json.has("brokerReduceTimeMs"),
          "Metainfo JSON must contain 'brokerReduceTimeMs', got: " + json);

      long brokerReduceTimeMs = json.get("brokerReduceTimeMs").asLong(-1);
      Assert.assertTrue(brokerReduceTimeMs >= 0,
          "brokerReduceTimeMs must be >= 0, got: " + brokerReduceTimeMs);
    }
  }

  /**
   * Verify that the {@code brokerReduceTimeMs} field is a plausible wall-clock measurement — it must
   * not exceed a generous upper bound (5 seconds), ruling out uninitialised / overflow values.
   */
  @Test
  public void testBrokerReduceTimeMsIsWithinReasonableBounds()
      throws InterruptedException {
    MultiStageOperator operator = OperatorTestUtil.getOperator(OP);
    OpChain opChain = new OpChain(OperatorTestUtil.getNoTracingContext(), operator);

    try (LazyBrokerResponse response = new LazyBrokerResponse(SCHEMA, opChain)) {
      StreamingBrokerResponse.Metainfo metainfo = response.consumeData(data -> { });
      long brokerReduceTimeMs = metainfo.asJson().get("brokerReduceTimeMs").asLong(-1);

      Assert.assertTrue(brokerReduceTimeMs >= 0 && brokerReduceTimeMs < 5_000,
          "brokerReduceTimeMs should be a plausible wall-clock measurement (0–5000 ms), got: " + brokerReduceTimeMs);
    }
  }
}
