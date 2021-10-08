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
package org.apache.pinot.request;

import java.io.IOException;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.QueryProcessingException;
import org.testng.Assert;
import org.testng.annotations.Test;


public class BrokerResponseNativeTest {

  @Test
  public void testEmptyResponse()
      throws IOException {
    BrokerResponseNative expected = BrokerResponseNative.EMPTY_RESULT;
    String brokerString = expected.toJsonString();
    BrokerResponseNative actual = BrokerResponseNative.fromJsonString(brokerString);
    Assert.assertEquals(actual.getNumDocsScanned(), expected.getNumDocsScanned());
    Assert.assertEquals(actual.getTimeUsedMs(), expected.getTimeUsedMs());
    Assert.assertEquals(actual.getAggregationResults(), expected.getAggregationResults());
    Assert.assertEquals(actual.getSegmentStatistics().size(), expected.getSegmentStatistics().size());
  }

  @Test
  public void testNullResponse()
      throws IOException {
    BrokerResponseNative expected = BrokerResponseNative.NO_TABLE_RESULT;
    String brokerString = expected.toJsonString();
    BrokerResponseNative actual = BrokerResponseNative.fromJsonString(brokerString);
    Assert.assertEquals(actual.getProcessingExceptions().get(0).getErrorCode(),
        QueryException.BROKER_RESOURCE_MISSING_ERROR.getErrorCode());
    Assert.assertEquals(actual.getProcessingExceptions().get(0).getMessage(),
        QueryException.BROKER_RESOURCE_MISSING_ERROR.getMessage());
  }

  @Test
  public void testMultipleExceptionsResponse()
      throws IOException {
    BrokerResponseNative expected = BrokerResponseNative.NO_TABLE_RESULT;
    String errorMsgStr = "Some random string!";
    QueryProcessingException processingException = new QueryProcessingException(400, errorMsgStr);
    expected.addToExceptions(processingException);
    String brokerString = expected.toJsonString();
    BrokerResponseNative newBrokerResponse = BrokerResponseNative.fromJsonString(brokerString);
    Assert.assertEquals(newBrokerResponse.getProcessingExceptions().get(0).getErrorCode(),
        QueryException.BROKER_RESOURCE_MISSING_ERROR.getErrorCode());
    Assert.assertEquals(newBrokerResponse.getProcessingExceptions().get(0).getMessage(),
        QueryException.BROKER_RESOURCE_MISSING_ERROR.getMessage());
    Assert.assertEquals(newBrokerResponse.getProcessingExceptions().get(1).getErrorCode(), 400);
    Assert.assertEquals(newBrokerResponse.getProcessingExceptions().get(1).getMessage(), errorMsgStr);
  }
}
