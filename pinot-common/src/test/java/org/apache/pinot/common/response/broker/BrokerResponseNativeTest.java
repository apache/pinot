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
package org.apache.pinot.common.response.broker;

import java.io.IOException;
import org.apache.pinot.common.exception.QueryException;
import org.apache.pinot.spi.exception.QException;
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
    Assert.assertEquals(actual.getResultTable(), expected.getResultTable());
  }

  @Test
  public void testNullResponse()
      throws IOException {
    BrokerResponseNative expected = BrokerResponseNative.NO_TABLE_RESULT;
    String brokerString = expected.toJsonString();
    BrokerResponseNative actual = BrokerResponseNative.fromJsonString(brokerString);
    Assert.assertEquals(actual.getExceptions().get(0).getErrorCode(), QException.BROKER_RESOURCE_MISSING_ERROR_CODE);
  }

  @Test
  public void testMultipleExceptionsResponse()
      throws IOException {
    BrokerResponseNative expected = BrokerResponseNative.NO_TABLE_RESULT;
    String errorMsgStr = "Some random string!";
    expected.addException(new QueryProcessingException(400, errorMsgStr));
    String brokerString = expected.toJsonString();
    BrokerResponseNative newBrokerResponse = BrokerResponseNative.fromJsonString(brokerString);
    Assert.assertEquals(newBrokerResponse.getExceptions().get(0).getErrorCode(),
        QException.BROKER_RESOURCE_MISSING_ERROR_CODE);
    Assert.assertEquals(newBrokerResponse.getExceptions().get(1).getErrorCode(), 400);
    Assert.assertEquals(newBrokerResponse.getExceptions().get(1).getMessage(), errorMsgStr);
  }
}
