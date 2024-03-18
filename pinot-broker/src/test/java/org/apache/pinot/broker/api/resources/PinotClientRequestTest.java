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
package org.apache.pinot.broker.api.resources;

import javax.ws.rs.core.Response;
import org.apache.pinot.common.response.BrokerResponse;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.apache.pinot.common.exception.QueryException.TABLE_DOES_NOT_EXIST_ERROR_CODE;
import static org.apache.pinot.spi.utils.CommonConstants.Controller.PINOT_QUERY_ERROR_CODE_HEADER;


public class PinotClientRequestTest {

  @Test
  public void testGetPinotQueryResponse()
      throws Exception {

    // for successful query result the 'X-Pinot-Error-Code' should be -1
    BrokerResponse emptyResultBrokerResponse = BrokerResponseNative.EMPTY_RESULT;
    Response successfulResponse = PinotClientRequest.getPinotQueryResponse(emptyResultBrokerResponse);
    Assert.assertEquals(successfulResponse.getStatus(), Response.Status.OK.getStatusCode());
    Assert.assertTrue(successfulResponse.getHeaders().containsKey(PINOT_QUERY_ERROR_CODE_HEADER));
    Assert.assertEquals(successfulResponse.getHeaders().get(PINOT_QUERY_ERROR_CODE_HEADER).size(), 1);
    Assert.assertEquals(successfulResponse.getHeaders().get(PINOT_QUERY_ERROR_CODE_HEADER).get(0), -1);

    // for failed query result the 'X-Pinot-Error-Code' should be Error code fo exception.
    BrokerResponse tableDoesNotExistBrokerResponse = BrokerResponseNative.TABLE_DOES_NOT_EXIST;
    Response tableDoesNotExistResponse = PinotClientRequest.getPinotQueryResponse(tableDoesNotExistBrokerResponse);
    Assert.assertEquals(tableDoesNotExistResponse.getStatus(), Response.Status.OK.getStatusCode());
    Assert.assertTrue(tableDoesNotExistResponse.getHeaders().containsKey(PINOT_QUERY_ERROR_CODE_HEADER));
    Assert.assertEquals(tableDoesNotExistResponse.getHeaders().get(PINOT_QUERY_ERROR_CODE_HEADER).size(), 1);
    Assert.assertEquals(tableDoesNotExistResponse.getHeaders().get(PINOT_QUERY_ERROR_CODE_HEADER).get(0),
        TABLE_DOES_NOT_EXIST_ERROR_CODE);
  }
}
