/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.request;

import org.json.JSONException;
import org.json.JSONObject;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.pinot.common.exception.QueryException;
import com.linkedin.pinot.common.response.BrokerResponseJSON;
import com.linkedin.pinot.common.response.ProcessingException;


public class BrokerResponseTest {

  @Test
  public void testEmptyResponse() throws JSONException {
    BrokerResponseJSON brokerResponse = BrokerResponseJSON.getEmptyBrokerResponse();
    String brokerString = brokerResponse.toJson().toString();
    BrokerResponseJSON newBrokerResponse = BrokerResponseJSON.fromJson(new JSONObject(brokerString));
    System.out.println(newBrokerResponse);
  }

  @Test
  public void testNullResponse() throws JSONException {
    BrokerResponseJSON brokerResponse = BrokerResponseJSON.getNullBrokerResponse();
    String brokerString = brokerResponse.toJson().toString();
    BrokerResponseJSON newBrokerResponse = BrokerResponseJSON.fromJson(new JSONObject(brokerString));
    System.out.println(newBrokerResponse);
    System.out.println(newBrokerResponse.getExceptions().get(0));
    Assert.assertEquals(newBrokerResponse.getExceptions().get(0).getErrorCode(), QueryException.BROKER_RESOURCE_MISSING_ERROR.getErrorCode());
    Assert.assertEquals(newBrokerResponse.getExceptions().get(0).getMessage(), "No table hit!");
  }

  @Test
  public void testMultipleExceptionsResponse() throws JSONException {
    BrokerResponseJSON brokerResponse = BrokerResponseJSON.getNullBrokerResponse();
    ProcessingException processingException = new ProcessingException(400);
    String errorMsgStr = "Some random string!";
    processingException.setMessage(errorMsgStr);
    brokerResponse.addToExceptions(processingException);
    String brokerString = brokerResponse.toJson().toString();
    BrokerResponseJSON newBrokerResponse = BrokerResponseJSON.fromJson(new JSONObject(brokerString));
    System.out.println(newBrokerResponse);
    System.out.println(newBrokerResponse.getExceptions().get(0));
    Assert.assertEquals(newBrokerResponse.getExceptions().get(0).getErrorCode(), QueryException.BROKER_RESOURCE_MISSING_ERROR.getErrorCode());
    Assert.assertEquals(newBrokerResponse.getExceptions().get(0).getMessage(), "No table hit!");
    System.out.println(newBrokerResponse.getExceptions().get(1));
    Assert.assertEquals(newBrokerResponse.getExceptions().get(1).getErrorCode(), 400);
    Assert.assertEquals(newBrokerResponse.getExceptions().get(1).getMessage(), errorMsgStr);
  }
}
