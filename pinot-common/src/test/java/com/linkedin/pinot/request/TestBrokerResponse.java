package com.linkedin.pinot.request;

import org.json.JSONException;
import org.json.JSONObject;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.pinot.common.exception.QueryException;
import com.linkedin.pinot.common.response.BrokerResponse;
import com.linkedin.pinot.common.response.ProcessingException;


public class TestBrokerResponse {

  @Test
  public void testEmptyResponse() throws JSONException {
    BrokerResponse brokerResponse = BrokerResponse.getEmptyBrokerResponse();
    String brokerString = brokerResponse.toJson().toString();
    BrokerResponse newBrokerResponse = BrokerResponse.fromJson(new JSONObject(brokerString));
    System.out.println(newBrokerResponse);
  }

  @Test
  public void testNullResponse() throws JSONException {
    BrokerResponse brokerResponse = BrokerResponse.getNullBrokerResponse();
    String brokerString = brokerResponse.toJson().toString();
    BrokerResponse newBrokerResponse = BrokerResponse.fromJson(new JSONObject(brokerString));
    System.out.println(newBrokerResponse);
    System.out.println(newBrokerResponse.getExceptions().get(0));
    Assert.assertEquals(newBrokerResponse.getExceptions().get(0).getErrorCode(), QueryException.BROKER_RESOURCE_MISSING_ERROR.getErrorCode());
    Assert.assertEquals(newBrokerResponse.getExceptions().get(0).getMessage(), "No resource hit!");
  }

  @Test
  public void testMultipleExceptionsResponse() throws JSONException {
    BrokerResponse brokerResponse = BrokerResponse.getNullBrokerResponse();
    ProcessingException processingException = new ProcessingException(400);
    String errorMsgStr = "Some random string!";
    processingException.setMessage(errorMsgStr);
    brokerResponse.addToExceptions(processingException);
    String brokerString = brokerResponse.toJson().toString();
    BrokerResponse newBrokerResponse = BrokerResponse.fromJson(new JSONObject(brokerString));
    System.out.println(newBrokerResponse);
    System.out.println(newBrokerResponse.getExceptions().get(0));
    Assert.assertEquals(newBrokerResponse.getExceptions().get(0).getErrorCode(), QueryException.BROKER_RESOURCE_MISSING_ERROR.getErrorCode());
    Assert.assertEquals(newBrokerResponse.getExceptions().get(0).getMessage(), "No resource hit!");
    System.out.println(newBrokerResponse.getExceptions().get(1));
    Assert.assertEquals(newBrokerResponse.getExceptions().get(1).getErrorCode(), 400);
    Assert.assertEquals(newBrokerResponse.getExceptions().get(1).getMessage(), errorMsgStr);
  }
}
