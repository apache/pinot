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

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import org.apache.pinot.common.utils.SimpleHttpErrorInfo;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;


public class BrokerCommonExceptionMapperTest {

  private BrokerCommonExceptionMapper _exceptionMapper;

  @Mock
  private WebApplicationException _webApplicationException;

  @BeforeMethod
  public void setUp() {
    MockitoAnnotations.openMocks(this);
    _exceptionMapper = new BrokerCommonExceptionMapper();
  }

  @Test
  public void testToResponseWithWebApplicationException() {
    // Arrange
    int status = 404;
    when(_webApplicationException.getResponse()).thenReturn(Response.status(status).build());
    when(_webApplicationException.getMessage()).thenReturn("Not Found");

    // Act
    Response response = _exceptionMapper.toResponse(_webApplicationException);

    // Assert
    assertEquals(response.getStatus(), status);
    SimpleHttpErrorInfo errorInfo = (SimpleHttpErrorInfo) response.getEntity();
    assertEquals(errorInfo.getCode(), status);
    assertEquals(errorInfo.getError(), "Not Found");
  }

  @Test
  public void testToResponseWithGenericException() {
    // Arrange
    Throwable throwable = new RuntimeException("Internal Server Error");

    // Act
    Response response = _exceptionMapper.toResponse(throwable);

    // Assert
    assertEquals(response.getStatus(), 500);
    SimpleHttpErrorInfo errorInfo = (SimpleHttpErrorInfo) response.getEntity();
    assertEquals(errorInfo.getCode(), 500);
    assertEquals(errorInfo.getError(), "Internal Server Error");
  }
}
