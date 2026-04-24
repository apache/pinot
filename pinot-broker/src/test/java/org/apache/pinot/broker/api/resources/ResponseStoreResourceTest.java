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
import javax.ws.rs.core.HttpHeaders;
import org.apache.pinot.common.cursors.AbstractResponseStore;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.longThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;


public class ResponseStoreResourceTest {

  private AutoCloseable _mocks;

  @Mock
  private AbstractResponseStore _responseStore;

  @Mock
  private HttpHeaders _httpHeaders;

  @InjectMocks
  private ResponseStoreResource _resource;

  @BeforeMethod
  public void setUp() {
    _mocks = MockitoAnnotations.openMocks(this);
  }

  @AfterMethod(alwaysRun = true)
  public void tearDown()
      throws Exception {
    _mocks.close();
  }

  @Test
  public void testDeleteExpiredResponsesSuccess()
      throws Exception {
    long cutoff = 1000000L;
    when(_responseStore.deleteExpiredResponses(cutoff)).thenReturn(3);

    String result = _resource.deleteExpiredResponses(cutoff, _httpHeaders);

    assertEquals(result, "Deleted 3 expired response(s).");
  }

  @Test
  public void testDeleteExpiredResponsesReturnsZeroCount()
      throws Exception {
    long cutoff = 1000000L;
    when(_responseStore.deleteExpiredResponses(cutoff)).thenReturn(0);

    String result = _resource.deleteExpiredResponses(cutoff, _httpHeaders);

    assertEquals(result, "Deleted 0 expired response(s).");
  }

  @Test
  public void testDeleteExpiredResponsesReturns500OnException()
      throws Exception {
    long cutoff = 1000000L;
    when(_responseStore.deleteExpiredResponses(cutoff)).thenThrow(new RuntimeException("FS failure"));

    try {
      _resource.deleteExpiredResponses(cutoff, _httpHeaders);
      fail("Expected WebApplicationException on store failure");
    } catch (WebApplicationException e) {
      assertEquals(e.getResponse().getStatus(), 500);
    }
  }

  @Test
  public void testDeleteExpiredResponsesDefaultsToCurrentTimeWhenParamMissing()
      throws Exception {
    when(_responseStore.deleteExpiredResponses(anyLong())).thenReturn(0);
    String result = _resource.deleteExpiredResponses(null, _httpHeaders);
    assertEquals(result, "Deleted 0 expired response(s).");
    // Verify deleteExpiredResponses was called with a timestamp close to now
    verify(_responseStore).deleteExpiredResponses(longThat(ts -> Math.abs(ts - System.currentTimeMillis()) < 5000));
  }
}
