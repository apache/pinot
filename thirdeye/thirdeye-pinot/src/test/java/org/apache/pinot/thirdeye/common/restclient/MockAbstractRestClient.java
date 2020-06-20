/*
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

package org.apache.pinot.thirdeye.common.restclient;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.nio.charset.Charset;
import java.util.concurrent.atomic.AtomicInteger;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.Assert;


public class MockAbstractRestClient {
  static AbstractRestClient.HttpURLConnectionFactory setupMock(final String expectedUrl, final String response,
      HttpURLConnection mockConn) throws Exception {
    return setupMock(expectedUrl, 200, response, mockConn);
  }

  static AbstractRestClient.HttpURLConnectionFactory setupMock(final String expectedUrl, final int responseCode,
      final String response, HttpURLConnection mockConn) throws Exception {
    AbstractRestClient.HttpURLConnectionFactory connectionFactory
        = Mockito.mock(AbstractRestClient.HttpURLConnectionFactory.class);
    final AtomicInteger callCount = new AtomicInteger(0);
    Mockito.when(connectionFactory.openConnection(Mockito.anyString())).thenAnswer(new Answer<HttpURLConnection>() {
      @Override
      public HttpURLConnection answer(InvocationOnMock invocation) throws Throwable {
        Assert.assertEquals(callCount.incrementAndGet(), 1,
            "HTTP server called more than once.");
        Assert.assertNotNull(invocation.getArguments(),
            "Bad arguments to openConnection (getArguments() returned null)");
        Assert.assertEquals(invocation.getArguments().length, 1,
            "Bad arguments to openConnection. Expected exactly one argument.");
        Assert.assertEquals(invocation.getArguments()[0], expectedUrl,
            "Incorrect URL in the HTTP request.");
        HttpURLConnection conn;

        if(mockConn == null) {
          conn = Mockito.mock(HttpURLConnection.class);
          Mockito.when(conn.getOutputStream()).thenReturn(new OutputStream() {
            @Override
            public void write(int b) throws IOException {
              // This is a mock implementation. Write to nowhere.
            }
          });
        }
        else {
          conn = mockConn;
        }

        Mockito.when(conn.getResponseCode()).thenReturn(responseCode);
        if (responseCode >= 200 && responseCode <= 299) {
          Mockito.when(conn.getInputStream())
              .thenReturn(new ByteArrayInputStream(response.getBytes(Charset.forName("UTF-8"))));
        } else {
          Mockito.when(conn.getErrorStream())
              .thenReturn(new ByteArrayInputStream(response.getBytes(Charset.forName("UTF-8"))));
        }

        return conn;
      }
    });

    return connectionFactory;
  }
}