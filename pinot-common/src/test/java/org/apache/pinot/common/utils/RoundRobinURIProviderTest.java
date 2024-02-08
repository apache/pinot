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
package org.apache.pinot.common.utils;

import com.google.common.net.InetAddresses;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


public class RoundRobinURIProviderTest {

  @Test
  public void testHostAddressRoundRobin()
      throws URISyntaxException, UnknownHostException {

    InetAddress[] testWebAddresses = new InetAddress[]{
        InetAddress.getByAddress("testweb.com", InetAddresses.forString("192.168.3.1").getAddress()),
        InetAddress.getByAddress("testweb.com", InetAddresses.forString("192.168.3.2").getAddress()),
        InetAddress.getByAddress("testweb.com", InetAddresses.forString("192.168.3.3").getAddress())
    };
    InetAddress[] localHostAddresses = new InetAddress[]{
        InetAddress.getByAddress("localhost", InetAddresses.forString("127.0.0.1").getAddress()),
        InetAddress.getByAddress("localhost", InetAddresses.forString("0:0:0:0:0:0:0:1").getAddress())
    };

    MockedStatic<InetAddress> mock = Mockito.mockStatic(InetAddress.class);
    mock.when(() -> InetAddress.getAllByName("localhost")).thenReturn(localHostAddresses);
    mock.when(() -> InetAddress.getAllByName("testweb.com")).thenReturn(testWebAddresses);

    TestCase[] testCases = new TestCase[]{
        new TestCase("http://127.0.0.1", Collections.singletonList("http://127.0.0.1")),
        new TestCase("http://127.0.0.1/", Collections.singletonList("http://127.0.0.1/")),
        new TestCase("http://127.0.0.1/?", Collections.singletonList("http://127.0.0.1/?")),
        new TestCase("http://127.0.0.1/?it=5", Collections.singletonList("http://127.0.0.1/?it=5")),
        new TestCase("http://127.0.0.1/me/out?it=5", Collections.singletonList("http://127.0.0.1/me/out?it=5")),
        new TestCase("http://127.0.0.1:20000", Collections.singletonList("http://127.0.0.1:20000")),
        new TestCase("http://127.0.0.1:20000/", Collections.singletonList("http://127.0.0.1:20000/")),
        new TestCase("http://127.0.0.1:20000/?", Collections.singletonList("http://127.0.0.1:20000/?")),
        new TestCase("http://127.0.0.1:20000/?it=5", Collections.singletonList("http://127.0.0.1:20000/?it=5")),
        new TestCase("http://127.0.0.1:20000/me/out?it=5",
            Collections.singletonList("http://127.0.0.1:20000/me/out?it=5")),

        new TestCase("http://localhost", Arrays.asList("http://127.0.0.1", "http://[0:0:0:0:0:0:0:1]")),
        new TestCase("http://localhost/", Arrays.asList("http://127.0.0.1/", "http://[0:0:0:0:0:0:0:1]/")),
        new TestCase("http://localhost/?", Arrays.asList("http://127.0.0.1/?", "http://[0:0:0:0:0:0:0:1]/?")),
        new TestCase("http://localhost/?it=5",
            Arrays.asList("http://127.0.0.1/?it=5", "http://[0:0:0:0:0:0:0:1]/?it=5")),
        new TestCase("http://localhost/me/out?it=5",
            Arrays.asList("http://127.0.0.1/me/out?it=5", "http://[0:0:0:0:0:0:0:1]/me/out?it=5")),
        new TestCase("http://localhost:20000",
            Arrays.asList("http://127.0.0.1:20000", "http://[0:0:0:0:0:0:0:1]:20000")),
        new TestCase("http://localhost:20000/",
            Arrays.asList("http://127.0.0.1:20000/", "http://[0:0:0:0:0:0:0:1]:20000/")),
        new TestCase("http://localhost:20000/?",
            Arrays.asList("http://127.0.0.1:20000/?", "http://[0:0:0:0:0:0:0:1]:20000/?")),
        new TestCase("http://localhost:20000/?it=5",
            Arrays.asList("http://127.0.0.1:20000/?it=5", "http://[0:0:0:0:0:0:0:1]:20000/?it=5")),
        new TestCase("http://localhost:20000/me/out?it=5",
            Arrays.asList("http://127.0.0.1:20000/me/out?it=5", "http://[0:0:0:0:0:0:0:1]:20000/me/out?it=5")),

        new TestCase("http://testweb.com",
            Arrays.asList("http://192.168.3.1", "http://192.168.3.2", "http://192.168.3.3")),
        new TestCase("http://testweb.com/",
            Arrays.asList("http://192.168.3.1/", "http://192.168.3.2/", "http://192.168.3.3/")),
        new TestCase("http://testweb.com/?",
            Arrays.asList("http://192.168.3.1/?", "http://192.168.3.2/?", "http://192.168.3.3/?")),
        new TestCase("http://testweb.com/?it=5",
            Arrays.asList("http://192.168.3.1/?it=5", "http://192.168.3.2/?it=5", "http://192.168.3.3/?it=5")),
        new TestCase("http://testweb.com/me/out?it=5",
            Arrays.asList("http://192.168.3.1/me/out?it=5", "http://192.168.3.2/me/out?it=5",
                "http://192.168.3.3/me/out?it=5")),
        new TestCase("http://testweb.com:20000",
            Arrays.asList("http://192.168.3.1:20000", "http://192.168.3.2:20000", "http://192.168.3.3:20000")),
        new TestCase("http://testweb.com:20000/",
            Arrays.asList("http://192.168.3.1:20000/", "http://192.168.3.2:20000/", "http://192.168.3.3:20000/")),
        new TestCase("http://testweb.com:20000/?",
            Arrays.asList("http://192.168.3.1:20000/?", "http://192.168.3.2:20000/?", "http://192.168.3.3:20000/?")),
        new TestCase("http://testweb.com:20000/?it=5",
            Arrays.asList("http://192.168.3.1:20000/?it=5", "http://192.168.3.2:20000/?it=5",
                "http://192.168.3.3:20000/?it=5")),
        new TestCase("http://testweb.com:20000/me/out?it=5",
            Arrays.asList("http://192.168.3.1:20000/me/out?it=5", "http://192.168.3.2:20000/me/out?it=5",
                "http://192.168.3.3:20000/me/out?it=5")),

        new TestCase("https://127.0.0.1", Collections.singletonList("https://127.0.0.1")),
        new TestCase("https://127.0.0.1/", Collections.singletonList("https://127.0.0.1/")),
        new TestCase("https://127.0.0.1/?", Collections.singletonList("https://127.0.0.1/?")),
        new TestCase("https://127.0.0.1/?it=5", Collections.singletonList("https://127.0.0.1/?it=5")),
        new TestCase("https://127.0.0.1/me/out?it=5", Collections.singletonList("https://127.0.0.1/me/out?it=5")),
        new TestCase("https://127.0.0.1:20000", Collections.singletonList("https://127.0.0.1:20000")),
        new TestCase("https://127.0.0.1:20000/", Collections.singletonList("https://127.0.0.1:20000/")),
        new TestCase("https://127.0.0.1:20000/?", Collections.singletonList("https://127.0.0.1:20000/?")),
        new TestCase("https://127.0.0.1:20000/?it=5", Collections.singletonList("https://127.0.0.1:20000/?it=5")),
        new TestCase("https://127.0.0.1:20000/me/out?it=5",
            Collections.singletonList("https://127.0.0.1:20000/me/out?it=5")),

        new TestCase("https://localhost", Arrays.asList("https://127.0.0.1", "https://[0:0:0:0:0:0:0:1]")),
        new TestCase("https://localhost/", Arrays.asList("https://127.0.0.1/", "https://[0:0:0:0:0:0:0:1]/")),
        new TestCase("https://localhost/?", Arrays.asList("https://127.0.0.1/?", "https://[0:0:0:0:0:0:0:1]/?")),
        new TestCase("https://localhost/?it=5",
            Arrays.asList("https://127.0.0.1/?it=5", "https://[0:0:0:0:0:0:0:1]/?it=5")),
        new TestCase("https://localhost/me/out?it=5",
            Arrays.asList("https://127.0.0.1/me/out?it=5", "https://[0:0:0:0:0:0:0:1]/me/out?it=5")),
        new TestCase("https://localhost:20000",
            Arrays.asList("https://127.0.0.1:20000", "https://[0:0:0:0:0:0:0:1]:20000")),
        new TestCase("https://localhost:20000/",
            Arrays.asList("https://127.0.0.1:20000/", "https://[0:0:0:0:0:0:0:1]:20000/")),
        new TestCase("https://localhost:20000/?",
            Arrays.asList("https://127.0.0.1:20000/?", "https://[0:0:0:0:0:0:0:1]:20000/?")),
        new TestCase("https://localhost:20000/?it=5",
            Arrays.asList("https://127.0.0.1:20000/?it=5", "https://[0:0:0:0:0:0:0:1]:20000/?it=5")),

        new TestCase("https://testweb.com",
            Arrays.asList("https://192.168.3.1", "https://192.168.3.2", "https://192.168.3.3")),
        new TestCase("https://testweb.com/",
            Arrays.asList("https://192.168.3.1/", "https://192.168.3.2/", "https://192.168.3.3/")),
        new TestCase("https://testweb.com/?",
            Arrays.asList("https://192.168.3.1/?", "https://192.168.3.2/?", "https://192.168.3.3/?")),
        new TestCase("https://testweb.com/?it=5",
            Arrays.asList("https://192.168.3.1/?it=5", "https://192.168.3.2/?it=5", "https://192.168.3.3/?it=5")),
        new TestCase("https://testweb.com/me/out?it=5",
            Arrays.asList("https://192.168.3.1/me/out?it=5", "https://192.168.3.2/me/out?it=5",
                "https://192.168.3.3/me/out?it=5")),
        new TestCase("https://testweb.com:20000",
            Arrays.asList("https://192.168.3.1:20000", "https://192.168.3.2:20000", "https://192.168.3.3:20000")),
        new TestCase("https://testweb.com:20000/",
            Arrays.asList("https://192.168.3.1:20000/", "https://192.168.3.2:20000/", "https://192.168.3.3:20000/")),
        new TestCase("https://testweb.com:20000/?",
            Arrays.asList("https://192.168.3.1:20000/?", "https://192.168.3.2:20000/?", "https://192.168.3.3:20000/?")),
        new TestCase("https://testweb.com:20000/?it=5",
            Arrays.asList("https://192.168.3.1:20000/?it=5", "https://192.168.3.2:20000/?it=5",
                "https://192.168.3.3:20000/?it=5")),
        new TestCase("https://testweb.com:20000/me/out?it=5",
            Arrays.asList("https://192.168.3.1:20000/me/out?it=5", "https://192.168.3.2:20000/me/out?it=5",
                "https://192.168.3.3:20000/me/out?it=5")),
    };

    for (TestCase testCase : testCases) {
      String uri = testCase._originalUri;
      RoundRobinURIProvider uriProvider = new RoundRobinURIProvider(List.of(new URI(uri)), true);
      int n = testCase._expectedUris.size();
      int previousIndex = -1;
      int currentIndex;
      for (int i = 0; i < 2 * n; i++) {
        String actualUri = uriProvider.next().toString();
        currentIndex = testCase._expectedUris.indexOf(actualUri);
        Assert.assertTrue(currentIndex != -1);
        if (previousIndex != -1) {
          Assert.assertEquals((previousIndex + 1) % n, currentIndex);
        }
        previousIndex = currentIndex;
      }
    }
  }

  static class TestCase {
    String _originalUri;
    List<String> _expectedUris;

    TestCase(String originalUri, List<String> expectedUris) {
      _originalUri = originalUri;
      _expectedUris = expectedUris;
    }
  }
}
