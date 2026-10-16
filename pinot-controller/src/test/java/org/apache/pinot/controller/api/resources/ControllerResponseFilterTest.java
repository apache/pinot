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
package org.apache.pinot.controller.api.resources;

import java.net.URI;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class ControllerResponseFilterTest {
  private static final String BASE = "https://controller:9000/tables/myTable";
  private static final String SECRET = "FAKE-SECRET-VALUE";

  @Test
  public void testMasksCredentialQueryParametersAndKeepsTheRest() {
    String logged = redact(BASE + "?type=OFFLINE&accessToken=" + SECRET + "&password=" + SECRET + "&verbose=true");

    assertEquals(logged, BASE + "?type=OFFLINE&accessToken=*****&password=*****&verbose=true");
  }

  @Test
  public void testJudgesTheDecodedName() {
    // "access%54oken" is "accessToken" once decoded, so encoding the name must not get it past the mask.
    assertEquals(redact(BASE + "?access%54oken=" + SECRET), BASE + "?accessToken=*****");
  }

  @Test
  public void testKeepsNonSensitiveValuesAsSent() {
    String query = "?segmentName=seg%200&segmentName=seg_1&filter=a%3Db";

    assertEquals(redact(BASE + query), BASE + query);
  }

  @Test
  public void testRepeatedSensitiveParameterCollapsesToOneMask() {
    // Hides how many values were sent, too.
    assertEquals(redact(BASE + "?token=" + SECRET + "&token=" + SECRET), BASE + "?token=*****");
  }

  @Test
  public void testParameterWithoutValue() {
    assertEquals(redact(BASE + "?verbose&password=" + SECRET), BASE + "?verbose=&password=*****");
  }

  @Test
  public void testUriWithoutQueryIsUnchanged() {
    assertEquals(redact(BASE), BASE);
    assertEquals(redact(BASE + "?"), BASE + "?");
  }

  private static String redact(String requestUri) {
    return ControllerResponseFilter.redactQueryParameters(URI.create(requestUri));
  }
}
