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
package org.apache.pinot.spi.exception;

import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;

import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;


/// Tests that every [QueryErrorCode] has its own id, so that [QueryErrorCode#fromErrorCode(int)] decodes each id back
/// to the constant that sent it.
public class QueryErrorCodeTest {

  @Test
  public void testIdsAreUnique() {
    Map<Integer, QueryErrorCode> codesById = new HashMap<>();
    for (QueryErrorCode code : QueryErrorCode.values()) {
      QueryErrorCode previous = codesById.put(code.getId(), code);
      assertNull(previous, "Error code id " + code.getId() + " is shared by " + previous + " and " + code);
    }
  }

  @Test
  public void testFromErrorCodeReturnsConstantWithThatId() {
    for (QueryErrorCode code : QueryErrorCode.values()) {
      assertSame(QueryErrorCode.fromErrorCode(code.getId()), code);
    }
  }

  @Test
  public void testTooManyRequestsDecodesAsClientError() {
    QueryErrorCode code = QueryErrorCode.fromErrorCode(429);
    assertSame(code, QueryErrorCode.TOO_MANY_REQUESTS);
    assertTrue(code.isClientError());
  }
}
