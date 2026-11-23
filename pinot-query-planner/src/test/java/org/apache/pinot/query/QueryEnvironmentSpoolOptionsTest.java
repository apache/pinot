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
package org.apache.pinot.query;

import java.util.Map;
import org.apache.pinot.spi.exception.QueryErrorCode;
import org.apache.pinot.spi.exception.QueryException;
import org.apache.pinot.spi.utils.CommonConstants.Broker.Request.QueryOptionKey;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


/// Tests how [QueryEnvironment] reads the spool query options.
public class QueryEnvironmentSpoolOptionsTest {

  @Test
  public void spoolsFeedbackAcceptsBooleans() {
    assertFalse(QueryEnvironment.isSpoolsFeedback(Map.of()));
    assertTrue(QueryEnvironment.isSpoolsFeedback(Map.of(QueryOptionKey.SPOOLS_FEEDBACK, "true")));
    assertFalse(QueryEnvironment.isSpoolsFeedback(Map.of(QueryOptionKey.SPOOLS_FEEDBACK, "false")));
  }

  @Test
  public void invalidSpoolsFeedbackIsAValidationError() {
    QueryException e = expectThrows(QueryException.class,
        () -> QueryEnvironment.isSpoolsFeedback(Map.of(QueryOptionKey.SPOOLS_FEEDBACK, "yes")));
    assertEquals(e.getErrorCode(), QueryErrorCode.QUERY_VALIDATION);
  }
}
