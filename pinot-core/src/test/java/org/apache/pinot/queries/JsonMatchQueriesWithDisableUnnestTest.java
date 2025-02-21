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
package org.apache.pinot.queries;

import java.util.Set;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


// same as JsonMatchQueriesTest but with array un-nesting disabled
public class JsonMatchQueriesWithDisableUnnestTest extends JsonMatchQueriesTest {

  @Override
  protected boolean isDisableCrossArrayUnnest() {
    return true;
  }

  @Test
  public void testQueriesOnNestedArrays() {
    // Top-level object with multiple nested-array values
    // Searching one more than one nested arrays work when 'disableCrossArrayUnnest' is false (default)
    assertEquals(getSelectedIds("'\"$.key[*][*][*]\"=true AND \"$.key2[1][0]\"=''bar'''"), Set.of());
    assertEquals(getSelectedIds("'\"$.key[0]\"=1 AND \"$.key2[0]\"=2'"), Set.of());
  }
}
