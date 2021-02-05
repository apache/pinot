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

package org.apache.pinot.controller.api.access;

import java.util.Optional;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class AuthenticationFilterTest {
  private final AuthenticationFilter _authFilter = new AuthenticationFilter();

  @Test
  public void testAuthFilter_extractTableName_withTableNameInPathParams() {
    MultivaluedMap<String, String> pathParams = new MultivaluedHashMap<>();
    MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<>();
    pathParams.putSingle("tableName", "A");
    pathParams.putSingle("tableNameWithType", "B");
    pathParams.putSingle("schemaName", "C");
    queryParams.putSingle("tableName", "D");
    queryParams.putSingle("tableNameWithType", "E");
    queryParams.putSingle("schemaName", "F");
    Optional<String> actual = _authFilter.extractTableName(pathParams, queryParams);
    assertEquals(actual, Optional.of("A"));
  }

  @Test
  public void testAuthFilter_extractTableName_withTableNameWithTypeInPathParams() {
    MultivaluedMap<String, String> pathParams = new MultivaluedHashMap<>();
    MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<>();
    pathParams.putSingle("tableNameWithType", "B");
    pathParams.putSingle("schemaName", "C");
    queryParams.putSingle("tableName", "D");
    queryParams.putSingle("tableNameWithType", "E");
    queryParams.putSingle("schemaName", "F");
    Optional<String> actual = _authFilter.extractTableName(pathParams, queryParams);
    assertEquals(actual, Optional.of("B"));
  }

  @Test
  public void testAuthFilter_extractTableName_withSchemaNameInPathParams() {
    MultivaluedMap<String, String> pathParams = new MultivaluedHashMap<>();
    MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<>();
    pathParams.putSingle("schemaName", "C");
    queryParams.putSingle("tableName", "D");
    queryParams.putSingle("tableNameWithType", "E");
    queryParams.putSingle("schemaName", "F");
    Optional<String> actual = _authFilter.extractTableName(pathParams, queryParams);
    assertEquals(actual, Optional.of("C"));
  }

  @Test
  public void testAuthFilter_extractTableName_withTableNameInQueryParams() {
    MultivaluedMap<String, String> pathParams = new MultivaluedHashMap<>();
    MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<>();
    queryParams.putSingle("tableName", "D");
    queryParams.putSingle("tableNameWithType", "E");
    queryParams.putSingle("schemaName", "F");
    Optional<String> actual = _authFilter.extractTableName(pathParams, queryParams);
    assertEquals(actual, Optional.of("D"));
  }

  @Test
  public void testAuthFilter_extractTableName_withTableNameWithTypeInQueryParams() {
    MultivaluedMap<String, String> pathParams = new MultivaluedHashMap<>();
    MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<>();
    queryParams.putSingle("tableNameWithType", "E");
    queryParams.putSingle("schemaName", "F");
    Optional<String> actual = _authFilter.extractTableName(pathParams, queryParams);
    assertEquals(actual, Optional.of("E"));
  }

  @Test
  public void testAuthFilter_extractTableName_withSchemaNameInQueryParams() {
    MultivaluedMap<String, String> pathParams = new MultivaluedHashMap<>();
    MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<>();
    queryParams.putSingle("schemaName", "F");
    Optional<String> actual = _authFilter.extractTableName(pathParams, queryParams);
    assertEquals(actual, Optional.of("F"));
  }

  @Test
  public void testAuthFilter_extractTableName_withEmptyParams() {
    MultivaluedMap<String, String> pathParams = new MultivaluedHashMap<>();
    MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<>();
    Optional<String> actual = _authFilter.extractTableName(pathParams, queryParams);
    assertEquals(actual, Optional.empty());
  }
}
