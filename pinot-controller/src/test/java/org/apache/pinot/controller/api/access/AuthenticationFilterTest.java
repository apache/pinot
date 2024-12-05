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

import java.lang.reflect.Method;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import org.apache.pinot.common.auth.AuthProviderUtils;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class AuthenticationFilterTest {
  private final AuthenticationFilter _authFilter = new AuthenticationFilter();

  @Test
  public void testExtractTableNameWithTableNameInPathParams() {
    MultivaluedMap<String, String> pathParams = new MultivaluedHashMap<>();
    MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<>();
    pathParams.putSingle("tableName", "A");
    pathParams.putSingle("tableNameWithType", "B");
    pathParams.putSingle("schemaName", "C");
    queryParams.putSingle("tableName", "D");
    queryParams.putSingle("tableNameWithType", "E");
    queryParams.putSingle("schemaName", "F");
    assertEquals(AuthenticationFilter.extractTableName(pathParams, queryParams), "A");
  }

  @Test
  public void testExtractTableNameWithTableNameWithTypeInPathParams() {
    MultivaluedMap<String, String> pathParams = new MultivaluedHashMap<>();
    MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<>();
    pathParams.putSingle("tableNameWithType", "B");
    pathParams.putSingle("schemaName", "C");
    queryParams.putSingle("tableName", "D");
    queryParams.putSingle("tableNameWithType", "E");
    queryParams.putSingle("schemaName", "F");
    assertEquals(AuthenticationFilter.extractTableName(pathParams, queryParams), "B");
  }

  @Test
  public void testExtractTableNameWithSchemaNameInPathParams() {
    MultivaluedMap<String, String> pathParams = new MultivaluedHashMap<>();
    MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<>();
    pathParams.putSingle("schemaName", "C");
    queryParams.putSingle("tableName", "D");
    queryParams.putSingle("tableNameWithType", "E");
    queryParams.putSingle("schemaName", "F");
    assertEquals(AuthenticationFilter.extractTableName(pathParams, queryParams), "C");
  }

  @Test
  public void testExtractTableNameWithTableNameInQueryParams() {
    MultivaluedMap<String, String> pathParams = new MultivaluedHashMap<>();
    MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<>();
    queryParams.putSingle("tableName", "D");
    queryParams.putSingle("tableNameWithType", "E");
    queryParams.putSingle("schemaName", "F");
    assertEquals(AuthenticationFilter.extractTableName(pathParams, queryParams), "D");
  }

  @Test
  public void testExtractTableNameWithTableNameWithTypeInQueryParams() {
    MultivaluedMap<String, String> pathParams = new MultivaluedHashMap<>();
    MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<>();
    queryParams.putSingle("tableNameWithType", "E");
    queryParams.putSingle("schemaName", "F");
    assertEquals(AuthenticationFilter.extractTableName(pathParams, queryParams), "E");
  }

  @Test
  public void testExtractTableNameWithSchemaNameInQueryParams() {
    MultivaluedMap<String, String> pathParams = new MultivaluedHashMap<>();
    MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<>();
    queryParams.putSingle("schemaName", "F");
    assertEquals(AuthenticationFilter.extractTableName(pathParams, queryParams), "F");
  }

  @Test
  public void testExtractTableNameWithEmptyParams() {
    MultivaluedMap<String, String> pathParams = new MultivaluedHashMap<>();
    MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<>();
    assertNull(AuthenticationFilter.extractTableName(pathParams, queryParams));
  }

  @Test
  public void testExtractAccessTypeWithAuthAnnotation() throws Exception {
    Method method = AuthenticationFilterTest.class.getMethod("methodWithAuthAnnotation");
    assertEquals(AccessType.UPDATE, _authFilter.extractAccessType(method));
  }

  @Test
  public void testExtractAccessTypeWithMissingAuthAnnotation() throws Exception {
    Method method = AuthenticationFilterTest.class.getMethod("methodWithGet");
    assertEquals(AccessType.READ, _authFilter.extractAccessType(method));
    method = AuthenticationFilterTest.class.getMethod("methodWithPost");
    assertEquals(AccessType.CREATE, _authFilter.extractAccessType(method));
    method = AuthenticationFilterTest.class.getMethod("methodWithPut");
    assertEquals(AccessType.UPDATE, _authFilter.extractAccessType(method));
    method = AuthenticationFilterTest.class.getMethod("methodWithDelete");
    assertEquals(AccessType.DELETE, _authFilter.extractAccessType(method));
  }

  // DataProvider supplying test cases
  @DataProvider(name = "pathProvider")
  public Object[][] pathProvider() {
    return new Object[][] {
        {"/path/to/resource;param1=value1;param2=value2", "/path/to/resource"}, // with matrix params
        {"/path/to/resource", "/path/to/resource"},                             // no matrix params
        {"", ""},                                                               // empty path
        {";param1=value1/path/to/resource", ""},                                // matrix at beginning
        {"/path;param1=value1;param2=value2/to/resource", "/path"}              // multiple semicolons
    };
  }

  @Test(dataProvider = "pathProvider")
  public void testStripMatrixParams(String input, String expected) {
    assertEquals(AuthProviderUtils.stripMatrixParams(input), expected);
  }

  @Authenticate(AccessType.UPDATE)
  public void methodWithAuthAnnotation() {
  }

  @GET
  public void methodWithGet() {
  }

  @PUT
  public void methodWithPut() {
  }

  @POST
  public void methodWithPost() {
  }

  @DELETE
  public void methodWithDelete() {
  }
}
