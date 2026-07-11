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
import java.util.Arrays;
import java.util.List;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import org.apache.pinot.common.auth.AuthProviderUtils;
import org.apache.pinot.controller.api.resources.LLCSegmentCompletionHandlers;
import org.apache.pinot.controller.api.resources.PinotBrokerRestletResource;
import org.apache.pinot.controller.api.resources.PinotControllerLogger;
import org.apache.pinot.controller.api.resources.PinotControllerPeriodicTaskRestletResource;
import org.apache.pinot.controller.api.resources.PinotInstanceRestletResource;
import org.apache.pinot.controller.api.resources.PinotQueryResource;
import org.apache.pinot.controller.api.resources.PinotTableRestletResource;
import org.apache.pinot.core.auth.Actions;
import org.apache.pinot.core.auth.Authorize;
import org.apache.pinot.core.auth.TargetType;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class AuthenticationFilterTest {
  private final AuthenticationFilter _authFilter = new AuthenticationFilter();

  @Test
  public void testExtractTableNameWithTableNameInPathParams() throws Exception {
    MultivaluedMap<String, String> pathParams = new MultivaluedHashMap<>();
    MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<>();
    pathParams.putSingle("tableName", "A");
    pathParams.putSingle("tableNameWithType", "B");
    pathParams.putSingle("schemaName", "C");
    queryParams.putSingle("tableName", "D");
    queryParams.putSingle("tableNameWithType", "E");
    queryParams.putSingle("schemaName", "F");
    assertEquals(AuthenticationFilter.extractTableName(allTableQueryParamsMethod(), pathParams, queryParams), "A");
  }

  @Test
  public void testExtractTableNameWithTableNameWithTypeInPathParams() throws Exception {
    MultivaluedMap<String, String> pathParams = new MultivaluedHashMap<>();
    MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<>();
    pathParams.putSingle("tableNameWithType", "B");
    pathParams.putSingle("schemaName", "C");
    queryParams.putSingle("tableName", "D");
    queryParams.putSingle("tableNameWithType", "E");
    queryParams.putSingle("schemaName", "F");
    assertEquals(AuthenticationFilter.extractTableName(allTableQueryParamsMethod(), pathParams, queryParams), "B");
  }

  @Test
  public void testExtractTableNameWithSchemaNameInPathParams() throws Exception {
    MultivaluedMap<String, String> pathParams = new MultivaluedHashMap<>();
    MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<>();
    pathParams.putSingle("schemaName", "C");
    queryParams.putSingle("tableName", "D");
    queryParams.putSingle("tableNameWithType", "E");
    queryParams.putSingle("schemaName", "F");
    assertEquals(AuthenticationFilter.extractTableName(allTableQueryParamsMethod(), pathParams, queryParams), "C");
  }

  @Test
  public void testExtractTableNameWithTableNameInQueryParams() throws Exception {
    MultivaluedMap<String, String> pathParams = new MultivaluedHashMap<>();
    MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<>();
    queryParams.putSingle("tableName", "D");
    queryParams.putSingle("tableNameWithType", "E");
    queryParams.putSingle("schemaName", "F");
    assertEquals(AuthenticationFilter.extractTableName(allTableQueryParamsMethod(), pathParams, queryParams), "D");
  }

  @Test
  public void testExtractTableNameWithTableNameWithTypeInQueryParams() throws Exception {
    MultivaluedMap<String, String> pathParams = new MultivaluedHashMap<>();
    MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<>();
    queryParams.putSingle("tableNameWithType", "E");
    queryParams.putSingle("schemaName", "F");
    assertEquals(AuthenticationFilter.extractTableName(allTableQueryParamsMethod(), pathParams, queryParams), "E");
  }

  @Test
  public void testExtractTableNameWithSchemaNameInQueryParams() throws Exception {
    MultivaluedMap<String, String> pathParams = new MultivaluedHashMap<>();
    MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<>();
    queryParams.putSingle("schemaName", "F");
    assertEquals(AuthenticationFilter.extractTableName(allTableQueryParamsMethod(), pathParams, queryParams), "F");
  }

  @Test
  public void testExtractTableNameWithEmptyParams() throws Exception {
    MultivaluedMap<String, String> pathParams = new MultivaluedHashMap<>();
    MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<>();
    assertNull(AuthenticationFilter.extractTableName(allTableQueryParamsMethod(), pathParams, queryParams));
  }

  private static Method allTableQueryParamsMethod()
      throws Exception {
    return AuthenticationFilterTest.class.getMethod("methodWithAllTableQueryParams", String.class, String.class,
        String.class);
  }

  @Test
  public void testAuthorizeTargetPreservesCoarseTableScope() throws Exception {
    MultivaluedMap<String, String> pathParams = new MultivaluedHashMap<>();
    MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<>();
    queryParams.putSingle("tableName", "A");

    // A cluster endpoint that declares a table query parameter is table-scoped when the caller supplies it.
    Method clusterMethodWithTableParam =
        AuthenticationFilterTest.class.getMethod("methodWithClusterAuthorizationAndTableParam", String.class);
    assertEquals(AuthenticationFilter.extractTableName(clusterMethodWithTableParam, pathParams, queryParams), "A");

    queryParams.clear();
    assertNull(AuthenticationFilter.extractTableName(clusterMethodWithTableParam, pathParams, queryParams));

    queryParams.putSingle("tableName", "A");

    Method tableMethod = AuthenticationFilterTest.class.getMethod("methodWithTableAuthorization", String.class);
    assertEquals(AuthenticationFilter.extractTableName(tableMethod, pathParams, queryParams), "A");

    // The same rule applies to the annotated branch: an @Authorize paramName the endpoint never binds is not the
    // caller's to supply, so it resolves no table and the request stays cluster-scoped.
    Method unboundParamMethod =
        AuthenticationFilterTest.class.getMethod("methodWithTableAuthorizationAndUnboundParam");
    assertNull(AuthenticationFilter.extractTableName(unboundParamMethod, pathParams, queryParams));

    pathParams.putSingle("materializedViewTableName", "B");
    Method customTableParamMethod =
        AuthenticationFilterTest.class.getMethod("methodWithCustomTableParamAuthorization");
    assertEquals(AuthenticationFilter.extractTableName(customTableParamMethod, pathParams, queryParams), "B");

    pathParams.remove("materializedViewTableName");
    assertNull(AuthenticationFilter.extractTableName(customTableParamMethod, pathParams, queryParams));
  }

  /// A caller must not be able to make a cluster endpoint look table-scoped by appending a table query parameter the
  /// endpoint never declared. `AccessControlUtils.validatePermission` picks the table-scoped or the cluster-wide check
  /// based on whether a table name was resolved, so an undeclared parameter honored here would let a principal scoped
  /// to one table pass the cluster check for every cluster endpoint.
  @Test
  public void testUndeclaredTableQueryParamDoesNotDowngradeClusterScope() throws Exception {
    MultivaluedMap<String, String> pathParams = new MultivaluedHashMap<>();
    MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<>();

    Method clusterMethod = AuthenticationFilterTest.class.getMethod("methodWithClusterAuthorization");
    for (String key : List.of("tableName", "tableNameWithType", "schemaName")) {
      queryParams.clear();
      queryParams.putSingle(key, "A");
      assertNull(AuthenticationFilter.extractTableName(clusterMethod, pathParams, queryParams),
          "undeclared query parameter " + key + " must not resolve a table name");
    }

    // A path parameter is a variable of the endpoint's own @Path template, so it stays authoritative.
    queryParams.clear();
    pathParams.putSingle("tableName", "A");
    assertEquals(AuthenticationFilter.extractTableName(clusterMethod, pathParams, queryParams), "A");
  }

  @Test
  public void testDeclaredTableQueryParamScopesRequest() throws Exception {
    MultivaluedMap<String, String> pathParams = new MultivaluedHashMap<>();
    MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<>();

    Method typedTableParamMethod = AuthenticationFilterTest.class.getMethod(
        "methodWithClusterAuthorizationAndTypedTableParam", String.class);
    queryParams.putSingle("tableNameWithType", "A");
    assertEquals(AuthenticationFilter.extractTableName(typedTableParamMethod, pathParams, queryParams), "A");

    // Only the declared key counts, even when an undeclared one of higher precedence is also supplied.
    queryParams.putSingle("tableName", "B");
    assertEquals(AuthenticationFilter.extractTableName(typedTableParamMethod, pathParams, queryParams), "A");

    Method schemaParamMethod =
        AuthenticationFilterTest.class.getMethod("methodWithClusterAuthorizationAndSchemaParam", String.class);
    queryParams.clear();
    queryParams.putSingle("schemaName", "C");
    assertEquals(AuthenticationFilter.extractTableName(schemaParamMethod, pathParams, queryParams), "C");
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

  @Test
  public void testMutatingGetEndpointDeclaresUpdateAccess() throws Exception {
    Method method = PinotControllerPeriodicTaskRestletResource.class.getMethod("runPeriodicTask", String.class,
        String.class, String.class, HttpHeaders.class);
    assertEquals(_authFilter.extractAccessType(method), AccessType.UPDATE);
  }

  @Test
  public void testReadOnlyEndpointsDeclareReadAccess() {
    assertReadAccess(PinotQueryResource.class, "validateMultiStageQuery", "extractTableNames");
    assertReadAccess(PinotInstanceRestletResource.class, "instanceTagUpdateSafetyCheck");
    assertReadAccess(PinotTableRestletResource.class, "rebalanceStatus");
    assertReadAccess(PinotControllerLogger.class, "downloadLogFile", "downloadLogFileFromInstance");
  }

  @Test
  public void testSegmentCompletionGetEndpointsDeclareCreateAccess() {
    for (String methodName : new String[]{"extendBuildTime", "segmentConsumed", "segmentStoppedConsuming",
        "segmentCommitStart", "reduceSegmentSize"}) {
      Method method = Arrays.stream(LLCSegmentCompletionHandlers.class.getDeclaredMethods())
          .filter(candidate -> candidate.getName().equals(methodName)).findFirst().orElseThrow();
      assertEquals(_authFilter.extractAccessType(method), AccessType.CREATE);
      assertEquals(method.getAnnotation(Authorize.class).action(), Actions.Cluster.COMMIT_SEGMENT);
    }
  }

  @Test
  public void testBrokerForTableEndpointDeclaresTableAuthorization() throws Exception {
    Method method = PinotBrokerRestletResource.class.getMethod("getBrokersForTableV2", String.class, String.class,
        String.class, HttpHeaders.class);
    Authorize authorize = method.getAnnotation(Authorize.class);
    assertEquals(authorize.targetType(), TargetType.TABLE);
    assertEquals(authorize.paramName(), "tableName");
    assertEquals(authorize.action(), Actions.Table.GET_BROKER);
  }

  private void assertReadAccess(Class<?> resourceClass, String... methodNames) {
    for (String methodName : methodNames) {
      Method method = Arrays.stream(resourceClass.getDeclaredMethods())
          .filter(candidate -> candidate.getName().equals(methodName)).findFirst().orElseThrow();
      assertEquals(_authFilter.extractAccessType(method), AccessType.READ,
          resourceClass.getSimpleName() + "." + methodName);
    }
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

  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.GET_CLUSTER_CONFIG)
  public void methodWithClusterAuthorization() {
  }

  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.EXECUTE_TASK)
  public void methodWithClusterAuthorizationAndTableParam(@QueryParam("tableName") String tableName) {
  }

  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.INGEST_FILE)
  public void methodWithClusterAuthorizationAndTypedTableParam(
      @QueryParam("tableNameWithType") String tableNameWithType) {
  }

  @Authorize(targetType = TargetType.CLUSTER, action = Actions.Cluster.GET_SCHEMA)
  public void methodWithClusterAuthorizationAndSchemaParam(@QueryParam("schemaName") String schemaName) {
  }

  /// Stands in for an endpoint that binds every table-identifying query parameter, so the precedence rules can be
  /// exercised independently of the declaration filter.
  public void methodWithAllTableQueryParams(@QueryParam("tableName") String tableName,
      @QueryParam("tableNameWithType") String tableNameWithType, @QueryParam("schemaName") String schemaName) {
  }

  @Authorize(targetType = TargetType.TABLE, paramName = "tableName", action = Actions.Table.GET_TABLE_CONFIG)
  public void methodWithTableAuthorization(@QueryParam("tableName") String tableName) {
  }

  /// Declares a table target whose parameter the method never binds — a misannotation the resolution must fail closed
  /// on rather than trust the caller for.
  @Authorize(targetType = TargetType.TABLE, paramName = "tableName", action = Actions.Table.GET_TABLE_CONFIG)
  public void methodWithTableAuthorizationAndUnboundParam() {
  }

  @Authorize(targetType = TargetType.TABLE, paramName = "materializedViewTableName",
      action = Actions.Table.GET_TABLE_CONFIG)
  public void methodWithCustomTableParamAuthorization() {
  }
}
