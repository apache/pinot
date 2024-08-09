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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.ArrayList;
import java.util.List;
import javax.ws.rs.core.Response;
import org.apache.pinot.common.response.BrokerResponse;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.common.utils.DataSchema;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.apache.pinot.common.exception.QueryException.TABLE_DOES_NOT_EXIST_ERROR_CODE;
import static org.apache.pinot.spi.utils.CommonConstants.Controller.PINOT_QUERY_ERROR_CODE_HEADER;


public class PinotClientRequestTest {

  @Test
  public void testGetPinotQueryResponse()
      throws Exception {

    // for successful query result the 'X-Pinot-Error-Code' should be -1
    BrokerResponse emptyResultBrokerResponse = BrokerResponseNative.EMPTY_RESULT;
    Response successfulResponse = PinotClientRequest.getPinotQueryResponse(emptyResultBrokerResponse);
    Assert.assertEquals(successfulResponse.getStatus(), Response.Status.OK.getStatusCode());
    Assert.assertTrue(successfulResponse.getHeaders().containsKey(PINOT_QUERY_ERROR_CODE_HEADER));
    Assert.assertEquals(successfulResponse.getHeaders().get(PINOT_QUERY_ERROR_CODE_HEADER).size(), 1);
    Assert.assertEquals(successfulResponse.getHeaders().get(PINOT_QUERY_ERROR_CODE_HEADER).get(0), -1);

    // for failed query result the 'X-Pinot-Error-Code' should be Error code fo exception.
    BrokerResponse tableDoesNotExistBrokerResponse = BrokerResponseNative.TABLE_DOES_NOT_EXIST;
    Response tableDoesNotExistResponse = PinotClientRequest.getPinotQueryResponse(tableDoesNotExistBrokerResponse);
    Assert.assertEquals(tableDoesNotExistResponse.getStatus(), Response.Status.OK.getStatusCode());
    Assert.assertTrue(tableDoesNotExistResponse.getHeaders().containsKey(PINOT_QUERY_ERROR_CODE_HEADER));
    Assert.assertEquals(tableDoesNotExistResponse.getHeaders().get(PINOT_QUERY_ERROR_CODE_HEADER).size(), 1);
    Assert.assertEquals(tableDoesNotExistResponse.getHeaders().get(PINOT_QUERY_ERROR_CODE_HEADER).get(0),
        TABLE_DOES_NOT_EXIST_ERROR_CODE);
  }

  @Test
  public void testPinotQueryComparison() throws Exception {
    // Aggregation type difference

    BrokerResponse v1BrokerResponse = new BrokerResponseNative();
    DataSchema v1DataSchema = new DataSchema(new String[]{"sum(col)"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.DOUBLE});
    v1BrokerResponse.setResultTable(new ResultTable(v1DataSchema, List.<Object[]>of(new Object[]{1234})));

    BrokerResponse v2BrokerResponse = new BrokerResponseNative();
    DataSchema v2DataSchema = new DataSchema(new String[]{"EXPR$0"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.LONG});
    v2BrokerResponse.setResultTable(new ResultTable(v2DataSchema, List.<Object[]>of(new Object[]{1234})));

    ObjectNode comparisonResponse = (ObjectNode) PinotClientRequest.getPinotQueryComparisonResponse(
        "SELECT SUM(col) FROM mytable", v1BrokerResponse, v2BrokerResponse).getEntity();

    List<String> comparisonAnalysis = new ObjectMapper().readerFor(new TypeReference<List<String>>() { })
        .readValue(comparisonResponse.get("comparisonAnalysis"));

    Assert.assertEquals(comparisonAnalysis.size(), 1);
    Assert.assertTrue(comparisonAnalysis.get(0).contains("v1 type: DOUBLE, v2 type: LONG"));

    // Default limit in v1

    v1DataSchema = new DataSchema(new String[]{"col1"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING});
    v2DataSchema = new DataSchema(new String[]{"col1"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.STRING});
    List<Object[]> rows = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      rows.add(new Object[]{i});
    }
    v1BrokerResponse.setResultTable(new ResultTable(v1DataSchema, new ArrayList<>(rows)));
    for (int i = 10; i < 100; i++) {
      rows.add(new Object[]{i});
    }
    v2BrokerResponse.setResultTable(new ResultTable(v2DataSchema, new ArrayList<>(rows)));

    comparisonResponse = (ObjectNode) PinotClientRequest.getPinotQueryComparisonResponse(
        "SELECT col1 FROM mytable", v1BrokerResponse, v2BrokerResponse).getEntity();

    comparisonAnalysis = new ObjectMapper().readerFor(new TypeReference<List<String>>() { })
        .readValue(comparisonResponse.get("comparisonAnalysis"));

    Assert.assertEquals(comparisonAnalysis.size(), 1);
    Assert.assertTrue(comparisonAnalysis.get(0).contains("Mismatch in number of rows returned"));
  }
}
