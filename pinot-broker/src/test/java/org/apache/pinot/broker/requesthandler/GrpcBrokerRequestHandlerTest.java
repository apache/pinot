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
package org.apache.pinot.broker.requesthandler;

import java.io.IOException;
import java.util.Iterator;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.proto.Server;
import org.apache.pinot.core.common.datatable.DataTableBuilderFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


/**
 * Unit tests for the protected static helper on {@link GrpcBrokerRequestHandler}.
 */
public class GrpcBrokerRequestHandlerTest {

  @Test
  public void testDataTableToStreamingIteratorRoundTrip()
      throws Exception {
    DataTable dataTable = DataTableBuilderFactory.getEmptyDataTable();
    Iterator<Server.ServerResponse> it = GrpcBrokerRequestHandler.dataTableToStreamingIterator(dataTable);
    Assert.assertTrue(it.hasNext(), "iterator must have one element");
    Server.ServerResponse response = it.next();
    Assert.assertFalse(it.hasNext(), "iterator must have exactly one element");
    Assert.assertFalse(response.getPayload().isEmpty(), "payload must not be empty");
    // Verify round-trip: payload must deserialise back to a valid DataTable.
    DataTable roundTripped =
        org.apache.pinot.common.datatable.DataTableFactory.getDataTable(response.getPayload().asReadOnlyByteBuffer());
    Assert.assertNotNull(roundTripped);
  }

  @Test(expectedExceptions = IOException.class)
  public void testDataTableToStreamingIteratorPropagatesIOException()
      throws Exception {
    DataTable brokenDataTable = mock(DataTable.class);
    when(brokenDataTable.toBytes()).thenThrow(new IOException("simulated serialisation failure"));
    GrpcBrokerRequestHandler.dataTableToStreamingIterator(brokenDataTable);
  }
}
