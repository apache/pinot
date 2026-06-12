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
package org.apache.pinot.common.cursors;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.response.CursorResponse;
import org.apache.pinot.common.response.broker.CursorResponseNative;
import org.apache.pinot.common.response.broker.ResultTable;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;


public class AbstractResponseStoreTest {

  private TestResponseStore _store;

  @BeforeMethod
  public void setUp()
      throws Exception {
    _store = new TestResponseStore();
    _store.init(new PinotConfiguration(), "localhost", 8099, "Broker_localhost_8099",
        mock(BrokerMetrics.class), "1h");
  }

  @Test
  public void testDeleteExpiredResponsesDeletesExpiredEntries()
      throws Exception {
    long now = System.currentTimeMillis();
    _store.putResponse("req-1", createResponse(now - 2000));
    _store.putResponse("req-2", createResponse(now - 1000));
    _store.putResponse("req-3", createResponse(now + 60_000));

    int deleted = _store.deleteExpiredResponses(now);
    Assert.assertEquals(deleted, 2);
    Assert.assertFalse(_store.exists("req-1"));
    Assert.assertFalse(_store.exists("req-2"));
    Assert.assertTrue(_store.exists("req-3"));
  }

  @Test
  public void testDeleteExpiredResponsesReturnsZeroWhenNothingExpired()
      throws Exception {
    long now = System.currentTimeMillis();
    _store.putResponse("req-1", createResponse(now + 60_000));
    _store.putResponse("req-2", createResponse(now + 120_000));

    int deleted = _store.deleteExpiredResponses(now);
    Assert.assertEquals(deleted, 0);
    Assert.assertTrue(_store.exists("req-1"));
    Assert.assertTrue(_store.exists("req-2"));
  }

  @Test
  public void testDeleteExpiredResponsesOnEmptyStore()
      throws Exception {
    int deleted = _store.deleteExpiredResponses(System.currentTimeMillis());
    Assert.assertEquals(deleted, 0);
  }

  @Test
  public void testDeleteExpiredResponsesDeletesAllWhenAllExpired()
      throws Exception {
    long now = System.currentTimeMillis();
    _store.putResponse("req-1", createResponse(now - 3000));
    _store.putResponse("req-2", createResponse(now - 2000));
    _store.putResponse("req-3", createResponse(now - 1000));

    int deleted = _store.deleteExpiredResponses(now);
    Assert.assertEquals(deleted, 3);
    Assert.assertTrue(_store.getAllStoredRequestIds().isEmpty());
  }

  @Test
  public void testDeleteExpiredResponsesExactBoundary()
      throws Exception {
    long cutoff = 5000L;
    _store.putResponse("req-at-boundary", createResponse(cutoff));
    _store.putResponse("req-after-boundary", createResponse(cutoff + 1));

    int deleted = _store.deleteExpiredResponses(cutoff);
    Assert.assertEquals(deleted, 1, "Response exactly at the boundary should be deleted");
    Assert.assertFalse(_store.exists("req-at-boundary"));
    Assert.assertTrue(_store.exists("req-after-boundary"));
  }

  private CursorResponse createResponse(long expirationTimeMs) {
    CursorResponseNative response = new CursorResponseNative();
    response.setExpirationTimeMs(expirationTimeMs);
    response.setSubmissionTimeMs(expirationTimeMs - 3600_000);
    response.setBrokerHost("localhost");
    response.setBrokerPort(8099);
    response.setBytesWritten(0);
    return response;
  }

  /**
   * Minimal in-memory ResponseStore for testing AbstractResponseStore behavior.
   */
  private static class TestResponseStore extends AbstractResponseStore {
    private final Map<String, CursorResponse> _responses = new HashMap<>();
    private final Map<String, ResultTable> _resultTables = new HashMap<>();

    void putResponse(String requestId, CursorResponse response) {
      _responses.put(requestId, response);
    }

    @Override
    public String getType() {
      return "test";
    }

    @Override
    public void init(PinotConfiguration config, String brokerHost, int brokerPort, String brokerId,
        BrokerMetrics brokerMetrics, String expirationTime)
        throws Exception {
      init(brokerHost, brokerPort, brokerId, brokerMetrics, expirationTime);
    }

    @Override
    public boolean exists(String requestId) {
      return _responses.containsKey(requestId);
    }

    @Override
    public Collection<String> getAllStoredRequestIds() {
      return new ArrayList<>(_responses.keySet());
    }

    @Override
    protected void writeResponse(String requestId, CursorResponse response) {
      _responses.put(requestId, response);
    }

    @Override
    protected long writeResultTable(String requestId, ResultTable resultTable) {
      _resultTables.put(requestId, resultTable);
      return 0;
    }

    @Override
    public CursorResponse readResponse(String requestId) {
      return _responses.get(requestId);
    }

    @Override
    protected ResultTable readResultTable(String requestId, int offset, int numRows) {
      return _resultTables.get(requestId);
    }

    @Override
    protected boolean deleteResponseImpl(String requestId) {
      _responses.remove(requestId);
      _resultTables.remove(requestId);
      return true;
    }
  }
}
