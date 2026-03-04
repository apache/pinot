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
package org.apache.pinot.spi.stream;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


/**
 * Unit tests for StreamMetadataProvider interface, specifically testing exception handling
 * in the computePartitionGroupMetadata method.
 */
public class StreamMetadataProviderTest {

  private static final String CLIENT_ID = "test-client";
  private static final int TIMEOUT_MILLIS = 5000;
  private static final String TOPIC_NAME = "test-topic";

  @Mock
  private StreamConfig _streamConfig;

  @BeforeMethod
  public void setUp() {
    MockitoAnnotations.openMocks(this);

    // Set up stream config mock
    when(_streamConfig.getTopicName()).thenReturn(TOPIC_NAME);
  }

  @Test
  public void testComputePartitionGroupMetadataWithFetchPartitionCountException() throws Exception {
    // Create existing partition group consumption statuses
    List<PartitionGroupConsumptionStatus> existingStatuses = createExistingPartitionStatuses();

    // Create a StreamMetadataProvider that throws exception on fetchPartitionCount
    StreamMetadataProvider provider = new TestStreamMetadataProviderWithException();

    // Call computePartitionGroupMetadata
    List<PartitionGroupMetadata> result = provider.computePartitionGroupMetadata(
        CLIENT_ID, _streamConfig, existingStatuses, TIMEOUT_MILLIS);

    // Verify that the method returns existing partitions only
    Assert.assertEquals(result.size(), existingStatuses.size());

    // Verify that the returned metadata matches the existing statuses
    for (int i = 0; i < result.size(); i++) {
      PartitionGroupMetadata metadata = result.get(i);
      PartitionGroupConsumptionStatus status = existingStatuses.get(i);

      Assert.assertEquals(metadata.getPartitionGroupId(), status.getStreamPartitionGroupId());
      Assert.assertEquals(metadata.getStartOffset(), status.getEndOffset());
    }
  }

  @Test
  public void testComputePartitionGroupMetadataWithEmptyExistingStatuses() throws Exception {
    // Test with empty existing partition statuses
    List<PartitionGroupConsumptionStatus> emptyStatuses = new ArrayList<>();

    StreamMetadataProvider provider = new TestStreamMetadataProviderWithException();

    List<PartitionGroupMetadata> result = provider.computePartitionGroupMetadata(
        CLIENT_ID, _streamConfig, emptyStatuses, TIMEOUT_MILLIS);

    // Should return empty list when no existing partitions and fetchPartitionCount fails
    Assert.assertEquals(result.size(), 0);
  }

  @Test
  public void testComputePartitionGroupMetadataWithDifferentExceptionTypes() throws Exception {
    List<PartitionGroupConsumptionStatus> existingStatuses = createExistingPartitionStatuses();

    // Test with RuntimeException
    StreamMetadataProvider providerWithRuntimeException = new StreamMetadataProvider() {
      @Override
      public int fetchPartitionCount(long timeoutMillis) {
        throw new RuntimeException("Connection failed");
      }

      @Override
      public StreamPartitionMsgOffset fetchStreamPartitionOffset(OffsetCriteria offsetCriteria, long timeoutMillis)
          throws TimeoutException {
        return mock(StreamPartitionMsgOffset.class);
      }

      @Override
      public boolean supportsOffsetLag() {
        return true;
      }

      @Override
      public void close() throws IOException {
      }
    };

    List<PartitionGroupMetadata> result = providerWithRuntimeException.computePartitionGroupMetadata(
        CLIENT_ID, _streamConfig, existingStatuses, TIMEOUT_MILLIS);

    Assert.assertEquals(result.size(), existingStatuses.size());

    // Test with TimeoutException
    StreamMetadataProvider providerWithTimeoutException = new StreamMetadataProvider() {
      @Override
      public int fetchPartitionCount(long timeoutMillis) {
        throw new RuntimeException(new TimeoutException("Timeout occurred"));
      }

      @Override
      public StreamPartitionMsgOffset fetchStreamPartitionOffset(OffsetCriteria offsetCriteria, long timeoutMillis)
          throws TimeoutException {
        return mock(StreamPartitionMsgOffset.class);
      }

      @Override
      public boolean supportsOffsetLag() {
        return true;
      }

      @Override
      public void close() throws IOException {
      }
    };

    result = providerWithTimeoutException.computePartitionGroupMetadata(
        CLIENT_ID, _streamConfig, existingStatuses, TIMEOUT_MILLIS);

    Assert.assertEquals(result.size(), existingStatuses.size());
  }

  /**
   * Creates a list of existing partition group consumption statuses for testing
   */
  private List<PartitionGroupConsumptionStatus> createExistingPartitionStatuses() {
    List<PartitionGroupConsumptionStatus> statuses = new ArrayList<>();

    // Create mock offset
    StreamPartitionMsgOffset mockOffset1 = mock(StreamPartitionMsgOffset.class);
    StreamPartitionMsgOffset mockOffset2 = mock(StreamPartitionMsgOffset.class);

    statuses.add(new PartitionGroupConsumptionStatus(0, 1, mockOffset1, mockOffset1, "CONSUMING"));
    statuses.add(new PartitionGroupConsumptionStatus(1, 2, mockOffset2, mockOffset2, "CONSUMING"));

    return statuses;
  }

  /**
   * Test implementation of StreamMetadataProvider that throws an exception
   * when fetchPartitionCount is called
   */
  private static class TestStreamMetadataProviderWithException implements StreamMetadataProvider {

    @Override
    public int fetchPartitionCount(long timeoutMillis) {
      throw new RuntimeException("Simulated partition count fetch failure");
    }

    @Override
    public StreamPartitionMsgOffset fetchStreamPartitionOffset(OffsetCriteria offsetCriteria, long timeoutMillis)
        throws TimeoutException {
      // This should not be called in the exception scenario
      return mock(StreamPartitionMsgOffset.class);
    }

    @Override
    public boolean supportsOffsetLag() {
      return true;
    }

    @Override
    public void close() throws IOException {
      // No-op for test
    }
  }
}
