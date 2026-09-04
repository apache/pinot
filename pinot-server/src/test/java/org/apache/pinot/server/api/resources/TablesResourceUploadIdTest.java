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
package org.apache.pinot.server.api.resources;

import java.lang.reflect.Method;
import java.util.UUID;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.HttpHeaders;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TablesResourceUploadIdTest {
  @Test
  public void testLegacyJavaMethodDescriptorsRemainAvailable()
      throws NoSuchMethodException {
    TablesResource.class.getMethod("uploadLLCSegment", String.class, String.class, int.class, HttpHeaders.class);
    TablesResource.class.getMethod("uploadLLCSegmentV2", String.class, String.class, int.class, HttpHeaders.class);
    TablesResource.class.getMethod("uploadCommittedSegment", String.class, String.class, int.class,
        HttpHeaders.class);
  }

  @Test
  public void testAllUploadEndpointsBindTheUploadIdQueryParameter()
      throws NoSuchMethodException {
    assertUploadIdQueryParameter("uploadLLCSegment");
    assertUploadIdQueryParameter("uploadLLCSegmentV2");
    assertUploadIdQueryParameter("uploadCommittedSegment");
  }

  @Test
  public void testUploadIdIsStableForOneControllerAttempt() {
    String segmentName = new LLCSegmentName("test_REALTIME", 1, 0, 1234L).getSegmentName();
    UUID requestUploadId = UUID.fromString("00000000-0000-0000-0000-000000000001");
    UUID nextRequestUploadId = UUID.fromString("00000000-0000-0000-0000-000000000002");

    UUID firstId =
        TablesResource.getSegmentUploadId("process-one", "Server_1", segmentName, "123", requestUploadId);
    UUID retryId =
        TablesResource.getSegmentUploadId("process-one", "Server_1", segmentName, "123", requestUploadId);
    UUID nextAttemptId =
        TablesResource.getSegmentUploadId("process-one", "Server_1", segmentName, "123", nextRequestUploadId);
    UUID restartedServerId =
        TablesResource.getSegmentUploadId("process-two", "Server_1", segmentName, "123", requestUploadId);
    UUID differentInstanceId =
        TablesResource.getSegmentUploadId("process-one", "Server_2", segmentName, "123", requestUploadId);
    UUID differentSegmentId = TablesResource.getSegmentUploadId("process-one", "Server_1",
        new LLCSegmentName("test_REALTIME", 2, 0, 1234L).getSegmentName(), "123", requestUploadId);
    UUID differentVersionId =
        TablesResource.getSegmentUploadId("process-one", "Server_1", segmentName, "456", requestUploadId);

    Assert.assertEquals(retryId, firstId);
    Assert.assertNotEquals(nextAttemptId, firstId);
    Assert.assertNotEquals(restartedServerId, firstId);
    Assert.assertNotEquals(differentInstanceId, firstId);
    Assert.assertNotEquals(differentSegmentId, firstId);
    Assert.assertNotEquals(differentVersionId, firstId);
  }

  @Test
  public void testMissingControllerUploadIdUsesFreshLegacyAttempt() {
    String segmentName = new LLCSegmentName("test_REALTIME", 1, 0, 1234L).getSegmentName();

    UUID firstId = TablesResource.getSegmentUploadId("process-one", "Server_1", segmentName, "123", null);
    UUID retryId = TablesResource.getSegmentUploadId("process-one", "Server_1", segmentName, "123", null);

    Assert.assertNotEquals(retryId, firstId);
  }

  private static void assertUploadIdQueryParameter(String methodName)
      throws NoSuchMethodException {
    Method method = TablesResource.class.getMethod(methodName, String.class, String.class, int.class, UUID.class,
        HttpHeaders.class);
    QueryParam queryParam = method.getParameters()[3].getAnnotation(QueryParam.class);
    Assert.assertNotNull(queryParam);
    Assert.assertEquals(queryParam.value(), "uploadId");
  }
}
