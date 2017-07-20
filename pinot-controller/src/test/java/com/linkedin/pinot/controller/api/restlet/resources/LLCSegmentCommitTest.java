/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.controller.api.restlet.resources;

import com.alibaba.fastjson.JSONObject;
import com.linkedin.pinot.common.metrics.ControllerMetrics;
import com.linkedin.pinot.common.protocols.SegmentCompletionProtocol;
import com.linkedin.pinot.common.utils.LLCSegmentName;
import com.linkedin.pinot.controller.ControllerConf;
import com.linkedin.pinot.controller.helix.ControllerTest;
import com.linkedin.pinot.controller.helix.core.realtime.SegmentCompletionManager;
import com.yammer.metrics.core.MetricsRegistry;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.helix.HelixManager;
import org.restlet.Application;
import org.restlet.Context;
import org.restlet.data.Reference;
import org.restlet.representation.Representation;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;


public class LLCSegmentCommitTest {
  HelixManager createMockHelixManager() {
    HelixManager helixManager = mock(HelixManager.class);
    when(helixManager.isLeader()).thenReturn(true);
    return helixManager;
  }

  @Test
  public void testSegmentCommit() throws Exception {
    SegmentCompletionManager.create(createMockHelixManager(), null, new ControllerConf(),
        new ControllerMetrics(new MetricsRegistry()));
    FakeLLCSegmentCommit segmentCommit = new FakeLLCSegmentCommit();
    Representation representation;
    String strResponse;
    JSONObject jsonResponse;

    // If commitStart returns failed, upload should not be called, and the client should get 'failed'
    segmentCommit._uploadCalled = false;
    segmentCommit._scm.commitStartResponse = SegmentCompletionProtocol.RESP_FAILED;
    representation = segmentCommit.post(null);
    strResponse = representation.getText();
    jsonResponse = JSONObject.parseObject(strResponse);
    Assert.assertEquals(jsonResponse.get(SegmentCompletionProtocol.STATUS_KEY),
        SegmentCompletionProtocol.ControllerResponseStatus.FAILED.toString());
    Assert.assertFalse(segmentCommit._uploadCalled);

    // if commitStart returns continue, and upload fails, then commitEnd should indicate upload failure.
    segmentCommit._uploadCalled = false;
    segmentCommit._scm.commitStartResponse = SegmentCompletionProtocol.RESP_COMMIT_CONTINUE;
    segmentCommit._scm.commitEndResponse = SegmentCompletionProtocol.RESP_FAILED;
    segmentCommit._uploadReturnValue = false;
    representation = segmentCommit.post(null);
    strResponse = representation.getText();
    jsonResponse = JSONObject.parseObject(strResponse);
    Assert.assertEquals(jsonResponse.get(SegmentCompletionProtocol.STATUS_KEY),
        SegmentCompletionProtocol.ControllerResponseStatus.FAILED.toString());
    Assert.assertTrue(segmentCommit._uploadCalled);
    Assert.assertFalse(segmentCommit._scm.uploadSuccess);

    // If commitstart returns CONINUE and upload succeeds, we should return whatever commitEnd returns.
    segmentCommit._uploadCalled = false;
    segmentCommit._scm.commitStartResponse = SegmentCompletionProtocol.RESP_COMMIT_CONTINUE;
    segmentCommit._scm.commitEndResponse = SegmentCompletionProtocol.RESP_COMMIT_SUCCESS;
    segmentCommit._uploadReturnValue = true;
    representation = segmentCommit.post(null);
    strResponse = representation.getText();
    jsonResponse = JSONObject.parseObject(strResponse);
    Assert.assertEquals(jsonResponse.get(SegmentCompletionProtocol.STATUS_KEY),
        SegmentCompletionProtocol.ControllerResponseStatus.COMMIT_SUCCESS.toString());
    Assert.assertTrue(segmentCommit._uploadCalled);
    Assert.assertTrue(segmentCommit._scm.uploadSuccess);
  }

  public class FakeLLCSegmentCommit extends LLCSegmentCommit {
    public boolean _uploadReturnValue;
    public final FakeSegmentCompletionManager _scm;
    public boolean _uploadCalled;
    private final LLCSegmentName _segmentName = new LLCSegmentName("table", 3, 12, System.currentTimeMillis());

    public FakeLLCSegmentCommit() throws IOException {
      _scm = new FakeSegmentCompletionManager();
    }

    @Override
    public Reference getReference() {
      return new Reference().addQueryParameter("offset", "1235")
          .addQueryParameter("instance", "Server_0")
          .addQueryParameter("name", _segmentName.getSegmentName());
    }

    Context createMockContext() {
      Context context = mock(Context.class);
      ConcurrentHashMap<String, Object> map = new ConcurrentHashMap<>(1);
      map.put(ControllerConf.class.toString(), ControllerTest.getDefaultControllerConfiguration());
      when(context.getAttributes()).thenReturn(map);
      return context;
    }

    Application createMockApplication() {
      Application application = mock(Application.class);
      final Context context = createMockContext();
      when(application.getContext()).thenReturn(context);
      return application;
    }

    @Override
    public Application getApplication() {
      return createMockApplication();
    }

    @Override
    boolean uploadSegment(final String instanceId, final String segmentNameStr) {
      _uploadCalled = true;
      return _uploadReturnValue;
    }

    @Override
    protected SegmentCompletionManager getSegmentCompletionManager() {
      return _scm;
    }
  }

  public class FakeSegmentCompletionManager extends SegmentCompletionManager {

    public SegmentCompletionProtocol.Response commitStartResponse;
    public SegmentCompletionProtocol.Response commitEndResponse;
    public boolean commitStartCalled;
    public boolean commitEndCalled;
    public boolean uploadSuccess;

    protected FakeSegmentCompletionManager() {
      super(null, null, new ControllerMetrics(new MetricsRegistry()));
    }

    @Override
    public SegmentCompletionProtocol.Response segmentConsumed(SegmentCompletionProtocol.Request.Params reqParams) {
      return null;
    }

    @Override
    public SegmentCompletionProtocol.Response segmentCommitStart(SegmentCompletionProtocol.Request.Params reqParams) {
      commitStartCalled = true;
      return commitStartResponse;
    }

    @Override
    public SegmentCompletionProtocol.Response segmentCommitEnd(SegmentCompletionProtocol.Request.Params reqParams, boolean success, boolean isSplitCommit) {
      commitEndCalled = true;
      uploadSuccess = success;
      return commitEndResponse;
    }
  }
}
