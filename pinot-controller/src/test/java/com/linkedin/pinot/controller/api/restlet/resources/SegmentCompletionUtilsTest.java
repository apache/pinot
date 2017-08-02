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

import com.linkedin.pinot.common.protocols.SegmentCompletionProtocol;
import com.linkedin.pinot.controller.util.SegmentCompletionUtils;
import junit.framework.Assert;
import org.restlet.data.Reference;
import org.testng.annotations.Test;


public class SegmentCompletionUtilsTest {

  @Test
  public void testRequiredParams() {
    Reference reference = new Reference();
    reference.addQueryParameter("name", "mytable__1__5__20170613T2145Z");
    reference.addQueryParameter("offset", "1");
    reference.addQueryParameter("instance", "mytable_REALTIME");

    SegmentCompletionProtocol.Request.Params params = SegmentCompletionUtils.extractParams(reference);
    Assert.assertEquals(params.getOffset(), 1L);
    Assert.assertEquals(params.getSegmentName(), "mytable__1__5__20170613T2145Z");
    Assert.assertEquals(params.getInstanceId(), "mytable_REALTIME");
    Assert.assertEquals(params.getSegmentLocation(), null);
  }

  @Test
  public void testSegmentLocation() {
    Reference reference = new Reference();
    reference.addQueryParameter("name", "mytable__1__5__20170613T2145Z");
    reference.addQueryParameter("offset", "1");
    reference.addQueryParameter("instance", "mytable_REALTIME");
    reference.addQueryParameter("location", "mars");

    SegmentCompletionProtocol.Request.Params params = SegmentCompletionUtils.extractParams(reference);
    Assert.assertEquals(params.getOffset(), 1L);
    Assert.assertEquals(params.getSegmentName(), "mytable__1__5__20170613T2145Z");
    Assert.assertEquals(params.getInstanceId(), "mytable_REALTIME");
    Assert.assertEquals(params.getSegmentLocation(), "mars");
  }

  @Test
  public void testGenerateSegmentFilePrefix() {
    String segmentName = "segment";
    Assert.assertEquals(SegmentCompletionUtils.getSegmentNamePrefix(segmentName), "segment.tmp.");
  }

  @Test
  public void testGenerateSegmentLocation() {
    String segmentName = "segment";
    String segmentNamePrefix = SegmentCompletionUtils.getSegmentNamePrefix(segmentName);
    Assert.assertTrue(SegmentCompletionUtils.generateSegmentFileName(segmentName).startsWith(segmentNamePrefix));
  }
}