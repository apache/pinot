/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.server.api.restlet;

import com.linkedin.pinot.common.restlet.swagger.Description;
import com.linkedin.pinot.common.restlet.swagger.HttpVerb;
import com.linkedin.pinot.common.restlet.swagger.Paths;
import com.linkedin.pinot.common.restlet.swagger.Summary;
import com.linkedin.pinot.common.utils.MmapUtils;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.restlet.representation.Representation;
import org.restlet.representation.StringRepresentation;
import org.restlet.resource.ResourceException;
import org.restlet.resource.ServerResource;


/**
 * Debug endpoint to check memory allocation.
 */
public class MmapDebugResource extends ServerResource {
  @Override
  @HttpVerb("get")
  @Description("Lists all off-heap allocations and their associated sizes")
  @Summary("View current off-heap allocations")
  @Paths({ "/debug/memory/offheap", "/debug/memory/offheap/" })
  protected Representation get() throws ResourceException {
    try {
      JSONObject returnValue = new JSONObject();

      JSONArray allocationsArray = new JSONArray();
      List<Pair<MmapUtils.AllocationContext, Integer>> allocations = MmapUtils.getAllocationsAndSizes();

      for (Pair<MmapUtils.AllocationContext, Integer> allocation : allocations) {
        JSONObject jsonAllocation = new JSONObject();
        jsonAllocation.put("context", allocation.getKey().getContext());
        jsonAllocation.put("type", allocation.getKey().getAllocationType().toString());
        jsonAllocation.put("size", allocation.getValue());
        allocationsArray.put(jsonAllocation);
      }

      returnValue.put("allocations", allocationsArray);

      return new StringRepresentation(returnValue.toString(2));
    } catch (JSONException e) {
      return new StringRepresentation(e.toString());
    }
  }
}
