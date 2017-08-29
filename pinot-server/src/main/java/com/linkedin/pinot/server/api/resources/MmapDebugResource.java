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
package com.linkedin.pinot.server.api.resources;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import com.linkedin.pinot.common.utils.MmapUtils;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;


/**
 * Debug endpoint to check memory allocation.
 */
@Api(value="debug", description="Debug information", tags = "Debug")
@Path("debug")
public class MmapDebugResource {

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class AllocationInfo {
    public String context;
    public String type;
    public int size;
  }

  @GET
  @Path("memory/offheap")
  @ApiOperation(value = "View current off-heap allocations",
      notes = "Lists all off-heap allocations and their associated sizes")
  @ApiResponses(value = {@ApiResponse(code=200, message = "Success")})
  @Produces(MediaType.APPLICATION_JSON)
  public Map<String, List<AllocationInfo>> getOffHeapSizes() {
    List<AllocationInfo> allocations = new ArrayList<>();

    List<Pair<MmapUtils.AllocationContext, Integer>> allocationsMap = MmapUtils.getAllocationsAndSizes();

    for (Pair<MmapUtils.AllocationContext, Integer> allocation : allocationsMap) {
      AllocationInfo info = new AllocationInfo();
      info.context = allocation.getKey().getContext();
      info.type = allocation.getKey().getContext();
      info.size = allocation.getValue();
      allocations.add(info);
    }
    Map<String, List<AllocationInfo> > allocationMap = new HashMap<>();
    allocationMap.put("allocations", allocations);
    return allocationMap;
  }
}
