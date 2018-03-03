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

import com.google.common.collect.ImmutableList;
import com.linkedin.pinot.common.restlet.resources.ServerPerfMetrics;
import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.core.data.manager.offline.InstanceDataManager;
import com.linkedin.pinot.core.data.manager.offline.SegmentDataManager;
import com.linkedin.pinot.core.data.manager.offline.TableDataManager;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.server.starter.ServerInstance;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * API to provide server performance metrics, for example all hosted segments count and storage size
 */
@Api(tags = "ServerPerfResource")
@Path("/")

public class ServerPerfResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(ServerPerfResource.class);
  private final String TableCPULoadConfigFilePath = "SegmentAssignmentResource/TableCPULoadMetric.properties";


  @Inject
  ServerInstance serverInstance;

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/ServerPerfMetrics/SegmentInfo")
  @ApiOperation(value = "Show all hosted segments count and storage size", notes = "Storage size and count of all segments hosted by a Pinot Server")
  @ApiResponses(value = {@ApiResponse(code = 200, message = "Success"), @ApiResponse(code = 500, message = "Internal server error")})
  public ServerPerfMetrics getSegmentInfo() throws WebApplicationException, IOException {
    InstanceDataManager dataManager = (InstanceDataManager) serverInstance.getInstanceDataManager();
    if (dataManager == null) {
      throw new WebApplicationException("Invalid server initialization", Response.Status.INTERNAL_SERVER_ERROR);
    }


    ClassLoader classLoader = ServerPerfResource.class.getClassLoader();
    String tableCPULoadModelPath = getFileFromResourceUrl(classLoader.getResource(TableCPULoadConfigFilePath));
    List<String> CPULoadModels = FileUtils.readLines(new File(tableCPULoadModelPath));

    Map<String, CPULoadFormulation> tableCPULoadFormulation = new HashMap<>();

    for (int i = 1; i < CPULoadModels.size(); i++) {
      String[] tableLoadModel = CPULoadModels.get(i).split(",");
      tableCPULoadFormulation.put(tableLoadModel[0], new CPULoadFormulation(Double.parseDouble(tableLoadModel[1]), Double.parseDouble(tableLoadModel[2]), Double.parseDouble(tableLoadModel[3])));
    }


    ServerPerfMetrics serverPerfMetrics = new ServerPerfMetrics();
    Collection <TableDataManager> tableDataManagers = dataManager.getTableDataManagers();
    for (TableDataManager tableDataManager : tableDataManagers) {
      ImmutableList <SegmentDataManager> segmentDataManagers = tableDataManager.acquireAllSegments();
      try {
        serverPerfMetrics.segmentCount += segmentDataManagers.size();
        for (SegmentDataManager segmentDataManager : segmentDataManagers) {
          IndexSegment segment = segmentDataManager.getSegment();
          serverPerfMetrics.segmentDiskSizeInBytes += segment.getDiskSizeBytes();
          //serverPerfMetrics.segmentList.add(segment.getSegmentMetadata());

          // LOGGER.info("adding segment " + segment.getSegmentName() + " to the list in server side! st: " + segment.getSegmentMetadata().getStartTime() + " et: " + segment.getSegmentMetadata().getEndTime());
          serverPerfMetrics.segmentCPULoad += tableCPULoadFormulation.get(tableDataManager.getTableName()).computeCPULoad(segment.getSegmentMetadata());
          LOGGER.info("CPU Load is updated to " + serverPerfMetrics.segmentCPULoad + " beacuse of segment " + segment.getSegmentName() );
        }

      } finally {
        // we could release segmentDataManagers as we iterate in the loop above
        // but this is cleaner with clear semantics of usage. Also, above loop
        // executes fast so duration of holding segments is not a concern
        for (SegmentDataManager segmentDataManager : segmentDataManagers) {
          tableDataManager.releaseSegment(segmentDataManager);
        }
      }
    }
    return serverPerfMetrics;
  }


  private static String getFileFromResourceUrl(@Nonnull URL resourceUrl) {
    // For maven cross package use case, we need to extract the resource from jar to a temporary directory.
    String resourceUrlStr = resourceUrl.toString();
    if (resourceUrlStr.contains("jar!")) {
      try {
        String extension = resourceUrlStr.substring(resourceUrlStr.lastIndexOf('.'));
        File tempFile = File.createTempFile("pinot-cpu-load-model-temp", extension);
        String tempFilePath = tempFile.getAbsolutePath();
        //LOGGER.info("Extracting from " + resourceUrlStr + " to " + tempFilePath);
        FileUtils.copyURLToFile(resourceUrl, tempFile);
        return tempFilePath;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    } else {
      return resourceUrl.getFile();
    }
  }


  class CPULoadFormulation {

    //CPU Load for a segment
    //(SegmentDocCount/avgDocCount)*a*e^(-bt)

    private double _avgDocCount = 0;
    private double _a = 0;
    private double _b = 0;

    public CPULoadFormulation(double avgDocCount, double a, double b) {
      _avgDocCount = avgDocCount;
      _a = a;
      _b = b;

    }

    public double computeCPULoad(SegmentMetadata segmentMetadata) {
      long segmentMiddleTime = (segmentMetadata.getStartTime() + segmentMetadata.getEndTime()) / 2;
      long segmentAge = System.currentTimeMillis() / 1000 - segmentMiddleTime;
      return (segmentMetadata.getTotalDocs() / _avgDocCount) * _a * Math.exp(-1 * _b * segmentAge);
    }

  }

}