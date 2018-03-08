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



      String tableName = tableLoadModel[0];
      double avgDocCount = Double.parseDouble(tableLoadModel[1]);
      double cpuModelA = Double.parseDouble(tableLoadModel[2]);
      double cpuModelB =  Double.parseDouble(tableLoadModel[3]);
      double cpuModelAlpha = Double.parseDouble(tableLoadModel[4]);
      double lifeTimeInMonth =  Double.parseDouble(tableLoadModel[5]);
      double timeScale =  Double.parseDouble(tableLoadModel[6]);
      double docScannedModelP1 = Double.parseDouble(tableLoadModel[7]);
      double docScannedModelP2 = Double.parseDouble(tableLoadModel[8]);
      double docScannedModelP3 = Double.parseDouble(tableLoadModel[9]);;

      //LOGGER.info("ReadingTableConfig: {}, {}, {}, {}, {}, {}, {}, {}, {}", tableName, avgDocCount, cpuModelA,cpuModelB,cpuModelAlpha,lifeTimeInMonth,timeScale,docScannedModelP1,docScannedModelP2,docScannedModelP3);
      //tableCPULoadFormulation.put(tableName, new CPULoadFormulation(avgDocCount,cpuModelA,alpha,b,lifeTime,timeScale));

      tableCPULoadFormulation.put(tableName, new CPULoadFormulation(avgDocCount,cpuModelA,cpuModelB,cpuModelAlpha,lifeTimeInMonth,timeScale,docScannedModelP1,docScannedModelP2,docScannedModelP3));
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
          double segmentLoad = tableCPULoadFormulation.get(tableDataManager.getTableName()).computeCPULoad(segment.getSegmentMetadata(),1522540921);
          serverPerfMetrics.segmentCPULoad += segmentLoad;
          LOGGER.info("SegmentLoadIsComputed: {}, {}, {}", System.currentTimeMillis(), segment.getSegmentMetadata().getName(), segmentLoad);
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
    private double _cpuModelA = 0;
    private double _cpuModelB = 0;
    private double _cpuModelAlpha = 0;
    private double _lifeTimeInMonth=0;
    private double _timeScale=0;
    private double _docScannedModelP1 = 0;
    private double _docScannedModelP2 = 0;
    private double _docScannedModelP3 = 0;

    public CPULoadFormulation(double avgDocCount, double cpuModelA, double cpuModelAlpha, double cpuModelB, double lifeTimeInMonth, double timeScale) {
      _avgDocCount = avgDocCount;
      _cpuModelA = cpuModelA;
      _cpuModelB = cpuModelB;
      _cpuModelAlpha = cpuModelAlpha;
      _lifeTimeInMonth = lifeTimeInMonth;
      _timeScale = timeScale;
    }

    public CPULoadFormulation(double avgDocCount, double cpuModelA, double cpuModelB, double cpuModelAlpha, double lifeTimeInMonth, double timeScale, double docScannedModelP1, double docScannedModelP2, double docScannedModelP3) {
      _avgDocCount = avgDocCount;
      _cpuModelA = cpuModelA;
      _cpuModelB = cpuModelB;
      _cpuModelAlpha = cpuModelAlpha;
      _lifeTimeInMonth = lifeTimeInMonth;
      _timeScale = timeScale;

      _docScannedModelP1 = docScannedModelP1;
      _docScannedModelP2 = docScannedModelP2;
      _docScannedModelP3 = docScannedModelP3;
    }


    public double computeCPULoad(SegmentMetadata segmentMetadata) {

      //LOGGER.info("ComputingLoadFor: {}, {}, {}, {}, {}, {}", _avgDocCount, _a, _alpha, _b, _lifeTimeInMonth, _timeScale);

      double lifeTimeInScaleSeconds = (_lifeTimeInMonth * 30 * 24 * 3600)/_timeScale;

      double segmentMiddleTime = (segmentMetadata.getStartTime() + segmentMetadata.getEndTime()) / 2;
      double segmentAgeInScaleSeconds = ((System.currentTimeMillis()/1000) - segmentMiddleTime)/_timeScale;

      if(segmentAgeInScaleSeconds < 0)
      {
        segmentAgeInScaleSeconds = (1525132800 - segmentMiddleTime)/_timeScale;
      }

      if(segmentAgeInScaleSeconds < 0)
      {
        segmentAgeInScaleSeconds = 0;
      }

      if(segmentAgeInScaleSeconds > lifeTimeInScaleSeconds) {
        return 0;
      }

      double tmp = 1 - _cpuModelAlpha;

      LOGGER.info("ComputingLoadFor: {}, {}, {}, {}, {}", tmp,  _cpuModelA*(lifeTimeInScaleSeconds - segmentAgeInScaleSeconds), (_cpuModelB/tmp), Math.pow(lifeTimeInScaleSeconds,tmp), Math.pow(segmentAgeInScaleSeconds,tmp));
      //For monthly segments
      double segmentCost = _cpuModelA*(lifeTimeInScaleSeconds - segmentAgeInScaleSeconds) + (_cpuModelB/tmp)*(Math.pow(lifeTimeInScaleSeconds,tmp) - Math.pow(segmentAgeInScaleSeconds,tmp));

      //For daily segments

      segmentCost *= segmentMetadata.getTotalDocs()/_avgDocCount;
      return segmentCost;

      //return 0;
    }

    public double computeCPULoad(SegmentMetadata segmentMetadata, long maxTime) {

      //LOGGER.info("ComputingLoadFor: {}, {}, {}, {}, {}, {}, {}, {}", _avgDocCount, _cpuModelA,_cpuModelB,_cpuModelAlpha,_lifeTimeInMonth,_timeScale,_docScannedModelP1,_docScannedModelP2,_docScannedModelP3);

      double lifeTimeInScaleSeconds = (_lifeTimeInMonth * 30 * 24 * 3600)/_timeScale;

      double segmentMiddleTime = (segmentMetadata.getStartTime() + segmentMetadata.getEndTime()) / 2;
      double segmentAgeInScaleSeconds = ( maxTime - segmentMiddleTime)/_timeScale;

      if(segmentAgeInScaleSeconds > lifeTimeInScaleSeconds) {
        return 0;
      }



      double tmp1 = 1 - _cpuModelAlpha;
      double tmp2 = 2 - _cpuModelAlpha;
      double tmp3 = 3 - _cpuModelAlpha;

      double tmp4 = (_docScannedModelP1/tmp3) * (Math.pow(lifeTimeInScaleSeconds,tmp3)-Math.pow(segmentAgeInScaleSeconds,tmp3));
      tmp4 += (_docScannedModelP2/tmp2) * (Math.pow(lifeTimeInScaleSeconds,tmp2)-Math.pow(segmentAgeInScaleSeconds,tmp2));
      tmp4 += (_docScannedModelP3/tmp1) * (Math.pow(lifeTimeInScaleSeconds,tmp1)-Math.pow(segmentAgeInScaleSeconds,tmp1));

      double tmp5 = (_docScannedModelP1/3)*(Math.pow(lifeTimeInScaleSeconds,3)-Math.pow(segmentAgeInScaleSeconds,3));
      tmp5 += (_docScannedModelP2/2)*(Math.pow(lifeTimeInScaleSeconds,2)-Math.pow(segmentAgeInScaleSeconds,2));
      tmp5 += (_docScannedModelP3)*(lifeTimeInScaleSeconds-segmentAgeInScaleSeconds);

      double perDocCost = (_cpuModelA * tmp4) + (_cpuModelB * tmp5);

      double segmentCost = segmentMetadata.getTotalDocs() * perDocCost;
      //LOGGER.info("DifferentLevelComputation: {}, {}, {}, {}, {}, {}, {}", tmp1,tmp2,tmp3,tmp4,tmp5,perDocCost,segmentCost);

      return segmentCost;

      //return 0;
    }

  }

}