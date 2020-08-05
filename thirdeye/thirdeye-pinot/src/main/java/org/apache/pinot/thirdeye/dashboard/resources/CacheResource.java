/*
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

package org.apache.pinot.thirdeye.dashboard.resources;

import org.apache.pinot.thirdeye.api.Constants;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.google.common.cache.LoadingCache;
import org.apache.pinot.thirdeye.datalayer.dto.DatasetConfigDTO;
import org.apache.pinot.thirdeye.datalayer.dto.MetricConfigDTO;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import org.apache.pinot.thirdeye.datasource.ThirdEyeCacheRegistry;
import org.apache.pinot.thirdeye.datasource.cache.MetricDataset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Api(tags = { Constants.CACHE_TAG })
@Produces(MediaType.APPLICATION_JSON)
public class CacheResource {
  private static final Logger LOG = LoggerFactory.getLogger(CacheResource.class);

  private static final ExecutorService EXECUTOR_SERVICE = Executors.newFixedThreadPool(20);
  private ThirdEyeCacheRegistry CACHE_INSTANCE = ThirdEyeCacheRegistry.getInstance();
  private DAORegistry DAO_REGISTRY = DAORegistry.getInstance();

  @GET
  @Path(value = "/")
  @Produces(MediaType.TEXT_HTML)
  public String getCaches() {
    return "usage";
  }

  @POST
  @Path("/refresh")
  @ApiOperation(value = "Refresh all caches")
  public Response refreshAllCaches() {

    refreshDatasets();

    refreshDatasetConfigCache();
    refreshMetricConfigCache();

    refreshMaxDataTimeCache();
    refreshDimensionFiltersCache();

    return Response.ok().build();
  }

  public Response refreshMaxDataTimeCache() {
    // TODO: Clean deprecate endpoint called by our own code
    List<String> datasets = CACHE_INSTANCE.getDatasetsCache().getDatasets();
    final LoadingCache<String,Long> cache = CACHE_INSTANCE.getDatasetMaxDataTimeCache();
    for (final String dataset : datasets) {
      EXECUTOR_SERVICE.submit(new Runnable() {

        @Override
        public void run() {
          cache.refresh(dataset);
        }
      });
    }
    return Response.ok().build();
  }

  @POST
  @Path("/refresh/datasetConfig")
  public Response refreshDatasetConfigCache() {
    List<String> datasets = CACHE_INSTANCE.getDatasetsCache().getDatasets();
    final LoadingCache<String,DatasetConfigDTO> cache = CACHE_INSTANCE.getDatasetConfigCache();
    for (final String dataset : datasets) {
      EXECUTOR_SERVICE.submit(new Runnable() {

        @Override
        public void run() {
          cache.refresh(dataset);
        }
      });
    }
    return Response.ok().build();
  }

  @POST
  @Path("/refresh/metricConfig")
  public Response refreshMetricConfigCache() {
    final LoadingCache<MetricDataset, MetricConfigDTO> cache = CACHE_INSTANCE.getMetricConfigCache();
    List<String> datasets = CACHE_INSTANCE.getDatasetsCache().getDatasets();
    for (final String dataset : datasets) {
      EXECUTOR_SERVICE.submit(new Runnable() {

        @Override
        public void run() {
          List<MetricConfigDTO> metricConfigs = DAO_REGISTRY.getMetricConfigDAO().findByDataset(dataset);
          for (MetricConfigDTO metricConfig : metricConfigs) {
            cache.refresh(new MetricDataset(metricConfig.getName(), metricConfig.getDataset()));
          }

        }
      });
    }
    return Response.ok().build();
  }

  // TODO: Clean deprecate endpoint called by our own code
  public Response refreshDimensionFiltersCache() {
    List<String> datasets = CACHE_INSTANCE.getDatasetsCache().getDatasets();
    final LoadingCache<String,String> cache = CACHE_INSTANCE.getDimensionFiltersCache();
    for (final String dataset : datasets) {
      EXECUTOR_SERVICE.submit(new Runnable() {

        @Override
        public void run() {
          cache.refresh(dataset);
        }
      });
    }
    return Response.ok().build();
  }

  // TODO: Clean deprecate endpoint called by our own code
  public Response refreshDatasets() {
    Response response = Response.ok().build();
    CACHE_INSTANCE.getDatasetsCache().expire();
    return response;
  }

}
