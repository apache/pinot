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

package org.apache.pinot.thirdeye.datasource.pinot.resources;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.base.Preconditions;
import org.apache.pinot.thirdeye.datasource.ThirdEyeCacheRegistry;
import org.apache.pinot.thirdeye.datasource.pinot.PinotQuery;
import org.apache.pinot.thirdeye.datasource.pinot.PinotThirdEyeDataSource;
import org.apache.pinot.thirdeye.datasource.pinot.resultset.ThirdEyeResultSet;
import org.apache.pinot.thirdeye.datasource.pinot.resultset.ThirdEyeResultSetGroup;
import org.apache.pinot.thirdeye.datasource.pinot.resultset.ThirdEyeResultSetSerializer;
import java.util.concurrent.ExecutionException;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/pinot-data-source")
@Produces(MediaType.APPLICATION_JSON)
public class PinotDataSourceResource {
  private static final Logger LOG = LoggerFactory.getLogger(PinotDataSourceResource.class);
  private static final ObjectMapper OBJECT_MAPPER;
  static {
    SimpleModule module = new SimpleModule("ThirdEyeResultSetSerializer", new Version(1, 0, 0, null, null, null));
    module.addSerializer(ThirdEyeResultSet.class, new ThirdEyeResultSetSerializer());
    OBJECT_MAPPER = new ObjectMapper();
    OBJECT_MAPPER.registerModule(module);
  }
  private static final String URL_ENCODING = "UTF-8";

  private PinotThirdEyeDataSource pinotDataSource;

  /**
   * Returns the JSON string of the ThirdEyeResultSetGroup of the given Pinot query.
   *
   * @param pql       the given Pinot query.
   * @param tableName the table name at which the query targets.
   *
   * @return the JSON string of the ThirdEyeResultSetGroup of the query; the string could be the exception message if
   * the query is not executed successfully.
   */
  @GET
  @Path("/query")
  public String executePQL(@QueryParam("pql") String pql, @QueryParam("tableName") String tableName) {
    initPinotDataSource();
    String resultString;
    PinotQuery pinotQuery = new PinotQuery(pql, tableName);
    try {
      ThirdEyeResultSetGroup thirdEyeResultSetGroup = pinotDataSource.executePQL(pinotQuery);
      resultString = OBJECT_MAPPER.writeValueAsString(thirdEyeResultSetGroup);
    } catch (ExecutionException | JsonProcessingException e) {
      LOG.error("Failed to execute PQL ({}) due to the exception:", pinotQuery);
      // TODO: Expand the definition of ThirdEyeResultSetGroup to include a field of error message?
      resultString = e.toString();
    }

    return resultString;
  }

  /**
   * Initializes the pinot data source if it is still not initialized.
   */
  private void initPinotDataSource() {
    if (pinotDataSource == null) {
      Preconditions.checkNotNull(ThirdEyeCacheRegistry.getInstance(),
          "Failed to get Pinot data source because ThirdEye cache registry is not initialized.");
      pinotDataSource = (PinotThirdEyeDataSource) ThirdEyeCacheRegistry.getInstance().getQueryCache()
          .getDataSource(PinotThirdEyeDataSource.class.getSimpleName());
      Preconditions
          .checkNotNull(pinotDataSource, "Failed to get Pinot data source because it is not initialized in ThirdEye.");
    }
  }
}
