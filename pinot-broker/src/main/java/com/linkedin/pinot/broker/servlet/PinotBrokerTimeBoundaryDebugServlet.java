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

package com.linkedin.pinot.broker.servlet;

import com.linkedin.pinot.common.config.TableNameBuilder;
import com.linkedin.pinot.common.utils.CommonConstants;
import com.linkedin.pinot.routing.TimeBoundaryService;
import java.io.IOException;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/*
 * A servlet to print the time boundary info for a table, if it exists.
 * Examples:
 *   curl broker:port/debug/timeBoundary/table will print the time boundar of offline table.
 *   curl broker:port/debug/timeBoundary/table_OFFLINE will do the same as above.
 *
 * Realtime tables do not have time boundary as of now, so it will return an empty json object
 *
 */
public class PinotBrokerTimeBoundaryDebugServlet extends HttpServlet {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotBrokerTimeBoundaryDebugServlet.class);

  private TimeBoundaryService _timeBoundaryService;

  @Override
  public void init(ServletConfig config) throws ServletException {
    _timeBoundaryService = (TimeBoundaryService) config.getServletContext().getAttribute(TimeBoundaryService.class.toString());
  }

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    String tableName = req.getPathInfo();
    final String emptyResponse = "{}";
    resp.setContentType("application/json");
    boolean found = false;
    try {
      if (tableName.startsWith("/")) {
        // Drop leading slash
        tableName = tableName.substring(1);
      }
      if (!tableName.isEmpty()) {
        CommonConstants.Helix.TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableName);
        if (tableType == null) {
          tableName = TableNameBuilder.OFFLINE.tableNameWithType(tableName);
        }
        TimeBoundaryService.TimeBoundaryInfo tbInfo = _timeBoundaryService.getTimeBoundaryInfoFor(tableName);
        if (tbInfo != null) {
          resp.getOutputStream().print(tbInfo.toJsonString());
          found = true;
        }
      }
    } catch (Exception e) {
      // Ignore exceptions
    }
    if (!found) {
      resp.getOutputStream().print(emptyResponse);
    }
    resp.getOutputStream().flush();
    resp.getOutputStream().close();
  }
}
