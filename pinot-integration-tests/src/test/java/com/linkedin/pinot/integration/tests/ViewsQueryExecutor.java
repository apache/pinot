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
package com.linkedin.pinot.integration.tests;

import java.util.Properties;

public class ViewsQueryExecutor extends QueryExecutor{
    private static ViewsQueryExecutor instance;
    private final String CONFIG_FILE = "ViewsConfig.properties";
    private static final String[] QUERIES = getQueries();

    private static String[] getQueries() {
        String[] queries = {
                "SELECT * from Views" +
                    " WHERE profileViewedTimestamp > %d",
                "SELECT viewerCompany, MIN(weeklyChange), MAX(weeklyChange)," +
                    " MIN(monthlyChange), MAX(monthlyChange), COUNT(*)," +
                    " MIN(appearancesInSearch), AVG(appearancesInSearch)," +
                    " MAX(appearancesInSearch) FROM Views" +
                    " WHERE profileViewedTimestamp > %d" +
                    " AND weeklyChange > %d" +
                    " GROUP BY viewerCompany",
                "SELECT viewerCompany, connectionDuration, appearancesInSearch," +
                    " COUNT(*), MIN(weeklyChange), MAX(weeklyChange), MIN(monthlyChange)," +
                    " MAX(monthlyChange), COUNT(*), MIN(appearancesInSearch)," +
                    " AVG(appearancesInSearch), MAX(appearancesInSearch) FROM Views" +
                    " WHERE profileViewedTimestamp > %d" +
                    " AND weeklyChange BETWEEN %d AND %d" +
                    " GROUP BY viewerCompany, connectionDuration, appearancesInSearch",
                "SELECT viewerId, viewerLocation, viewerTitle, connectionDuration," +
                    " appearancesInSearch, COUNT(*)" +
                    " FROM Views WHERE profileViewedTimestamp > %d" +
                    " GROUP BY viewerId, viewerLocation, viewerTitle," +
                    " connectionDuration, appearancesInSearch"
        };
        return queries;
    }

    public String getConfigFile() {
        return CONFIG_FILE;
    }

    public static ViewsQueryExecutor getInstance() {
        if (instance == null)
            instance = new ViewsQueryExecutor();
        return instance;
    }

    public ViewsQueryTask getTask(Properties config) {
        return new ViewsQueryTask(config, QUERIES);
    }
}