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
package com.linkedin.pinot.tools.pacelab.benchmark;

import java.util.Properties;


public class CompanySearchQueryExecutor extends QueryExecutor{
    private static CompanySearchQueryExecutor instance;
    private final String CONFIG_FILE = "CompanySearchAppearanceConfig.properties";
    private static final String[] QUERIES = getQueries();

    private static String[] getQueries() {
        String[] queries = {
                "SELECT COUNT(*) FROM CompanySearchAppearance" +
                        "%s",
                "SELECT COUNT(*) FROM CompanySearchAppearance" +
                        " WHERE ViewedProfileId = '%s'" + "%s",
                "SELECT CompanyDomain,COUNT(*), AVG(TimeSpent) FROM CompanySearchAppearance" +
                        "%s" +
                        " GROUP BY CompanyDomain TOP %d",
                "SELECT CompanyDomain, Location, CompanySize,COUNT(*),AVG(TimeSpent) FROM CompanySearchAppearance" +
                        "%s" +
                        " GROUP BY CompanyDomain, Location, CompanySize TOP %d"
        };
        return queries;
    }

    public String getConfigFile() {
        return CONFIG_FILE;
    }

    public static CompanySearchQueryExecutor getInstance() {
        if (instance == null)
            instance = new CompanySearchQueryExecutor();
        return instance;
    }

    public CompanySearchQueryTask getTask(Properties config) {
        return new CompanySearchQueryTask(config, QUERIES, _dataDir, _testDuration, getCriteria(Constant.MAX_SEARCH_START_TIME,Constant.MIN_SEARCH_START_TIME));
    }
}