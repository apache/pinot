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

public class AdsQueryExecutor extends QueryExecutor{
    private static AdsQueryExecutor instance;
    private final String CONFIG_FILE = "AdsConfig.properties";
    private static final String[] QUERIES = getQueries();

    private static String[] getQueries() {
        String[] queries = {
                "SELECT * from Ads WHERE adPostTime > %d",
                "SELECT category, MIN(ranking), MAX(ranking), MIN(popularityScore)," +
                    " MAX(popularityScore),COUNT(*), MIN(earning), AVG(earning)," +
                    " MAX(earning) FROM Ads WHERE adPostTime > %d" +
                    " AND ranking > %d" +
                    " GROUP BY category",
                "SELECT company, title, costOfMaking, viewersCount," +
                    " earning, COUNT(*), MIN(ranking), MAX(ranking), MIN(popularityScore)," +
                    " AVG(popularityScore), MAX(popularityScore) FROM Ads" +
                    " WHERE adPostTime > %d" +
                    " AND ranking BETWEEN %d AND %d" +
                    " GROUP BY company, title, costOfMaking, viewersCount, earning",
                "SELECT company, title, costOfMaking, viewersCount," +
                    " earning, COUNT(*) FROM Ads" +
                    " WHERE adPostTime > %d" +
                    " GROUP BY company, title, costOfMaking, viewersCount, earning"
        };
        return queries;
    }

    public String getConfigFile() {
        return CONFIG_FILE;
    }

    public static AdsQueryExecutor getInstance() {
        if (instance == null)
            instance = new AdsQueryExecutor();
        return instance;
    }

    public AdsQueryTask getTask(Properties config) {
        return new AdsQueryTask(config, QUERIES);
    }
}