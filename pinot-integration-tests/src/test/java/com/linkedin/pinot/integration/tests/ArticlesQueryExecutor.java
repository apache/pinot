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

public class ArticlesQueryExecutor extends QueryExecutor{
    private static ArticlesQueryExecutor instance;
    private final String CONFIG_FILE = "ArticlesConfig.properties";
    private static final String[] QUERIES = getQueries();

    private static String[] getQueries() {
        String[] queries = {
                "SELECT * from Articles" +
                    " WHERE articlePostTime > %d",
                "SELECT category, MIN(ranking), AVG(ranking), MAX(ranking)," +
                    " MIN(popularityScore), AVG(popularityScore), MAX(popularityScore)," +
                    " COUNT(*) FROM Articles" +
                    " WHERE articlePostTime > %d AND ranking > %d" +
                    " GROUP BY category",
                "SELECT category, headline, posterId, sourceLink, COUNT(*)," +
                    " MIN(ranking), MAX(ranking), MIN(popularityScore), AVG(popularityScore)," +
                    " MAX(popularityScore) FROM Articles" +
                    " WHERE articlePostTime > %d" +
                    " AND ranking BETWEEN %d AND %d" +
                    " GROUP BY category, headline, posterId, sourceLink",
                "SELECT category, headline, sourceLink, posterId, COUNT(*)" +
                    " FROM Articles WHERE articlePostTime > %d" +
                    " GROUP BY category, headline, sourceLink, posterId"
        };
        return queries;
    }

    public String getConfigFile() {
        return CONFIG_FILE;
    }

    public static ArticlesQueryExecutor getInstance() {
        if (instance == null)
            instance = new ArticlesQueryExecutor();
        return instance;
    }

    public ArticlesQueryTask getTask(Properties config) {
        return new ArticlesQueryTask(config, QUERIES);
    }
}