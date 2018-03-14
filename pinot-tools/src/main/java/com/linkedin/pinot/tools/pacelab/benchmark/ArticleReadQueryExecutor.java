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

public class ArticleReadQueryExecutor extends QueryExecutor{
    private static ArticleReadQueryExecutor instance;
    private final String CONFIG_FILE = "pinot_benchmark/query_generator_config/ArticleReadConfig.properties";
    private static final String[] QUERIES = getQueries();

    private static String[] getQueries() {
        String[] queries = {
                "SELECT * FROM ArticleRead" +
                    " WHERE  ReadStartTime > %d AND ReadStartTime < %d LIMIT %d",
                "SELECT COUNT(*) FROM ArticleRead" +
                    " WHERE ReadStartTime > %d AND ReadStartTime < %d AND ArticleID = '%s'",
                "SELECT ArticleTopic, COUNT(*), AVG(TimeSpent), AVG(ReaderStrength) FROM ArticleRead" +
                    " WHERE ReadStartTime > %d AND ReadStartTime < %d" +
                    " GROUP BY ArticleTopic ORDER BY COUNT(*) desc LIMIT %d",
                "SELECT ArticleAuthor, ArticleTitle, ReaderStrength, COUNT(*) FROM ArticleRead" +
                    " WHERE ReadStartTime > %d AND ReadStartTime < %d" +
                    " GROUP BY ArticleAuthor, ArticleTitle, ReaderStrength ORDER BY COUNT(*) desc LIMIT %d"
        };
        return queries;
    }

    public String getConfigFile() {
        return CONFIG_FILE;
    }

    public static ArticleReadQueryExecutor getInstance() {
        if (instance == null)
            instance = new ArticleReadQueryExecutor();
        return instance;
    }

    public ArticleReadQueryTask getTask(Properties config) {
        return new ArticleReadQueryTask(config, QUERIES, _dataDir, _testDuration);
    }
}