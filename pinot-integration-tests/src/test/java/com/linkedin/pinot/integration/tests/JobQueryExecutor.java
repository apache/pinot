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

public class JobQueryExecutor extends QueryExecutor{
    private static JobQueryExecutor instance;
    private final String CONFIG_FILE = "JobConfig.properties";
    private static final String[] QUERIES = getQueries();

    private static String[] getQueries() {
        String[] queries = {
                "SELECT * from Job " +
                        "WHERE jobPostTime > %d",
                "SELECT industry, MIN(baseSalary), AVG(baseSalary)," +
                        "MAX(baseSalary),COUNT(*), AVG(stock), MIN(pto), MAX(pto)," +
                        "AVG(signOnBonus) " + "FROM Job " +
                        "WHERE jobPostTime > %d" +
                        " AND experience > %d" +
                        " GROUP BY industry",
                "SELECT company, title, location, stock, baseSalary, bonus," +
                        "COUNT(*), AVG(stock), MIN(pto), MAX(pto), AVG(annualBonus)," +
                        "MIN(experience) FROM Job " +
                        "WHERE jobPostTime > %d" +
                        " AND experience BETWEEN %d AND %d" +
                        " GROUP BY company, title, location, stock, baseSalary, signOnBonus",
                "SELECT company, title, location, stock, baseSalary, signOnBonus," +
                        "COUNT(*) FROM Job " +
                        "WHERE jobPostTime > %d" +
                        " GROUP BY company, title, location, stock, baseSalary, signOnBonus"
        };
        return queries;
    }

    public String getConfigFile() {
        return CONFIG_FILE;
    }

    public static JobQueryExecutor getInstance() {
        if (instance == null)
            instance = new JobQueryExecutor();
        return instance;
    }

    public JobQueryTask getTask(Properties config) {
        return new JobQueryTask(config, QUERIES);
    }
}