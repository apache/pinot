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