package com.linkedin.pinot.integration.tests;

import org.json.JSONObject;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;



public class MyQueryGenerator {
    private static final long MIN_TIMESTAMP = 946684800;
    private static final long MAX_TIMESTAMP = 1508873892;
    private static final int MIN_EXPERIENCE = 1;
    private static final int MAX_EXPERIENCE = 10;

    private static final Map<String, String> TABLE_TO_QUERY_MAP = createMap();
    private static final String OUTPUT_DIR = "pinot-integration-tests/src/test/resources/";

    private static Map<String, String> createMap() {
        Map<String, String> result = new HashMap<>();
        result.put("Job", "JobQueries");
        return Collections.unmodifiableMap(result);
    }

    public static void main(String[] args) throws Exception {
        generateQueries();
    }

    private static void generateQueries() {
        long timestampRange = MAX_TIMESTAMP - MIN_TIMESTAMP + 1;
        int experienceRange = MAX_EXPERIENCE - MIN_EXPERIENCE + 1;

        for (Map.Entry<String, String> entry : TABLE_TO_QUERY_MAP.entrySet()) {
            generateQueriesForTable(entry.getValue(), timestampRange, experienceRange);
        }
        /*try {
            executeQueries("Job");
        } catch (Exception e) {
            e.printStackTrace();
        }*/
    }

    private static void generateQueriesForTable(String tableName, long timestampRange, int experienceRange) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(OUTPUT_DIR + tableName))) {
            for (int i = 0; i < 1000; i++) {
                long timestamp = MIN_TIMESTAMP + (int)(Math.random() * timestampRange);
                String query = "SELECT * from Job " +
                                "WHERE jobPostTime > " + timestamp;
                JSONObject queryJson = new JSONObject();
                queryJson.put("pql", query);
                writer.write(query);
                writer.newLine();
            }

            for (int i = 0; i < 1000; i++) {
                long timestamp = MIN_TIMESTAMP + (int)(Math.random() * timestampRange);
                int experience = MIN_EXPERIENCE + (int)(Math.random() * experienceRange);

                String query = "SELECT industry, MIN(baseSalary), AVG(baseSalary)," +
                                "MAX(baseSalary),COUNT(*), AVG(stock), MIN(pto), MAX(pto)," +
                                "AVG(signOnBonus) " + "FROM Job " +
                                "WHERE jobPostTime > " +  timestamp +
                                " AND experience > " + experience +
                                " GROUP BY industry";
                JSONObject queryJson = new JSONObject();
                queryJson.put("pql", query);
                writer.write(query);
                writer.newLine();
            }

            for (int i = 0; i < 1000; i++) {
                long timestamp = MIN_TIMESTAMP + (int)(Math.random() * timestampRange);
                int lowerBound = MIN_EXPERIENCE + (int)(Math.random() * experienceRange);
                int higherBound = MIN_EXPERIENCE + (int)(Math.random() * experienceRange);
                if (lowerBound > higherBound) {
                    //swap them
                    int temp = lowerBound;
                    lowerBound = higherBound;
                    higherBound = temp;
                }

                String query = "SELECT company, title, location, stock, baseSalary, bonus," +
                                "COUNT(*), AVG(stock), MIN(pto), MAX(pto), AVG(annualBonus)," +
                                "MIN(experience) FROM Job " +
                                "WHERE jobPostTime > " + timestamp +
                                " AND experience BETWEEN " + lowerBound + " AND " + higherBound +
                                " GROUP BY company, title, location, stock, baseSalary, signOnBonus";
                JSONObject queryJson = new JSONObject();
                queryJson.put("pql", query);
                writer.write(query);
                writer.newLine();
            }

            for (int i = 0; i < 1000; i++) {
                long timestamp = MIN_TIMESTAMP + (int)(Math.random() * timestampRange);
                String query = "SELECT company, title, location, stock, baseSalary, signOnBonus," +
                                "COUNT(*) FROM Job " +
                                "WHERE jobPostTime > " + timestamp +
                                " GROUP BY company, title, location, stock, baseSalary, signOnBonus";
                        ;
                JSONObject queryJson = new JSONObject();
                queryJson.put("pql", query);
                writer.write(query);
                writer.newLine();
            }


        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private static void executeQueries(String tableName) throws Exception {
        Thread t = new Thread(new QueryExecutor(tableName, OUTPUT_DIR, TABLE_TO_QUERY_MAP));
        t.start();
    }

}
