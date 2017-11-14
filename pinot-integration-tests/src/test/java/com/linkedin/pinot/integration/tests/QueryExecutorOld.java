package com.linkedin.pinot.integration.tests;

import com.linkedin.pinot.tools.admin.command.PostQueryCommand;
import org.json.JSONObject;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;
import java.io.InputStream;
import java.util.Properties;


import static com.linkedin.pinot.tools.Quickstart.prettyPrintResponse;

public class QueryExecutorOld implements Runnable {
    private String tableName;
    private String outputDir;
    private Map<String, String> tableToQueryFileMap;
    private static final Map<String, String[]> TABLE_TO_QUERY_MAP = createMap();
    private static Properties jobConfig;
    private Properties config;
    private static Map<String, String[]> createMap() {
        Map<String, String[]> result = new HashMap<>();
        result.put("Job", new String[] {
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
        });
        return Collections.unmodifiableMap(result);
    }
    static {
        jobConfig = new Properties();
        try {
            InputStream in = QueryExecutor.class.getClassLoader().getResourceAsStream("JobConfig.properties");
            jobConfig.load(in);
        } catch (FileNotFoundException e) {
            System.out.println("FileNotFoundException");
            e.printStackTrace();
        } catch (IOException e) {
            System.out.println("IOException");
            e.printStackTrace();
        }
    }

    public enum Color {
        RESET("\u001B[0m"),
        GREEN("\u001B[32m"),
        YELLOW("\u001B[33m"),
        CYAN("\u001B[36m");

        private String _code;

        Color(String code) {
            _code = code;
        }
    }

    Random rand;


    public QueryExecutorOld(String tableName, String outputDir, Map<String, String> tableToQueryFileMap) {
        this.tableName = tableName;
        this.outputDir = outputDir;
        this.tableToQueryFileMap = tableToQueryFileMap;
        rand = new Random();
        this.config = jobConfig;
    }

    /*public void run() {
        Scanner scan = null;
        try {
            scan = new Scanner(new File(outputDir + tableToQueryFileMap.get(tableName)));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        while(scan.hasNextLine()){
            String query = scan.nextLine();
            try {
                runQuery(query);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }*/

    public void run() {
        while(true){
            try {
                float[] likelihood = getLikelihoodArrayFromProps();
                float randomLikelihood = rand.nextFloat();

                for (int i = 0; i < likelihood.length; i++) {
                    if (randomLikelihood < likelihood[i]) {
                        generateAndRunQuery(tableName, i);
                        break;
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
    private float[] getLikelihoodArrayFromProps() {
        String[] a = config.getProperty("likelihood").split(",");
        float[] likelihoodArray = new float[a.length];
        for(int i = 0;i < a.length;i++) {
            if (i == 0)
                likelihoodArray[i] = Float.parseFloat(a[i]);
            else
                likelihoodArray[i] = Float.parseFloat(a[i]) + likelihoodArray[i - 1];
        }
        return likelihoodArray;
    }

    public static void printStatus(QueryExecutorOld.Color color, String message) {
        System.out.println(color._code + message + QueryExecutorOld.Color.RESET._code);
    }

    private static void generateAndRunQuery(String tableName, int queryId) throws Exception {
        if (tableName.equals("Job"))
            generateAndRunQueriesForJobTable(TABLE_TO_QUERY_MAP.get(tableName), queryId);
    }

    private static void generateAndRunQueriesForJobTable(String[] queries, int queryId) throws Exception {
        long max_timestamp = Long.parseLong(jobConfig.getProperty("max_timestamp"));
        long min_timestamp = Long.parseLong(jobConfig.getProperty("min_timestamp"));
        int min_experience = Integer.parseInt(jobConfig.getProperty("min_experience"));
        int max_experience = Integer.parseInt(jobConfig.getProperty("max_experience"));


        long timestampRange = max_timestamp - min_timestamp + 1;
        int experienceRange = max_experience - min_experience + 1;
        long timestamp = min_timestamp + (int)(Math.random() * timestampRange);

        switch (queryId) {
            case 0:
                runQuery(String.format(queries[queryId], timestamp));
                break;
            case 1:
                int experience = min_experience + (int)(Math.random() * experienceRange);
                runQuery(String.format(queries[queryId], timestamp, experience));
                break;
            case 2:
                int lowerBound = min_experience + (int)(Math.random() * experienceRange);
                int higherBound = min_experience + (int)(Math.random() * experienceRange);
                if (lowerBound > higherBound) {
                    //swap them
                    int temp = lowerBound;
                    lowerBound = higherBound;
                    higherBound = temp;
                }
                runQuery(String.format(queries[queryId], timestamp, lowerBound, higherBound));
                break;
            case 3:
                runQuery(String.format(queries[queryId], timestamp));
                break;
        }

    }

    private static void runQuery(String query) throws Exception {
        printStatus(QueryExecutorOld.Color.YELLOW, "Total number of documents in the table");
        printStatus(QueryExecutorOld.Color.CYAN, "Query : " + query);
        printStatus(QueryExecutorOld.Color.YELLOW, prettyPrintResponse(new JSONObject(new PostQueryCommand().
                setBrokerPort(String.valueOf(8099)).setQuery(query).run())));
        printStatus(QueryExecutorOld.Color.GREEN, "***************************************************");
    }
}


