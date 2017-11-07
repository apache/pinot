package com.linkedin.pinot.integration.tests;

import com.linkedin.pinot.tools.admin.command.PostQueryCommand;
import org.json.JSONObject;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Map;
import java.util.Scanner;

import static com.linkedin.pinot.tools.Quickstart.prettyPrintResponse;

public class QueryExecutor implements Runnable {
    private String tableName;
    private String outputDir;
    private Map<String, String> tableToQueryFileMap;
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

    public QueryExecutor(String tableName, String outputDir, Map<String, String> tableToQueryFileMap) {
        this.tableName = tableName;
        this.outputDir = outputDir;
        this.tableToQueryFileMap = tableToQueryFileMap;
    }

    public void run() {
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
    }

    public static void printStatus(QueryExecutor.Color color, String message) {
        System.out.println(color._code + message + QueryExecutor.Color.RESET._code);
    }


    private static void runQuery(String query) throws Exception {
        printStatus(QueryExecutor.Color.YELLOW, "Total number of documents in the table");
        printStatus(QueryExecutor.Color.CYAN, "Query : " + query);
        printStatus(QueryExecutor.Color.YELLOW, prettyPrintResponse(new JSONObject(new PostQueryCommand().
                setBrokerPort(String.valueOf(8099)).setQuery(query).run())));
        printStatus(QueryExecutor.Color.GREEN, "***************************************************");
    }
}


