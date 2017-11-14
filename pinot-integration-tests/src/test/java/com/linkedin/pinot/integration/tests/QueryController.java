package com.linkedin.pinot.integration.tests;

import java.util.*;

public class QueryController {

    public static void main(String[] args) throws Exception {
        List<QueryExecutor> executorList = createTableExecutors();
        for (QueryExecutor executor : executorList)
            executor.start();
    }

    private static List<QueryExecutor> createTableExecutors() {
        List<QueryExecutor> tableExecutorList = QueryExecutor.getTableExecutors();
        return tableExecutorList;
    }

}
