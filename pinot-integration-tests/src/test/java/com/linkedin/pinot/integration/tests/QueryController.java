package com.linkedin.pinot.integration.tests;

import com.linkedin.pinot.tools.admin.command.PostQueryCommand;

import java.util.*;

public class QueryController {

    public static void main(String[] args) throws Exception {
        PostQueryCommand postQueryCommand = getPostQueryCommand(args[0], args[1]);

        List<QueryExecutor> executorList = createTableExecutors();
        for (QueryExecutor executor : executorList) {
            executor.setPostQueryCommand(postQueryCommand);
            executor.start();
        }
    }

    private static PostQueryCommand getPostQueryCommand(String host, String port) {
        PostQueryCommand postQueryCommand = new PostQueryCommand();
        postQueryCommand.setBrokerPort(port).setBrokerHost(host);
        return postQueryCommand;
    }

    private static List<QueryExecutor> createTableExecutors() {
        List<QueryExecutor> tableExecutorList = QueryExecutor.getTableExecutors();
        return tableExecutorList;
    }

}
