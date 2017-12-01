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

import com.linkedin.pinot.tools.admin.command.PostQueryCommand;

import java.util.*;

public class QueryController {

    public static void main(String[] args) throws Exception {
        PostQueryCommand postQueryCommand = getPostQueryCommand(args[0], args[1]);
        int testDuration = Integer.parseInt(args[2]);

        List<QueryExecutor> executorList = createTableExecutors();
        for (QueryExecutor executor : executorList) {
            executor.setPostQueryCommand(postQueryCommand);
            executor.setTestDuration(testDuration);
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
