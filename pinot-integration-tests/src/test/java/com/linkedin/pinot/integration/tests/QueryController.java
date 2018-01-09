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
import com.linkedin.pinot.tools.admin.command.SegmentCreationCommand;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class QueryController {
    private static final Logger LOGGER = LoggerFactory.getLogger(SegmentCreationCommand.class);
    private static final int DEFAULT_TEST_DURATION = 60;

    @Option(name = "-brokerHost", required = true, metaVar = "<String>", usage = "host name for controller.")
    private  String _brokerHost = null;

    @Option(name = "-brokerPort", required = true, metaVar = "<int>", usage = "Port number for controller.")
    private  String _brokerPort = null;

    @Option(name = "-testDuration", required = false, metaVar = "<int>", usage = "Port number for controller.")
    private  int _testDuration = DEFAULT_TEST_DURATION;


    public static void main(String[] args) throws Exception {
        QueryController queryController = new QueryController();
        CmdLineParser parser = new CmdLineParser(queryController);
        parser.parseArgument(args);
        if (queryController._brokerHost == null || queryController._brokerPort == null) {
            LOGGER.error("brokerHost or brokerPort not specified");
            return;
        }

        PostQueryCommand postQueryCommand = getPostQueryCommand(queryController._brokerHost, queryController._brokerPort);
        int testDuration = queryController._testDuration;

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
