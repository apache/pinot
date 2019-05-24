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


package com.linkedin.pinot.tools.admin.command;

import com.linkedin.pinot.tools.Command;
import com.linkedin.pinot.tools.pacelab.benchmark.QueryExecutor;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.*;
import java.util.*;

public class PinotBenchmarkQueryGeneratorCommand extends AbstractBaseAdminCommand implements Command {
    private static final Logger LOGGER = LoggerFactory.getLogger(SegmentCreationCommand.class);
    private static final int DEFAULT_TEST_DURATION = 60;

    @Option(name = "-brokerHost", required = true, metaVar = "<String>", usage = "Host name for broker.")
    private  String _brokerHost;

    @Option(name = "-brokerPort", required = true, metaVar = "<String>", usage = "Port number for broker.")
    private  String _brokerPort;

    @Option(name = "-testDuration", required = false, metaVar = "<int>", usage = "Test duration.")
    private  int _testDuration = DEFAULT_TEST_DURATION;

    @Option(name = "-dataDir", required = false, metaVar = "<string>", usage = "Directory containing the data.")
    private String _dataDir;

    @Option(name = "-recordFile", required = false, metaVar = "<String>", usage = "File containing the Traces of CPU.")
    private String _recordFile;

    @Option(name = "-slotDuration", required = false, metaVar = "<int>", usage = "Slot duration.")
    private  int _slotDuration = DEFAULT_TEST_DURATION;

    @Option(name = "-useCPUMap", required = false, metaVar = "<int>", usage = "Test using CPU load mappings")
    private int _useCPUMap = 0;

    public PinotBenchmarkQueryGeneratorCommand setBrokerHost (String brokerHost) {
        _brokerHost = brokerHost;
        return this;
    }

    public PinotBenchmarkQueryGeneratorCommand setBrokerPort (String brokerPort) {
        _brokerPort = brokerPort;
        return this;
    }

    public PinotBenchmarkQueryGeneratorCommand setTestDuration (int testDuration) {
        _testDuration = testDuration;
        return this;
    }

    List<QueryExecutor> executorList;
    @Override
    public boolean execute() throws Exception {
        PostQueryCommand postQueryCommand = new PostQueryCommand();
        postQueryCommand.setBrokerPort(_brokerPort);
        postQueryCommand.setBrokerHost(_brokerHost);
	executorList = QueryExecutor.getTableExecutors();

        for (QueryExecutor executor : executorList) {
            executor.setPostQueryCommand(postQueryCommand);
            executor.setDataDir(_dataDir);
            executor.setRecordFile(_recordFile);
            executor.setTestDuration(_testDuration);
            executor.setSlotDuration(_slotDuration);
            executor.setUseCPUMap(_useCPUMap);
	    //    executor.start();
        }
	Thread t1 = new Thread() {
		public void run() {
			try {
				executorList.get(0).start();
			}
			catch (InterruptedException e) {
                                System.out.println("EXCEPTION " + e);
			}
		}
	};
	Thread t2 = new Thread() {
                public void run() {
                        try {
                                executorList.get(1).start();
                        }
                        catch (InterruptedException e) {
                                System.out.println("EXCEPTION " + e);
                        }
                }
        };
	Thread t3 = new Thread() {
                public void run() {
                        try {
                                executorList.get(2).start();
                        }
                        catch (InterruptedException e) {
                                System.out.println("EXCEPTION " + e);
                        }
                }
        };
	t1.start();
	t2.start();
	t3.start();
	t1.join();
	t2.join();
	t3.join();
        return true;
    }

    @Override
    public String description() {
        return null;
    }

    @Override
    public boolean getHelp() {
        return false;
    }
}
