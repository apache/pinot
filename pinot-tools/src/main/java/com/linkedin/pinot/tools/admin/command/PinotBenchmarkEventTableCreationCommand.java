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
import com.linkedin.pinot.tools.pacelab.benchmark.EventTableGenerator;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PinotBenchmarkEventTableCreationCommand extends AbstractBaseAdminCommand implements Command {
    private static final Logger LOGGER = LoggerFactory.getLogger(CreateSegmentCommand.class);
    @Option(name = "-dataDir", required = false, metaVar = "<string>", usage = "Directory containing the data.")
    private String _dataDir;

    @Option(name = "-outDir", required = false, metaVar = "<string>", usage = "Name of output directory.")
    private String _outDir;

    @Option(name = "-overwrite", required = false, usage = "Overwrite existing output directory.")
    private boolean _overwrite = false;

    @Option(name = "-numRecords", required = true, metaVar = "<int>", usage = "Number of records to generate.")
    private int _numRecords = 1000;

    public PinotBenchmarkEventTableCreationCommand setDataDir(String dataDir) {
        _dataDir = dataDir;
        return this;
    }


    public PinotBenchmarkEventTableCreationCommand setOutDir(String outDir) {
        _outDir = outDir;
        return this;
    }


    public PinotBenchmarkEventTableCreationCommand setOverwrite(boolean overwrite) {
        _overwrite = overwrite;
        return this;
    }

    @Override
    public boolean execute() throws Exception {
        EventTableGenerator eventTableGenerator = new EventTableGenerator(_dataDir,_outDir, _numRecords);
        eventTableGenerator.generateProfileViewTable();
        eventTableGenerator.generateAdClickTable();
        eventTableGenerator.generateArticleReadTable();
        eventTableGenerator.generateJobApplyTable();

        return false;
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
