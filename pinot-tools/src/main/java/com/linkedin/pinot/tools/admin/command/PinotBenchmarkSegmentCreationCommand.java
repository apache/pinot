package com.linkedin.pinot.tools.admin.command;

import com.linkedin.pinot.controller.helix.core.sharding.LatencyBasedLoadMetric;
import com.linkedin.pinot.tools.Command;
import com.linkedin.pinot.tools.pacelab.benchmark.EventTableGenerator;
import org.apache.commons.io.FileUtils;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class PinotBenchmarkSegmentCreationCommand extends AbstractBaseAdminCommand implements Command {
    private static final Logger LOGGER = LoggerFactory.getLogger(CreateSegmentCommand.class);
    @Option(name = "-dataDir", required = false, metaVar = "<string>", usage = "Directory containing the event table data.")
    private String _dataDir;

    @Option(name = "-outDir", required = false, metaVar = "<string>", usage = "Name of output directory.")
    private String _outDir;

    @Option(name = "-overwrite", required = false, usage = "Overwrite existing output directory.")
    private boolean _overwrite = false;

    final String _eventConfig ="pinot_benchmark/?/?";
    final String _tableNameFile = "";


    private void createOutDir(String dirPath) throws Exception
    {
        // Make sure output directory does not already exist, or can be overwritten.
        File outDir = new File(dirPath);
        if (outDir.exists()) {
            if (!_overwrite) {
                throw new IOException("Output directory " + dirPath + " already exists.");
            } else {
                FileUtils.deleteDirectory(outDir);
            }
        }

        outDir.mkdir();
    }


    @Override
    public boolean execute() throws Exception {
        ClassLoader classLoader = PinotBenchmarkSegmentCreationCommand.class.getClassLoader();
        String tableNameFilePath = EventTableGenerator.getFileFromResourceUrl(classLoader.getResource(_tableNameFile));
        List<String> tablesInfo = FileUtils.readLines(new File(tableNameFilePath));

        String configFile = EventTableGenerator.getFileFromResourceUrl(classLoader.getResource(_eventConfig));
        List<String> configLines =  FileUtils.readLines(new File(configFile));


        for(int i=1;i<=tablesInfo.size();i++)
        {
            String[] tableInfoRecord = tablesInfo.get(i).split(",");

            String tableSegmentsDir = _dataDir+"/"+tableInfoRecord[0];
            createOutDir(tableSegmentsDir);

            for(int j=1;j<configLines.size();j++)
            {
                String[] evenDataInfo = configLines.get(j).split(",");

                String eventDataDir = _dataDir + "/" + evenDataInfo[0] + "/" + tableInfoRecord[0];
                String segmentDir = tableSegmentsDir + evenDataInfo[0];

                CreateSegmentCommand createSegmentCommand = new CreateSegmentCommand();
                createSegmentCommand.setDataDir(eventDataDir);
                createSegmentCommand.setOutDir(segmentDir);
                createSegmentCommand.setTableName(tableInfoRecord[0]);
                createSegmentCommand.setSchemaFile(tableInfoRecord[1]);
                createSegmentCommand.setOverwrite(_overwrite);
                createSegmentCommand.setSegmentName(evenDataInfo[i+3]);
                createSegmentCommand.execute();
            }
        }

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
