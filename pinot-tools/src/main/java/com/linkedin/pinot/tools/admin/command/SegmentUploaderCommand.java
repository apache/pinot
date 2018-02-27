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

import com.google.common.base.Preconditions;
import com.linkedin.pinot.tools.Command;
import com.linkedin.pinot.tools.SchemaInfo;
import org.apache.commons.io.FileUtils;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URL;


public class SegmentUploaderCommand extends AbstractBaseAdminCommand implements Command, Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(SegmentUploaderCommand.class);

    private String _segDir;
    private int _uploadPeriod;

    @Option(name="-help", required=false, help=true, aliases={"-h", "--h", "--help"}, usage="Print this message.")
    private boolean _help = false;

    @Option(name = "-dir", required = true, metaVar = "<String>", usage = "Parent directory to store data & segments.")
    private String _dir;

    @Option(name = "-controllerHost", required = false, metaVar = "<String>", usage = "host name for controller.")
    private String _controllerHost;

    @Option(name = "-controllerPort", required = false, metaVar = "<int>", usage = "Port number for controller.")
    private String _controllerPort = DEFAULT_CONTROLLER_PORT;

    @Override
    public String description() {
        return "Upload segments periodically as per the intervals specified in SchemaInfo.java";
    }

    @Override
    public boolean getHelp() {
        return _help;
    }

    public SegmentUploaderCommand set_controllerHost(String _controllerHost) {
        this._controllerHost = _controllerHost;
        return this;
    }

    public SegmentUploaderCommand set_controllerPort(String _controllerPort) {
        this._controllerPort = _controllerPort;
        return this;
    }

    private SegmentUploaderCommand set_segDir(String _segDir) {
        this._segDir = _segDir;
        return this;
    }

    private SegmentUploaderCommand set_uploadPeriod(int _uploadPeriod) {
        this._uploadPeriod = _uploadPeriod;
        return this;
    }

    private SegmentUploaderCommand addTable(String tableSchema) throws Exception {
        final File tmpDir = new File("temp");

        if (!tmpDir.exists()) {
            Preconditions.checkState(tmpDir.mkdirs());
        }

        File file = new File(tmpDir, "tempSchemaFile.json");
        ClassLoader classLoader = SegmentCreationCommand.class.getClassLoader();
        URL resource = classLoader.getResource(tableSchema);
        com.google.common.base.Preconditions.checkNotNull(resource);
        FileUtils.copyURLToFile(resource, file);

        AddTableCommand adder = new AddTableCommand();
        adder.setFilePath(file.getAbsolutePath())
                .setControllerHost(_controllerHost)
                .setControllerPort(_controllerPort)
                .setExecute(true);
        adder.execute();
        return this;
    }

    private void uploadSegments() throws Exception {
        UploadSegmentCommand uploader = new UploadSegmentCommand();
        uploader.setControllerPort(_controllerPort)
                .setControllerHost(_controllerHost)
                .setSegmentDir(_segDir);
        uploader.execute();
    }

    @Override
    public void run() {
        try {
            uploadSegments();
        }
        catch (Exception e) {
            LOGGER.error("Error while uploading segments: {}", e.getMessage());
        }
    }

    @Override
    public boolean execute() throws Exception {

        String PARENT_FOLDER = _dir + "/";
        String SEG_DIR = "segment" + "/";

        for (int i = 0; i < SchemaInfo.TABLE_NAMES.size(); i++) {

            /* Converting entire duration to seconds */
            int fullUploadPeriod = SchemaInfo.UPLOAD_DURATION.get(i) * 60;

            /* computing wait period for uploading each segment */
            int segmentUploadPeriod = fullUploadPeriod / SchemaInfo.NUM_SEGMENTS.get(i);

            /* Spawning thread for the table */
            Thread t = new Thread(new SegmentUploaderCommand().set_controllerHost(_controllerHost)
                    .set_controllerPort(_controllerPort)
                    .set_segDir(PARENT_FOLDER + SEG_DIR + SchemaInfo.DATA_DIRS.get(i))
                    .set_uploadPeriod(segmentUploadPeriod));
            t.start();

        }

        /* Deleting temporary directory used to store config files */
        FileUtils.deleteDirectory(new File("temp"));

        return true;
    }

}
