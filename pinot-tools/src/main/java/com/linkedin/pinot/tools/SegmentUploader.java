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
package com.linkedin.pinot.tools;
import com.linkedin.pinot.tools.admin.command.*;
import static com.linkedin.pinot.tools.SchemaInfo.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SegmentUploader implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(SegmentUploader.class);

    private String segDir;
    private int uploadPeriod;

    public SegmentUploader(String segDir, int uploadPeriod) {
        this.segDir = segDir;
        this.uploadPeriod = uploadPeriod;
    }

    public void uploadSegments() throws Exception {
        UploadSegmentCommand uploader = new UploadSegmentCommand();
        uploader.setControllerPort(DEFAULT_CONTROLLER_PORT)
                .setSegmentDir(segDir);
        uploader.execute(uploadPeriod);
    }

    public void run() {
        try {
            uploadSegments();
        }
        catch (Exception e) {
            LOGGER.error(e.getMessage());
        }
    }

    public static void main(String[] args) throws Exception {

        for (int i = 0; i < SchemaInfo.TABLE_NAMES.size(); i++) {
            /* Converting entire duration to seconds */
            int fullUploadPeriod = SchemaInfo.UPLOAD_DURATION.get(i) * 60;

            /* computing wait period for uploading each segment */
            int segmentUploadPeriod = fullUploadPeriod / SchemaInfo.NUM_SEGMENTS.get(i);

            /* Spawning thread for the table */
            Thread t = new Thread(new SegmentUploader("segment/" + SchemaInfo.DATA_DIRS.get(i),
                    segmentUploadPeriod));
            t.start();
        }

    }
}
