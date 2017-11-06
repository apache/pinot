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

import java.util.Timer;
import java.util.TimerTask;

public class SegmentUploader {
    private static final Logger LOGGER = LoggerFactory.getLogger(SegmentUploader.class);

    public static void uploadSegments(String segDir) throws Exception {
        UploadSegmentCommand uploader = new UploadSegmentCommand();
        uploader.setControllerPort(DEFAULT_CONTROLLER_PORT)
                .setSegmentDir(segDir);
        uploader.execute();
    }


    public static void main(String[] args) throws Exception {
        final Timer timer = new Timer();
        timer.scheduleAtFixedRate(new TimerTask() {
            private int index = 0;
            @Override
            public void run() {
                try {
                    uploadSegments("segment/" + SchemaInfo.DATA_DIRS.get(index));
                }
                catch (Exception e) {
                    e.getMessage();
                }
                if (++index == SCHEMAS.size())
                    timer.cancel();

            }
        }, 0, 10000);
    }
}
