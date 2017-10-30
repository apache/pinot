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

public class SegmentUploader {
    private static final Logger LOGGER = LoggerFactory.getLogger(SegmentUploader.class);

    public void uploadSegments(String segDir) throws Exception {
        UploadSegmentCommand uploader = new UploadSegmentCommand();
        uploader.setControllerPort(DEFAULT_CONTROLLER_PORT)
                .setSegmentDir(segDir);
        uploader.execute();
    }

    public static void main(String[] args) throws Exception {
        SegmentUploader uploader = new SegmentUploader();
        for (int i = 0; i < SCHEMAS.size(); i++) {
            uploader.uploadSegments("segment/" + SchemaInfo.DATA_DIRS.get(i));
        }
    }
}
