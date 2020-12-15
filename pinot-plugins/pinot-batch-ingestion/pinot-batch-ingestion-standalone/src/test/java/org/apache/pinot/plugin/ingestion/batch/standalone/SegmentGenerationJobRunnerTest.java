/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.plugin.ingestion.batch.standalone;

import org.testng.Assert;
import org.testng.annotations.Test;
import java.net.URI;
import java.net.URISyntaxException;

public class SegmentGenerationJobRunnerTest {

    // Don't lose authority portion of inputDirURI when creating output files
    // https://github.com/apache/incubator-pinot/issues/6355
    
    @Test
    public void testGetFileURI() throws Exception {
        // Typical file URI
        validateFileURI(new URI("file:/path/to/"));
        
        // Namenode as authority, plus non-standard port
        validateFileURI(new URI("hdfs://namenode:9999/path/to/"));
        
        // S3 bucket + path
        validateFileURI(new URI("s3://bucket/path/to/"));
        
        // S3 URI with userInfo (username/password)
        validateFileURI(new URI("s3://username:password@bucket/path/to/"));
    }
    
    private void validateFileURI(URI directoryURI) throws URISyntaxException {
        URI fileURI = new URI(directoryURI.toString() + "file");
        String rawPath = fileURI.getRawPath();
        
        Assert.assertEquals(SegmentGenerationJobRunner.getFileURI(rawPath, fileURI).toString(),
                fileURI.toString());

    }
}
