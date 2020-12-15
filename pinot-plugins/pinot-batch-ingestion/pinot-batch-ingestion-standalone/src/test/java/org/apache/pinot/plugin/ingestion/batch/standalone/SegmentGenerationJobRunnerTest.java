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
