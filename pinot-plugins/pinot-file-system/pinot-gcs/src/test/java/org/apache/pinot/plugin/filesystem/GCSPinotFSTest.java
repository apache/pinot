package org.apache.pinot.plugin.filesystem;

import org.apache.pinot.spi.env.PinotConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.print.URIException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;


@Test
public class GCSPinotFSTest {
    final String DELIMITER = "/";
    final String BUCKET = "test-bucket";
    final String SCHEME = "s3";
    final String FILE_FORMAT = "%s://%s/%s";
    final String DIR_FORMAT = "%s://%s";
    GcsPinotFS gsPinot;
    private static final Logger LOGGER = LoggerFactory.getLogger(GCSPinotFSTest.class);

    @BeforeClass
    public void setUp() {
        PinotConfiguration configuration = new PinotConfiguration();
        gsPinot = new GcsPinotFS();
        gsPinot.init(configuration);
    }

    @AfterClass
    public void tearDown()
            throws IOException {
    }

    private void createEmptyFile(String folderName, String fileName) {
    }

    @Test
    public void testCopy() throws Exception {

        URI srcUri = new URI("gs://test/tmp");
        URI destUri = new URI("gs://test/data");

        try {
//             gsPinot.copy(srcUri, destUri);
             normalizeToDirectoryUri(srcUri);
        } catch (IOException e) {
            LOGGER.error(e.getMessage());
        }
        LOGGER.error("uri before {}, after {}", srcUri, destUri);
    }

    private boolean isPathTerminatedByDelimiter(URI uri) {
        return uri.getPath().endsWith(DELIMITER);
    }

    public URI normalizeToDirectoryUri(URI uri) throws IOException {
        LOGGER.error("uri schema {}, host {}, sanitizePath {}", uri.getScheme(), uri.getHost(), sanitizePath(uri.getPath() + DELIMITER));
        if (isPathTerminatedByDelimiter(uri)) {
            return uri;
        }
        try {
            LOGGER.error("uri schema {}, host {}, sanitizePath {}", uri.getScheme(), uri.getHost(), sanitizePath(uri.getPath() + DELIMITER));
            return new URI(uri.getScheme(), uri.getHost(), sanitizePath(uri.getPath() + DELIMITER), null);
        } catch (URISyntaxException e) {
            throw new IOException(e);
        }
    }

    private String sanitizePath(String path) {
        path = path.replaceAll(DELIMITER + "+", DELIMITER);
        if (path.startsWith(DELIMITER) && !path.equals(DELIMITER)) {
            path = path.substring(1);
        }
        return path;
    }
}


//    public boolean copy(URI srcUri, URI dstUri) throws IOException {
//        LOGGER.error("Copying uri {} to uri {}", srcUri, dstUri);
//        checkState(exists(srcUri), "Source URI '%s' does not exist", srcUri);
//        if (srcUri.equals(dstUri)) {
//            return true;
//        }
//        if (!isDirectory(srcUri)) {
//            delete(dstUri, true);
//            return copyFile(srcUri, dstUri);
//        }
//        dstUri = normalizeToDirectoryUri(dstUri);
//        ImmutableList.Builder<URI> builder = ImmutableList.builder();
//        Path srcPath = Paths.get(srcUri.getPath());
//        try {
//            boolean copySucceeded = true;
//            for (String directoryEntry : listFiles(srcUri, true)) {
//                URI src = new URI(srcUri.getScheme(), srcUri.getHost(), directoryEntry, null);
//
//                String relativeSrcPath = srcPath.relativize(Paths.get(directoryEntry)).toString();
//                String dstPath = dstUri.resolve(relativeSrcPath).getPath();
//
//                URI dst = new URI(dstUri.getScheme(), dstUri.getHost(), dstPath, null);
//                copySucceeded &= copyFile(src, dst);
//            }
//            return copySucceeded;
//        } catch (URISyntaxException e) {
//            throw new IOException(e);
//        }
//    }