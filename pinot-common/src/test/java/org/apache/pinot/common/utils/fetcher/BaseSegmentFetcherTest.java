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
package org.apache.pinot.common.utils.fetcher;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.retry.AttemptsExceededException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.expectThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;


public class BaseSegmentFetcherTest {
    private static final String TEST_URI_STRING = "test://foo/bar";
    private static final String TEST_DEST_PATH = "/tmp/foo/bar";

    private TestableBaseSegmentFetcher _segmentFetcher;
    private URI _testUri;
    private File _testDestFile;

    @BeforeMethod
    public void setUp() throws Exception {
        _segmentFetcher = new TestableBaseSegmentFetcher();
        _testUri = new URI(TEST_URI_STRING);
        _testDestFile = new File(TEST_DEST_PATH);

        PinotConfiguration config = new PinotConfiguration();
        _segmentFetcher.init(config);
    }

    @Test
    public void testFetchSegmentToLocalSuccessOnFirstAttempt() throws Exception {
        _segmentFetcher.setFetchBehavior(TestableBaseSegmentFetcher.FetchBehavior.SUCCESS);

        _segmentFetcher.fetchSegmentToLocal(_testUri, _testDestFile);

        assertEquals(_segmentFetcher.getFetchAttemptCount(), 1, "Should succeed on first attempt");
        assertTrue(_segmentFetcher.getLastSuccessfulUri().equals(_testUri), "Should use correct URI");
        assertTrue(_segmentFetcher.getLastSuccessfulDest().equals(_testDestFile), "Should use correct destination");
    }

    @Test
    public void testFetchSegmentToLocalSuccessAfterRetries() throws Exception {
        _segmentFetcher.setFetchBehavior(TestableBaseSegmentFetcher.FetchBehavior.FAIL_THEN_SUCCESS);
        _segmentFetcher.setFailureCount(2);

        _segmentFetcher.fetchSegmentToLocal(_testUri, _testDestFile);

        assertEquals(_segmentFetcher.getFetchAttemptCount(), 3, "Should succeed on third attempt after 2 failures");
        assertTrue(_segmentFetcher.getLastSuccessfulUri().equals(_testUri), "Should use the correct URI");
        assertTrue(_segmentFetcher.getLastSuccessfulDest().equals(_testDestFile), "Should use the correct destination");
    }

    @Test
    public void testFetchSegmentToLocalFileNotFoundException() throws Exception {
        _segmentFetcher.setFetchBehavior(TestableBaseSegmentFetcher.FetchBehavior.FILE_NOT_FOUND);

        FileNotFoundException exception = expectThrows(FileNotFoundException.class, () -> {
            _segmentFetcher.fetchSegmentToLocal(_testUri, _testDestFile);
        });

        assertEquals(exception.getMessage(), "File not found: " + TEST_URI_STRING);
        assertEquals(_segmentFetcher.getFetchAttemptCount(), 1, "Should only attempt once for FileNotFoundException");
    }

    @Test
    public void testFetchSegmentToLocalRetryExhaustion() throws Exception {
        _segmentFetcher.setFetchBehavior(TestableBaseSegmentFetcher.FetchBehavior.ALWAYS_FAIL);

        expectThrows(AttemptsExceededException.class, () -> {
            _segmentFetcher.fetchSegmentToLocal(_testUri, _testDestFile);
        });

        assertEquals(_segmentFetcher.getFetchAttemptCount(), 3, "Should exhaust all retry attempts");
    }

    @Test
    public void testFetchSegmentToLocalCustomRetryConfiguration() throws Exception {
        PinotConfiguration config = new PinotConfiguration();
        config.setProperty(BaseSegmentFetcher.RETRY_COUNT_CONFIG_KEY, 2);
        config.setProperty(BaseSegmentFetcher.RETRY_WAIT_MS_CONFIG_KEY, 50);
        config.setProperty(BaseSegmentFetcher.RETRY_DELAY_SCALE_FACTOR_CONFIG_KEY, 2.0);
        _segmentFetcher.init(config);

        _segmentFetcher.setFetchBehavior(TestableBaseSegmentFetcher.FetchBehavior.ALWAYS_FAIL);

        expectThrows(AttemptsExceededException.class, () -> {
            _segmentFetcher.fetchSegmentToLocal(_testUri, _testDestFile);
        });

        assertEquals(_segmentFetcher.getFetchAttemptCount(), 2, "Should use custom retry count");
    }

    @Test
    public void testFetchSegmentToLocalMixedExceptions() throws Exception {
        _segmentFetcher.setFetchBehavior(TestableBaseSegmentFetcher.FetchBehavior.MIXED_EXCEPTIONS);

        _segmentFetcher.fetchSegmentToLocal(_testUri, _testDestFile);

        assertTrue(_segmentFetcher.getFetchAttemptCount() > 1, "Should have made multiple attempts");
        assertTrue(_segmentFetcher.getLastSuccessfulUri().equals(_testUri), "Should have succeeded eventually");
    }

    @Test
    public void testFetchSegmentToLocalFileNotFoundWrapperExceptionUnwrapping() throws Exception {
        _segmentFetcher.setFetchBehavior(TestableBaseSegmentFetcher.FetchBehavior.FILE_NOT_FOUND);
        try {
            _segmentFetcher.fetchSegmentToLocal(_testUri, _testDestFile);
            fail("Expected FileNotFoundException to be thrown");
        } catch (FileNotFoundException e) {
            // Verify that the original FileNotFoundException is thrown (not the wrapper)
            assertEquals(e.getMessage(), "File not found: " + TEST_URI_STRING);
            assertEquals(_segmentFetcher.getFetchAttemptCount(), 1, "Should only attempt once for FileNotFoundException");
        }
    }

    /**
     * Testable implementation of BaseSegmentFetcher for unit testing
     */
    private static class TestableBaseSegmentFetcher extends BaseSegmentFetcher {

        public enum FetchBehavior {
            SUCCESS,
            ALWAYS_FAIL,
            FAIL_THEN_SUCCESS,
            FILE_NOT_FOUND,
            MIXED_EXCEPTIONS
        }

        private FetchBehavior _fetchBehavior = FetchBehavior.SUCCESS;
        private int _failureCount = 0;
        private final AtomicInteger _fetchAttemptCount = new AtomicInteger(0);
        private final AtomicInteger _currentAttempt = new AtomicInteger(0);
        private URI _lastSuccessfulUri;
        private File _lastSuccessfulDest;

        public void setFetchBehavior(FetchBehavior behavior) {
            _fetchBehavior = behavior;
            _currentAttempt.set(0);
        }

        public void setFailureCount(int count) {
            _failureCount = count;
        }

        public int getFetchAttemptCount() {
            return _fetchAttemptCount.get();
        }

        public URI getLastSuccessfulUri() {
            return _lastSuccessfulUri;
        }

        public File getLastSuccessfulDest() {
            return _lastSuccessfulDest;
        }

        @Override
        protected void fetchSegmentToLocalWithoutRetry(URI uri, File dest) throws Exception {
            _fetchAttemptCount.incrementAndGet();
            int attemptNumber = _currentAttempt.incrementAndGet();

            switch (_fetchBehavior) {
                case SUCCESS:
                    _lastSuccessfulUri = uri;
                    _lastSuccessfulDest = dest;
                    return;

                case ALWAYS_FAIL:
                    throw new IOException("Simulated fetch failure for " + uri);

                case FAIL_THEN_SUCCESS:
                    if (attemptNumber <= _failureCount) {
                        throw new IOException("Simulated fetch failure attempt " + attemptNumber + " for " + uri);
                    } else {
                        _lastSuccessfulUri = uri;
                        _lastSuccessfulDest = dest;
                        return;
                    }

                case FILE_NOT_FOUND:
                    throw new FileNotFoundException("File not found: " + uri);

                case MIXED_EXCEPTIONS:
                    if (attemptNumber == 1) {
                        throw new IOException("First attempt failed with IOException");
                    } else if (attemptNumber == 2) {
                        throw new RuntimeException("Second attempt failed with RuntimeException");
                    } else {
                        _lastSuccessfulUri = uri;
                        _lastSuccessfulDest = dest;
                        return;
                    }

                default:
                    throw new IllegalStateException("Unknown fetch behavior: " + _fetchBehavior);
            }
        }
    }
}
