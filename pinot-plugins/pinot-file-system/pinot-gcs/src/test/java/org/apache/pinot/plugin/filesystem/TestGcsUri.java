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
package org.apache.pinot.plugin.filesystem;

import java.net.URI;
import org.testng.annotations.Test;

import static org.apache.pinot.plugin.filesystem.GcsUri.SCHEME;
import static org.apache.pinot.plugin.filesystem.GcsUri.createGcsUri;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

public class TestGcsUri {
    @Test
    public void testDifferentScheme() {
        URI uri = URI.create("file://bucket/file");
        GcsUri gcsUri = new GcsUri(uri);
        assertEquals(gcsUri.getUri().getScheme(), SCHEME);
    }

    @Test
    public void testNonAbsolutePath() {
        // Relative path must be normalized to absolute path for gcs uri
        // This is because the URI must have an absolute path component,
        // ex. new URI("gs", "bucket",
        GcsUri gcsUri = createGcsUri("bucket", "dir/file");
        assertEquals(gcsUri, createGcsUri("bucket", "/dir/file"));
    }

    @Test
    public void testUnderScoreBucketName() {
        // This is why getAuthority is used instead of getHostName()
        // see https://cloud.google.com/storage/docs/naming-buckets
        // gcs allows _'s which would cause URI.getHost() to be null:
        GcsUri gcsUri = new GcsUri(URI.create("gs://bucket_name/dir"));
        assertEquals(gcsUri.getBucketName(), "bucket_name");
    }

    @Test
    public void testRelativize() {
        GcsUri gcsUri = new GcsUri(URI.create("gs://bucket_name/dir"));
        GcsUri subDir = new GcsUri(URI.create("gs://bucket_name/dir/subdir/file"));
        assertEquals(gcsUri.relativize(subDir), "subdir/file");

        GcsUri nonRelativeGcsUri = new GcsUri(URI.create("gs://bucket_name/other/subdir/file"));
        assertThrows(IllegalStateException.class, () -> gcsUri.relativize(nonRelativeGcsUri));
    }

    @Test
    public void testResolve() {
        GcsUri gcsUri = new GcsUri(URI.create("gs://bucket_name/dir"));
        GcsUri subDir = gcsUri.resolve("subdir/file");
        assertEquals(new GcsUri(URI.create("gs://bucket_name/dir/subdir/file")), subDir);
    }
}
