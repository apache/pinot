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
package org.apache.pinot.core.segment.creator.impl.presence;

import org.apache.commons.io.FileUtils;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;

public class PresenceVectorCreatorTest {
    private static final File TEMP_DIR = new File(FileUtils.getTempDirectory(), "PresenceVectorCreatorTest");
    private static final String COLUMN_NAME = "test";
    private static final String PRESENCE_FILE = "test.bitmap.presence";

    @BeforeClass
    public void setUp()
            throws Exception {
        if (TEMP_DIR.exists()) {
            FileUtils.deleteQuietly(TEMP_DIR);
        }
        TEMP_DIR.mkdir();
    }

    @Test
    public void testPresenceVectorCreation() {
        try (PresenceVectorCreator creator = new PresenceVectorCreator(TEMP_DIR, COLUMN_NAME)) {
            for (int i = 0; i < 100; i++) {
                creator.setNull(i);
            }
            ImmutableRoaringBitmap nullBitmap = creator.getNullBitmap();
            for (int i = 0; i < 100; i++) {
                Assert.assertTrue(nullBitmap.contains(i));
            }
        } catch (IOException e) {
            Assert.fail("Unable to create a valid PresenceVectorCreator object", e);
        }

        Assert.assertEquals(TEMP_DIR.list().length, 1);
        Assert.assertEquals(PRESENCE_FILE, TEMP_DIR.list()[0]);
    }

    @AfterClass
    public void tearDown()
            throws Exception {
        FileUtils.deleteDirectory(TEMP_DIR);
    }
}
