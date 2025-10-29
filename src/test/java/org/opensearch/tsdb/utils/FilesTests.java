/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.utils;

import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class FilesTests extends OpenSearchTestCase {
    public void testDeleteNotEmptyDirectory() throws IOException {
        Path parentDir = createTempDir("testDeleteDirectory");
        Path firstChildDir = Files.createDirectories(parentDir.resolve("childDir1"));
        Path secondChildDir = Files.createDirectories(parentDir.resolve("childDir2"));
        org.opensearch.tsdb.core.utils.Files.deleteDirectory(parentDir);
        assertFalse(Files.exists(firstChildDir));
        assertFalse(Files.exists(secondChildDir));
        assertFalse(Files.exists(parentDir));
    }

    public void testDeleteNotExistentDirectory() {
        try {
            org.opensearch.tsdb.core.utils.Files.deleteDirectory(Path.of("testDeleteDirectory"));
        } catch (Exception e) {
            fail("Deleting a non-existent directory should not fail: " + e.getMessage());
        }
    }
}
