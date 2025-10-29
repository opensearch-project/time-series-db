/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.utils;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Comparator;

public class Files {

    /**
     * Recursively deletes a directory and all its contents.
     * <p>
     * This method uses a depth-first traversal with reverse ordering to ensure
     * files are deleted before their parent directories. If the directory doesn't
     * exist, the method returns without error. Care should be taken  by the caller
     * to ensure the directory is not in use.
     *</p>
     * @param delPath the path to the directory to delete
     * @throws IOException      if there's an error walking the directory tree
     * @throws RuntimeException if there's an error deleting individual files or directories
     */
    public static void deleteDirectory(Path delPath) throws IOException {
        if (!java.nio.file.Files.exists(delPath)) {
            return;
        }

        try (var s = java.nio.file.Files.walk(delPath)) {
            for (var path : s.sorted(Comparator.reverseOrder()).toList()) {
                java.nio.file.Files.delete(path);
            }
        }
    }
}
