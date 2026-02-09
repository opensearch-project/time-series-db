/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.reader;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.util.BytesRef;
import org.opensearch.tsdb.core.mapping.LabelStorageType;
import org.opensearch.tsdb.core.model.IndexedByteLabels;
import org.opensearch.tsdb.core.model.Labels;

import java.io.IOException;

/**
 * Label storage implementation using BinaryDocValues with IndexedByteLabels format.
 *
 * <p>Reads bytes stored in the IndexedByteLabels format (header + data) and returns
 * {@link IndexedByteLabels} instances that support O(log n) binary search on label lookups
 * with zero initialization cost.</p>
 *
 * <p>This storage type is used with the {@code BINARY_INDEXED_BYTESLABEL} index setting.
 * The on-disk format is <strong>not compatible</strong> with {@code BINARY} (ByteLabels) storage.</p>
 */
public final class IndexedBinaryLabelsStorage implements LabelsStorage {
    private final BinaryDocValues docValues;

    /**
     * Creates a new indexed binary labels storage.
     *
     * @param docValues the binary doc values containing serialized IndexedByteLabels
     */
    public IndexedBinaryLabelsStorage(BinaryDocValues docValues) {
        this.docValues = docValues;
    }

    @Override
    public LabelStorageType getStorageType() {
        return LabelStorageType.BINARY_INDEXED_BYTESLABEL;
    }

    @Override
    public Labels readLabels(int docId) throws IOException {
        if (docValues == null || !docValues.advanceExact(docId)) {
            return IndexedByteLabels.emptyLabels();
        }
        BytesRef serialized = docValues.binaryValue();
        // Copy bytes because BytesRef might be reused by Lucene
        byte[] copy = new byte[serialized.length];
        System.arraycopy(serialized.bytes, serialized.offset, copy, 0, serialized.length);
        return IndexedByteLabels.fromRawBytes(copy);
    }

    /**
     * Gets the underlying doc values.
     *
     * @return the binary doc values (may be null if field doesn't exist)
     */
    public BinaryDocValues getDocValues() {
        return docValues;
    }
}
