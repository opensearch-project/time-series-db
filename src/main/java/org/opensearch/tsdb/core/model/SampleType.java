/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.model;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;

import java.io.IOException;

/**
 * Enum representing different types of time series samples.
 * Used for type-safe serialization and deserialization of samples.
 */
public enum SampleType implements Writeable {
    /** Floating-point sample type */
    FLOAT_SAMPLE((byte) 0),
    /** Sum-count sample type for averaging */
    SUM_COUNT_SAMPLE((byte) 1),
    /** Multi-value sample type for efficient percentile calculations */
    MULTI_VALUE_SAMPLE((byte) 2),
    /** Min-max sample type for range calculations */
    MIN_MAX_SAMPLE((byte) 3);

    private final byte id;

    /**
     * Constructs a SampleType with the specified ID.
     *
     * @param id the byte identifier
     */
    SampleType(byte id) {
        this.id = id;
    }

    /**
     * Returns the byte identifier for this sample type.
     *
     * @return the byte ID
     */
    public byte getId() {
        return id;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeByte(id);
    }

    /**
     * Read a SampleType from the input stream.
     *
     * @param in the input stream
     * @return the sample type
     * @throws IOException if an I/O error occurs or unknown sample type is encountered
     */
    public static SampleType readFrom(StreamInput in) throws IOException {
        byte id = in.readByte();
        return fromId(id);
    }

    /**
     * Get SampleType by its byte ID.
     *
     * @param id the byte identifier
     * @return the sample type
     * @throws IllegalArgumentException if the ID is not recognized
     */
    public static SampleType fromId(byte id) {
        for (SampleType type : values()) {
            if (type.id == id) {
                return type;
            }
        }
        throw new IllegalArgumentException("Unknown sample type ID: " + id);
    }
}
