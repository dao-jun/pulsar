/*
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
package org.apache.bookkeeper.mledger;

import java.io.Closeable;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.common.annotation.InterfaceAudience;
import org.apache.bookkeeper.common.annotation.InterfaceStability;
import org.apache.bookkeeper.mledger.util.ManagedLedgerUtils;

/**
 * A cursorless, stateless reader for entries that are currently available in a managed ledger.
 *
 * <p>A random reader does not maintain a read position, acknowledge entries, contribute to backlog, or prevent ledger
 * trimming. A read can therefore fail, or start at the next retained ledger, when ledgers are trimmed concurrently.
 * Reads do not wait for future entries.
 *
 * <p>Callers must release every returned {@link Entry}. Completion can run on a BookKeeper, Netty, or managed-ledger
 * thread; callers that mutate thread-confined state must explicitly select an appropriate executor.
 */
@InterfaceAudience.LimitedPrivate
@InterfaceStability.Evolving
public interface RandomReader extends Closeable {

    /**
     * Read up to {@code numberOfEntries} starting at {@code startPosition}, inclusive.
     */
    CompletableFuture<List<Entry>> read(Position startPosition, int numberOfEntries);

    /**
     * Read up to {@code maxPosition}, inclusive, without a size limit.
     */
    default CompletableFuture<List<Entry>> read(Position startPosition, int numberOfEntries, Position maxPosition) {
        return read(startPosition, numberOfEntries, maxPosition, ManagedLedgerUtils.NO_MAX_SIZE_LIMIT);
    }

    /**
     * Read using an estimated-size limit and no position limit.
     */
    default CompletableFuture<List<Entry>> read(Position startPosition, int numberOfEntries, long maxSizeBytes) {
        return read(startPosition, numberOfEntries, PositionFactory.LATEST, maxSizeBytes);
    }

    /**
     * Read entries subject to count, position, and estimated-size limits.
     *
     * <p>{@code maxPosition} is inclusive. A null value is equivalent to {@link PositionFactory#LATEST}.
     * {@code maxSizeBytes} uses the same estimate-based cap as {@link ManagedCursor}; at least one entry can be
     * returned even when that entry exceeds the requested size.
     *
     * <p>If a storage error occurs after entries have been collected, the future completes successfully with that
     * partial list and the read stops. An error before the first entry completes the future exceptionally.
     */
    CompletableFuture<List<Entry>> read(Position startPosition, int numberOfEntries, Position maxPosition,
                                        long maxSizeBytes);

    /**
     * Unregister this reader. Closing does not cancel reads already in progress.
     */
    @Override
    void close();
}
