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
package org.apache.bookkeeper.mledger.impl.cache;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ReadEntryUtils {
    private static final Logger log = LoggerFactory.getLogger(ReadEntryUtils.class);

    static CompletableFuture<LedgerEntries> readAsync(ManagedLedger ml, ReadHandle handle, long firstEntry,
                                                      long lastEntry) {
        return readAsync(ml, handle, firstEntry, lastEntry, false, 0);
    }

    static CompletableFuture<LedgerEntries> readAsync(ManagedLedger ml, ReadHandle handle, long firstEntry,
                                                      long lastEntry, boolean batchReadEnabled, int batchReadMaxSize) {
        if (ml.getOptionalLedgerInfo(handle.getId()).isEmpty()) {
            // The read handle comes from another managed ledger, in this case, we can only compare the entry range with
            // the LAC of that read handle. Specifically, it happens when this method is called by a
            // ReadOnlyManagedLedgerImpl object.
            return handle.readAsync(firstEntry, lastEntry);
        }
        // Compare the entry range with the lastConfirmedEntry maintained by the managed ledger because the entry cache
        // of `ShadowManagedLedgerImpl` reads entries via `ReadOnlyLedgerHandle`, which never updates `lastAddConfirmed`
        final var lastConfirmedEntry = ml.getLastConfirmedEntry();
        if (lastConfirmedEntry == null) {
            return CompletableFuture.failedFuture(new ManagedLedgerException(
                    "LastConfirmedEntry is null when reading ledger " + handle.getId()));
        }
        if (handle.getId() > lastConfirmedEntry.getLedgerId()) {
            return CompletableFuture.failedFuture(new ManagedLedgerException("LastConfirmedEntry is "
                    + lastConfirmedEntry + " when reading ledger " + handle.getId()));
        }
        if (handle.getId() == lastConfirmedEntry.getLedgerId() && lastEntry > lastConfirmedEntry.getEntryId()) {
            return CompletableFuture.failedFuture(new ManagedLedgerException("LastConfirmedEntry is "
                    + lastConfirmedEntry + " when reading entry " + lastEntry));
        }

        int numberOfEntries = (int) (lastEntry - firstEntry + 1);

        // Use batch read for multiple entries when enabled.
        if (batchReadEnabled && numberOfEntries > 1 && batchReadMaxSize > 0 && handle instanceof LedgerHandle lh) {
            if (log.isDebugEnabled()) {
                log.debug("Using batch read for ledger {} entries {}-{}, maxCount={}, maxSize={}",
                        handle.getId(), firstEntry, lastEntry, numberOfEntries, batchReadMaxSize);
            }
            return batchReadWithAutoRefill(lh, firstEntry, numberOfEntries, batchReadMaxSize);
        }

        return handle.readUnconfirmedAsync(firstEntry, lastEntry);
    }

    private static CompletableFuture<LedgerEntries> batchReadWithAutoRefill(LedgerHandle lh, long firstEntry,
                                                                            int maxCount, int maxSize) {
        List<LedgerEntry> receivedEntries = new ArrayList<>(maxCount);
        List<LedgerEntries> ledgerEntries = new ArrayList<>(4);

        CompletableFuture<LedgerEntries> future = new CompletableFuture<>();
        batchRead(lh, firstEntry, maxCount, maxCount, maxSize, receivedEntries, ledgerEntries)
                .whenComplete((v, t) -> {
                    if (t != null) {
                        ledgerEntries.forEach(LedgerEntries::close);
                        future.completeExceptionally(t);
                    } else if (receivedEntries.isEmpty()) {
                        ledgerEntries.forEach(LedgerEntries::close);
                        future.completeExceptionally(new ManagedLedgerException(
                                "Batch read returned no entries for ledger " + lh.getId()
                                        + " starting from entry " + firstEntry));
                    } else {
                        future.complete(CompositeLedgerEntriesImpl.create(receivedEntries, ledgerEntries));
                    }
                });
        return future;
    }


    private static CompletableFuture<Void> batchRead(LedgerHandle lh, long firstEntry, int maxCount,
                                                     int entriesToRead, int maxSize, List<LedgerEntry> receivedEntries,
                                                     List<LedgerEntries> ledgerEntries) {
        return lh.batchReadAsync(firstEntry, entriesToRead, maxSize)
                .thenCompose(entries -> {
                    long lastReceivedEntry = -1;
                    int prevReceivedCount = receivedEntries.size();
                    for (LedgerEntry entry : entries) {
                        receivedEntries.add(entry);
                        lastReceivedEntry = entry.getEntryId();
                    }
                    // Add LedgerEntries, it needs recycle.
                    ledgerEntries.add(entries);
                    int currentReceivedCount = receivedEntries.size();
                    // Return if we have enough entries or no more entries available
                    if (currentReceivedCount >= maxCount) {
                        return CompletableFuture.completedFuture(null);
                    }
                    // If no entries returned, we've reached the end of available entries
                    if (prevReceivedCount == currentReceivedCount) {
                        return CompletableFuture.completedFuture(null);
                    }
                    // If it still has more entries to read.
                    long nextReadEntry = lastReceivedEntry + 1;
                    int nextRoundEntriesToRead = maxCount - currentReceivedCount;
                    return batchRead(lh, nextReadEntry, maxCount, nextRoundEntriesToRead, maxSize,
                            receivedEntries, ledgerEntries);
                });
    }
}
