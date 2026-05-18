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
package org.apache.bookkeeper.mledger.impl;

import static java.lang.Math.min;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.CustomLog;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ReadEntriesCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerException.ManagedLedgerFencedException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl.State;
import org.apache.bookkeeper.mledger.proto.ManagedLedgerInfo.LedgerInfo;

@CustomLog
class OpReadEntries implements ReadEntriesCallback {
    ManagedLedgerImpl ledger;
    Position readPosition;
    Position maxPosition;
    private int count;
    private CompletableFuture<List<Entry>> promise;

    private List<Entry> entries;
    private Position nextReadPosition;

    static OpReadEntries create(ManagedLedgerImpl ledger, Position readPosition, int count,
                                Position maxPosition, CompletableFuture<List<Entry>> promise) {
        OpReadEntries op = new OpReadEntries();
        op.ledger = ledger;
        op.readPosition = ledger.startReadOperationOnLedger(readPosition);
        op.count = count;
        op.maxPosition = maxPosition;
        op.promise = promise;
        op.entries = new ArrayList<>(count);
        op.nextReadPosition = op.readPosition;
        return op;
    }

    void readEntries() {
        final State state = ManagedLedgerImpl.STATE_UPDATER.get(ledger);
        if (state.isFenced() || state == State.Closed) {
            readEntriesFailed(new ManagedLedgerFencedException(), null);
            return;
        }

        if (readPosition.compareTo(maxPosition) > 0) {
            checkReadCompletion();
            return;
        }

        long ledgerId = readPosition.getLedgerId();
        LedgerHandle currentLedger = ledger.currentLedger;

        if (currentLedger != null && ledgerId == currentLedger.getId()) {
            // Current writing ledger is not in the cache (since we don't want
            // it to be automatically evicted), and we cannot use 2 different
            // ledger handles (read & write)for the same ledger.
            internalReadFromLedger(currentLedger);
        } else {
            LedgerInfo ledgerInfo = ledger.ledgers.get(ledgerId);
            if (ledgerInfo == null || ledgerInfo.getEntries() == 0) {
                updateReadPosition(getNextLedgerPosition(ledgerId));
                checkReadCompletion();
                return;
            }

            ledger.getLedgerHandle(ledgerId).thenAccept(this::internalReadFromLedger)
                    .exceptionally(ex -> {
                        ledger.log.error().attr("position", readPosition).exceptionMessage(ex)
                                .log("Error opening ledger for reading");
                        readEntriesFailed(ManagedLedgerException.getManagedLedgerException(ex.getCause()), null);
                        return null;
                    });
        }
    }

    private void internalReadFromLedger(ReadHandle readHandle) {
        long firstEntry = readPosition.getEntryId();
        long lastEntryInLedger;

        Position lastPosition = ledger.lastConfirmedEntry;

        if (readHandle.getId() == lastPosition.getLedgerId()) {
            // For the current ledger, we only give read visibility to the last entry we have received a confirmation in
            // the managed ledger layer
            lastEntryInLedger = lastPosition.getEntryId();
        } else {
            // For other ledgers, already closed the BK lastAddConfirmed is appropriate
            lastEntryInLedger = readHandle.getLastAddConfirmed();
        }

        if (readHandle.getId() == maxPosition.getLedgerId()) {
            lastEntryInLedger = min(maxPosition.getEntryId(), lastEntryInLedger);
        }

        if (firstEntry > lastEntryInLedger) {
            log.debug().attr("ledgerId", readHandle.getId())
                    .attr("lastEntry", lastEntryInLedger)
                    .attr("readEntry", firstEntry)
                    .log("No more messages to read from ledger");

            LedgerHandle currentLedger = ledger.currentLedger;
            if (currentLedger == null || readHandle.getId() != currentLedger.getId()) {
                updateReadPosition(getNextLedgerPosition(readHandle.getId()));
            } else {
                updateReadPosition(readPosition);
            }

            checkReadCompletion();
            return;
        }

        long lastEntry = min(firstEntry + getNumberOfEntriesToRead() - 1, lastEntryInLedger);

        log.debug().attr("ledgerId", readHandle.getId())
                .attr("firstEntry", firstEntry)
                .attr("lastEntry", lastEntry)
                .log("Reading entries from ledger");
        ledger.asyncReadEntry(readHandle, firstEntry, lastEntry, this, null);
    }

    private Position getNextLedgerPosition(long ledgerId) {
        Long nextLedgerId = ledger.ledgers.ceilingKey(ledgerId + 1);
        return PositionFactory.create(nextLedgerId != null ? nextLedgerId : ledgerId + 1, 0);
    }

    @Override
    public void readEntriesComplete(List<Entry> returnedEntries, Object ctx) {
        try {
            internalReadEntriesComplete(returnedEntries);
        } catch (Throwable throwable) {
            log.error().attr("op", this).exception(throwable)
                    .log("Fallback to readEntriesFailed for exception in readEntriesComplete");
            readEntriesFailed(ManagedLedgerException.getManagedLedgerException(throwable), ctx);
        }
    }

    private void internalReadEntriesComplete(List<Entry> returnedEntries) {
        if (returnedEntries.isEmpty()) {
            log.warn().attr("op", this).log("Read no entries unexpectedly");
            checkReadCompletion();
            return;
        }

        log.debug()
                .attr("managedLedger", ledger.getName())
                .attr("batchSize", returnedEntries.size())
                .attr("cumulativeSize", entries.size())
                .attr("requestedCount", count)
                .log("Read entries succeeded");

        entries.addAll(returnedEntries);
        updateReadPosition(returnedEntries.get(returnedEntries.size() - 1).getPosition().getNext());
        checkReadCompletion();
    }

    @Override
    public void readEntriesFailed(ManagedLedgerException exception, Object ctx) {
        try {
            internalReadEntriesFailed(exception);
        } catch (Throwable throwable) {
            promise.completeExceptionally(ManagedLedgerException.getManagedLedgerException(throwable));
        }
    }

    private void internalReadEntriesFailed(ManagedLedgerException exception) {
        if (!entries.isEmpty()) {
            promise.complete(entries);
            return;
        }

        log.warn()
                .attr("managedLedger", ledger.getName())
                .attr("readPosition", readPosition)
                .exception(exception)
                .log("Read failed from ledger");
        promise.completeExceptionally(exception);
    }

    void updateReadPosition(Position newReadPosition) {
        nextReadPosition = newReadPosition;
    }

    void checkReadCompletion() {
        if (entries.size() < count
                && ledger.hasMoreEntries(nextReadPosition)
                && maxPosition.compareTo(readPosition) > 0) {
            ledger.getExecutor().execute(() -> {
                readPosition = ledger.startReadOperationOnLedger(nextReadPosition);
                readEntries();
            });
        } else {
            promise.complete(entries);
        }
    }

    int getNumberOfEntriesToRead() {
        return count - entries.size();
    }


    @Override
    public String toString() {
        final var ledger = this.ledger;
        final var readPosition = this.readPosition;
        final var maxPosition = this.maxPosition;
        final var nextReadPosition = this.nextReadPosition;
        final var entries = this.entries;
        final var count = this.count;
        if (ledger != null) {
            return ledger.getName() + "{ readPosition: "
                    + (readPosition != null ? readPosition : "(null)") + ", maxPosition: "
                    + (maxPosition != null ? maxPosition : "(null)") + ", nextReadPosition: "
                    + (nextReadPosition != null ? nextReadPosition : "(null)") + ", entries count: "
                    + (entries != null ? entries.size() : "(null)") + ", count: " + count + " }";
        } else {
            return "(null)";
        }
    }
}
