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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import io.netty.buffer.Unpooled;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.client.impl.LedgerEntryImpl;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ReadEntryUtilsTest {

    private ManagedLedger ml;
    private LedgerHandle lh;

    @BeforeMethod
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void setup() {
        ml = mock(ManagedLedger.class);
        lh = mock(LedgerHandle.class);
        when(lh.getId()).thenReturn(1L);
        Position lastConfirmedEntry = PositionFactory.create(1L, 99L);
        when(ml.getLastConfirmedEntry()).thenReturn(lastConfirmedEntry);
        // Return non-empty Optional to take the normal managed ledger path
        when(ml.getOptionalLedgerInfo(1L)).thenReturn((Optional) Optional.of(new Object()));
    }

    @Test
    public void testBatchReadSingleBatch() {
        LedgerEntries batchResult = createLedgerEntries(1L, 0, 1, 2, 3, 4);
        when(lh.batchReadAsync(eq(0L), anyInt(), anyLong()))
                .thenReturn(CompletableFuture.completedFuture(batchResult));

        CompletableFuture<LedgerEntries> future =
                ReadEntryUtils.readAsync(ml, lh, 0L, 4L, true, 1024);

        assertThat(future).isCompleted();
        LedgerEntries result = future.getNow(null);
        try {
            List<Long> entryIds = new ArrayList<>();
            for (LedgerEntry e : result) {
                entryIds.add(e.getEntryId());
            }
            assertThat(entryIds).containsExactly(0L, 1L, 2L, 3L, 4L);
        } finally {
            result.close();
        }

        verify(lh, never()).readUnconfirmedAsync(anyLong(), anyLong());
    }

    @Test
    public void testBatchReadMultipleBatches() {
        // First batch returns entries 0-2, second returns 3-4
        LedgerEntries batch1 = createLedgerEntries(1L, 0, 1, 2);
        LedgerEntries batch2 = createLedgerEntries(1L, 3, 4);

        when(lh.batchReadAsync(eq(0L), anyInt(), anyLong()))
                .thenReturn(CompletableFuture.completedFuture(batch1));
        when(lh.batchReadAsync(eq(3L), anyInt(), anyLong()))
                .thenReturn(CompletableFuture.completedFuture(batch2));

        CompletableFuture<LedgerEntries> future =
                ReadEntryUtils.readAsync(ml, lh, 0L, 4L, true, 1024);

        assertThat(future).isCompleted();
        LedgerEntries result = future.getNow(null);
        try {
            List<Long> entryIds = new ArrayList<>();
            for (LedgerEntry e : result) {
                entryIds.add(e.getEntryId());
            }
            assertThat(entryIds).containsExactly(0L, 1L, 2L, 3L, 4L);
        } finally {
            result.close();
        }
    }

    @Test
    public void testBatchReadEmptyResult() {
        LedgerEntries emptyBatch = createLedgerEntries(1L);
        when(lh.batchReadAsync(eq(0L), anyInt(), anyLong()))
                .thenReturn(CompletableFuture.completedFuture(emptyBatch));

        CompletableFuture<LedgerEntries> future =
                ReadEntryUtils.readAsync(ml, lh, 0L, 4L, true, 1024);

        assertThat(future).isCompletedExceptionally();
    }

    @Test
    public void testBatchReadFailure() {
        when(lh.batchReadAsync(eq(0L), anyInt(), anyLong()))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("BK read failed")));
        // Fallback also fails to verify the exception propagates
        when(lh.readUnconfirmedAsync(0L, 4L))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("readUnconfirmed also failed")));

        CompletableFuture<LedgerEntries> future =
                ReadEntryUtils.readAsync(ml, lh, 0L, 4L, true, 1024);

        assertThat(future).isCompletedExceptionally();
        assertThatThrownBy(future::get)
                .hasCauseInstanceOf(RuntimeException.class)
                .hasRootCauseMessage("readUnconfirmed also failed");
    }

    @Test
    public void testBatchReadDisabledFallback() {
        LedgerEntries mockEntries = createLedgerEntries(1L, 0, 1, 2, 3, 4);
        when(lh.readUnconfirmedAsync(0L, 4L))
                .thenReturn(CompletableFuture.completedFuture(mockEntries));

        CompletableFuture<LedgerEntries> future =
                ReadEntryUtils.readAsync(ml, lh, 0L, 4L, false, 1024);

        assertThat(future).isCompleted();
        verify(lh).readUnconfirmedAsync(0L, 4L);
        verify(lh, never()).batchReadAsync(anyLong(), anyInt(), anyLong());

        future.getNow(null).close();
    }

    @Test
    public void testBatchReadSingleEntryFallback() {
        LedgerEntries mockEntries = createLedgerEntries(1L, 0);
        when(lh.readUnconfirmedAsync(0L, 0L))
                .thenReturn(CompletableFuture.completedFuture(mockEntries));

        // Single entry reads should use readUnconfirmedAsync even with batch read enabled
        CompletableFuture<LedgerEntries> future =
                ReadEntryUtils.readAsync(ml, lh, 0L, 0L, true, 1024);

        assertThat(future).isCompleted();
        verify(lh).readUnconfirmedAsync(0L, 0L);
        verify(lh, never()).batchReadAsync(anyLong(), anyInt(), anyLong());

        future.getNow(null).close();
    }

    @Test
    public void testBatchReadWithNonLedgerHandle() {
        ReadHandle rh = mock(ReadHandle.class);
        when(rh.getId()).thenReturn(1L);
        LedgerEntries mockEntries = createLedgerEntries(1L, 0, 1, 2);
        when(rh.readUnconfirmedAsync(0L, 2L))
                .thenReturn(CompletableFuture.completedFuture(mockEntries));

        // ReadHandle (not LedgerHandle) should use readUnconfirmedAsync even with batch read enabled
        CompletableFuture<LedgerEntries> future =
                ReadEntryUtils.readAsync(ml, rh, 0L, 2L, true, 1024);

        assertThat(future).isCompleted();
        verify(rh).readUnconfirmedAsync(0L, 2L);

        future.getNow(null).close();
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void testReadOnlyManagedLedgerFallback() {
        when(ml.getOptionalLedgerInfo(1L)).thenReturn((Optional) Optional.empty());

        ReadHandle rh = mock(ReadHandle.class);
        when(rh.getId()).thenReturn(1L);
        LedgerEntries mockEntries = createLedgerEntries(1L, 0, 1);
        when(rh.readAsync(0L, 1L)).thenReturn(CompletableFuture.completedFuture(mockEntries));

        CompletableFuture<LedgerEntries> future =
                ReadEntryUtils.readAsync(ml, rh, 0L, 1L, true, 1024);

        assertThat(future).isCompleted();
        verify(rh).readAsync(0L, 1L);

        future.getNow(null).close();
    }

    @Test
    public void testAutoRefillWithSizeLimitedReturns() {
        // Simulate: first batch returns only entries 0-1 (size-limited),
        // second batch returns entries 2-4 to complete the read
        LedgerEntries batch1 = createLedgerEntriesWithSizes(1L,
                new long[]{0, 1}, new int[]{256, 256});
        LedgerEntries batch2 = createLedgerEntriesWithSizes(1L,
                new long[]{2, 3, 4}, new int[]{128, 128, 128});

        when(lh.batchReadAsync(eq(0L), eq(5), anyLong()))
                .thenReturn(CompletableFuture.completedFuture(batch1));
        when(lh.batchReadAsync(eq(2L), eq(3), anyLong()))
                .thenReturn(CompletableFuture.completedFuture(batch2));

        CompletableFuture<LedgerEntries> future =
                ReadEntryUtils.readAsync(ml, lh, 0L, 4L, true, 1024);

        assertThat(future).isCompleted();
        LedgerEntries result = future.getNow(null);
        try {
            List<Long> entryIds = new ArrayList<>();
            for (LedgerEntry e : result) {
                entryIds.add(e.getEntryId());
            }
            assertThat(entryIds).containsExactly(0L, 1L, 2L, 3L, 4L);
        } finally {
            result.close();
        }
    }

    @Test
    public void testBatchReadFailureFallsBackToReadUnconfirmed() {
        // First batch read fails
        when(lh.batchReadAsync(eq(0L), anyInt(), anyLong()))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("BK batch read failed")));
        // Fallback succeeds
        LedgerEntries fallbackResult = createLedgerEntries(1L, 0, 1, 2, 3, 4);
        when(lh.readUnconfirmedAsync(0L, 4L))
                .thenReturn(CompletableFuture.completedFuture(fallbackResult));

        CompletableFuture<LedgerEntries> future =
                ReadEntryUtils.readAsync(ml, lh, 0L, 4L, true, 1024);

        assertThat(future).isCompleted();
        LedgerEntries result = future.getNow(null);
        try {
            List<Long> entryIds = new ArrayList<>();
            for (LedgerEntry e : result) {
                entryIds.add(e.getEntryId());
            }
            assertThat(entryIds).containsExactly(0L, 1L, 2L, 3L, 4L);
        } finally {
            result.close();
        }
        verify(lh).readUnconfirmedAsync(0L, 4L);
    }

    @Test
    public void testBatchReadFailureWithPartialDataDoesNotFallback() {
        // First batch succeeds with entries 0-2
        LedgerEntries batch1 = createLedgerEntries(1L, 0, 1, 2);
        when(lh.batchReadAsync(eq(0L), anyInt(), anyLong()))
                .thenReturn(CompletableFuture.completedFuture(batch1));
        // Second batch fails
        when(lh.batchReadAsync(eq(3L), anyInt(), anyLong()))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("Second batch failed")));

        CompletableFuture<LedgerEntries> future =
                ReadEntryUtils.readAsync(ml, lh, 0L, 4L, true, 1024);

        // Should fail without falling back to readUnconfirmedAsync
        assertThat(future).isCompletedExceptionally();
        verify(lh, never()).readUnconfirmedAsync(anyLong(), anyLong());
    }

    @Test
    public void testBatchReadMidBatchFailureCleansUpResources() {
        // Track whether the first batch's LedgerEntries is closed
        AtomicBoolean batch1Closed = new AtomicBoolean(false);
        LedgerEntries batch1Inner = createLedgerEntries(1L, 0, 1, 2);
        LedgerEntries trackedBatch1 = new LedgerEntries() {
            @Override
            public LedgerEntry getEntry(long eid) {
                return batch1Inner.getEntry(eid);
            }

            @Override
            public Iterator<LedgerEntry> iterator() {
                return batch1Inner.iterator();
            }

            @Override
            public void close() {
                batch1Closed.set(true);
                batch1Inner.close();
            }
        };

        when(lh.batchReadAsync(eq(0L), anyInt(), anyLong()))
                .thenReturn(CompletableFuture.completedFuture(trackedBatch1));
        when(lh.batchReadAsync(eq(3L), anyInt(), anyLong()))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("Second batch failed")));

        CompletableFuture<LedgerEntries> future =
                ReadEntryUtils.readAsync(ml, lh, 0L, 4L, true, 1024);

        assertThat(future).isCompletedExceptionally();
        // Verify first batch resources were cleaned up
        assertThat(batch1Closed.get()).isTrue();
        // Verify no fallback since partial data was received
        verify(lh, never()).readUnconfirmedAsync(anyLong(), anyLong());
    }

    @Test
    public void testBatchReadMidBatchFailurePreservesOriginalException() {
        LedgerEntries batch1 = createLedgerEntries(1L, 0, 1, 2);
        when(lh.batchReadAsync(eq(0L), anyInt(), anyLong()))
                .thenReturn(CompletableFuture.completedFuture(batch1));
        when(lh.batchReadAsync(eq(3L), anyInt(), anyLong()))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("Second batch read failed")));

        CompletableFuture<LedgerEntries> future =
                ReadEntryUtils.readAsync(ml, lh, 0L, 4L, true, 1024);

        assertThat(future).isCompletedExceptionally();
        assertThatThrownBy(future::get)
                .hasCauseInstanceOf(RuntimeException.class)
                .hasRootCauseMessage("Second batch read failed");
    }

    // --- helpers ---

    private static LedgerEntries createLedgerEntries(long ledgerId, long... entryIds) {
        List<LedgerEntry> entries = new ArrayList<>();
        for (long entryId : entryIds) {
            entries.add(LedgerEntryImpl.create(ledgerId, entryId, 1,
                    Unpooled.wrappedBuffer(new byte[]{(byte) entryId})));
        }
        return wrapLedgerEntries(entries);
    }

    private static LedgerEntries createLedgerEntriesWithSizes(long ledgerId, long[] entryIds, int[] sizes) {
        List<LedgerEntry> entries = new ArrayList<>();
        for (int i = 0; i < entryIds.length; i++) {
            byte[] data = new byte[sizes[i]];
            entries.add(LedgerEntryImpl.create(ledgerId, entryIds[i], sizes[i],
                    Unpooled.wrappedBuffer(data)));
        }
        return wrapLedgerEntries(entries);
    }

    private static LedgerEntries wrapLedgerEntries(List<LedgerEntry> entries) {
        return new LedgerEntries() {
            @Override
            public LedgerEntry getEntry(long eid) {
                for (LedgerEntry e : entries) {
                    if (e.getEntryId() == eid) {
                        return e;
                    }
                }
                throw new IndexOutOfBoundsException("Entry " + eid + " not found");
            }

            @Override
            public Iterator<LedgerEntry> iterator() {
                return entries.iterator();
            }

            @Override
            public void close() {
                entries.forEach(LedgerEntry::close);
            }
        };
    }
}
