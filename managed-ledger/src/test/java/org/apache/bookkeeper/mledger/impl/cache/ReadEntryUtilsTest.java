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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import io.netty.buffer.Unpooled;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.client.AsyncCallback.ReadCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.api.LedgerEntries;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.client.impl.LedgerEntryImpl;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.mockito.stubbing.Answer;
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
        doAnswer(successBatchReadAnswer(1L, 0, 1, 2, 3, 4)).when(lh)
                .asyncBatchReadUnconfirmedEntries(eq(0L), anyInt(), anyLong(), any(), any());

        CompletableFuture<LedgerEntries> future =
                ReadEntryUtils.readAsync(ml, lh, 0L, 4L, true, 1024);

        assertThat(future).isCompleted();
        try (LedgerEntries result = future.getNow(null)) {
            List<Long> entryIds = new ArrayList<>();
            for (LedgerEntry e : result) {
                entryIds.add(e.getEntryId());
            }
            assertThat(entryIds).containsExactly(0L, 1L, 2L, 3L, 4L);
        }

        verify(lh, never()).readUnconfirmedAsync(anyLong(), anyLong());
    }

    @Test
    public void testBatchReadMultipleBatches() {
        // First batch returns entries 0-2, second returns 3-4
        doAnswer(successBatchReadAnswer(1L, 0, 1, 2)).when(lh)
                .asyncBatchReadUnconfirmedEntries(eq(0L), anyInt(), anyLong(), any(), any());
        doAnswer(successBatchReadAnswer(1L, 3, 4)).when(lh)
                .asyncBatchReadUnconfirmedEntries(eq(3L), anyInt(), anyLong(), any(), any());

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
        doAnswer(invocation -> {
            ReadCallback callback = invocation.getArgument(3);
            Object ctx = invocation.getArgument(4);
            Enumeration<org.apache.bookkeeper.client.LedgerEntry> empty = Collections.emptyEnumeration();
            callback.readComplete(BKException.Code.OK, null, empty, ctx);
            return null;
        }).when(lh)
                .asyncBatchReadUnconfirmedEntries(eq(0L), anyInt(), anyLong(), any(), any());
        // Empty result triggers fallback to readUnconfirmedAsync
        when(lh.readUnconfirmedAsync(0L, 4L))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("No entries")));

        CompletableFuture<LedgerEntries> future =
                ReadEntryUtils.readAsync(ml, lh, 0L, 4L, true, 1024);

        assertThat(future).isCompletedExceptionally();
    }

    @Test
    public void testBatchReadFailure() {
        doAnswer(failureBatchReadAnswer()).when(lh)
                .asyncBatchReadUnconfirmedEntries(eq(0L), anyInt(), anyLong(), any(), any());
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
        verify(lh, never()).asyncBatchReadUnconfirmedEntries(anyLong(), anyInt(), anyLong(), any(), any());

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
        verify(lh, never()).asyncBatchReadUnconfirmedEntries(anyLong(), anyInt(), anyLong(), any(), any());

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
        doAnswer(successBatchReadAnswerWithSizes(1L,
                new long[]{0, 1}, new int[]{256, 256})).when(lh)
                .asyncBatchReadUnconfirmedEntries(eq(0L), eq(5), anyLong(), any(), any());
        doAnswer(successBatchReadAnswerWithSizes(1L,
                new long[]{2, 3, 4}, new int[]{128, 128, 128})).when(lh)
                .asyncBatchReadUnconfirmedEntries(eq(2L), eq(3), anyLong(), any(), any());

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
        doAnswer(failureBatchReadAnswer()).when(lh)
                .asyncBatchReadUnconfirmedEntries(eq(0L), anyInt(), anyLong(), any(), any());
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
        doAnswer(successBatchReadAnswer(1L, 0, 1, 2)).when(lh)
                .asyncBatchReadUnconfirmedEntries(eq(0L), anyInt(), anyLong(), any(), any());
        // Second batch fails
        doAnswer(failureBatchReadAnswer()).when(lh)
                .asyncBatchReadUnconfirmedEntries(eq(3L), anyInt(), anyLong(), any(), any());

        CompletableFuture<LedgerEntries> future =
                ReadEntryUtils.readAsync(ml, lh, 0L, 4L, true, 1024);

        // Should fail without falling back to readUnconfirmedAsync
        assertThat(future).isCompletedExceptionally();
        verify(lh, never()).readUnconfirmedAsync(anyLong(), anyLong());
    }

    @Test
    public void testBatchReadMidBatchFailurePreservesOriginalException() {
        // First batch succeeds with entries 0-2
        doAnswer(successBatchReadAnswer(1L, 0, 1, 2)).when(lh)
                .asyncBatchReadUnconfirmedEntries(eq(0L), anyInt(), anyLong(), any(), any());
        // Second batch fails
        doAnswer(failureBatchReadAnswer()).when(lh)
                .asyncBatchReadUnconfirmedEntries(eq(3L), anyInt(), anyLong(), any(), any());

        CompletableFuture<LedgerEntries> future =
                ReadEntryUtils.readAsync(ml, lh, 0L, 4L, true, 1024);

        assertThat(future).isCompletedExceptionally();
        assertThatThrownBy(future::get)
                .hasCauseInstanceOf(BKException.class);
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

    private static Answer<Void> successBatchReadAnswer(long ledgerId, long... entryIds) {
        return invocation -> {
            ReadCallback callback = invocation.getArgument(3);
            Object ctx = invocation.getArgument(4);
            Queue<org.apache.bookkeeper.client.LedgerEntry> queue = new ArrayDeque<>();
            for (long eid : entryIds) {
                org.apache.bookkeeper.client.LedgerEntry e =
                        mock(org.apache.bookkeeper.client.LedgerEntry.class);
                when(e.getEntryId()).thenReturn(eid);
                when(e.getLedgerId()).thenReturn(ledgerId);
                when(e.getLength()).thenReturn(1L);
                when(e.getEntryBuffer()).thenReturn(Unpooled.wrappedBuffer(new byte[]{(byte) eid}));
                queue.add(e);
            }
            callback.readComplete(BKException.Code.OK, null, Collections.enumeration(queue), ctx);
            return null;
        };
    }

    private static Answer<Void> successBatchReadAnswerWithSizes(long ledgerId, long[] entryIds, int[] sizes) {
        return invocation -> {
            ReadCallback callback = invocation.getArgument(3);
            Object ctx = invocation.getArgument(4);
            Queue<org.apache.bookkeeper.client.LedgerEntry> queue = new ArrayDeque<>();
            for (int i = 0; i < entryIds.length; i++) {
                org.apache.bookkeeper.client.LedgerEntry e =
                        mock(org.apache.bookkeeper.client.LedgerEntry.class);
                when(e.getEntryId()).thenReturn(entryIds[i]);
                when(e.getLedgerId()).thenReturn(ledgerId);
                when(e.getLength()).thenReturn((long) sizes[i]);
                when(e.getEntryBuffer()).thenReturn(Unpooled.wrappedBuffer(new byte[sizes[i]]));
                queue.add(e);
            }
            callback.readComplete(BKException.Code.OK, null, Collections.enumeration(queue), ctx);
            return null;
        };
    }

    private static Answer<Void> failureBatchReadAnswer() {
        return invocation -> {
            ReadCallback callback = invocation.getArgument(3);
            Object ctx = invocation.getArgument(4);
            callback.readComplete(BKException.Code.BookieHandleNotAvailableException, null, null, ctx);
            return null;
        };
    }
}
