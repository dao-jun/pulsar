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
package org.apache.pulsar.broker.service;

import io.netty.buffer.ByteBuf;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.api.proto.ServerError;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.protocol.Markers;
import org.apache.pulsar.common.util.FutureUtil;

public class BrokerRandomReader implements AutoCloseable {
    private final long randomReaderId;
    private final String readerName;
    private final Map<String, String> metadata;
    private final PersistentTopic topic;
    private final ServerCnx owner;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicLong inFlightRequestId = new AtomicLong(-1L);
    private final boolean readCommitted;

    public BrokerRandomReader(long randomReaderId, String readerName, Map<String, String> metadata,
                              PersistentTopic topic, ServerCnx owner, boolean readCommitted) {
        this.randomReaderId = randomReaderId;
        this.readerName = readerName;
        this.metadata = Map.copyOf(metadata);
        this.topic = topic;
        this.owner = owner;
        this.readCommitted = readCommitted;
    }

    public boolean beginRead(long requestId) {
        return !closed.get() && inFlightRequestId.compareAndSet(-1L, requestId);
    }

    public void endRead(long requestId) {
        inFlightRequestId.compareAndSet(requestId, -1L);
    }

    public PersistentTopic topic() {
        return topic;
    }

    public ServerCnx owner() {
        return owner;
    }

    public long randomReaderId() {
        return randomReaderId;
    }

    public String readerName() {
        return readerName;
    }

    public Map<String, String> metadata() {
        return metadata;
    }

    public boolean isClosed() {
        return closed.get();
    }

    public static CompletableFuture<PersistentTopic> validatePersistentTopic(Topic topic) {
        if (topic instanceof PersistentTopic) {
            return CompletableFuture.completedFuture((PersistentTopic) topic);
        }
        return FutureUtil.failedFuture(new BrokerServiceException.NotAllowedException(
                "RandomReader only supports persistent topics"));
    }

    @Override
    public void close() {
        closed.set(true);
    }

    public CompletableFuture<List<Entry>> readEntries(Position start, int needed,
                                                      Position maxVisible, int maxTotalEntries) {
        if (needed <= 0 || maxTotalEntries <= 0 || start.compareTo(maxVisible) > 0) {
            return CompletableFuture.completedFuture(List.of());
        }
        int batchSize = Math.min(needed, maxTotalEntries);
        return topic.getManagedLedger().readEntries(start, batchSize).
                thenCompose(entries -> {
                    List<Entry> visible = filterEntries(entries, maxVisible);
                    if (entries.size() < batchSize || visible.size() >= needed) {
                        return CompletableFuture.completedFuture(visible);
                    }
                    Position next = entries.get(entries.size() - 1).getPosition().getNext();
                    return readEntries(next, needed - visible.size(), maxVisible,
                            maxTotalEntries - entries.size())
                            .thenApply(more -> {
                                List<Entry> all = new ArrayList<>(visible.size() + more.size());
                                all.addAll(visible);
                                all.addAll(more);
                                return all;
                            });
                });
    }

    public CompletableFuture<Void> disconnect(String brokerServiceUrl, String brokerServiceUrlTls) {
        close();
        owner.disconnectRandomReader(randomReaderId, brokerServiceUrl, brokerServiceUrlTls);
        return CompletableFuture.completedFuture(null);
    }

    private List<Entry> filterEntries(List<Entry> entries, Position maxVisiblePosition) {
        List<Entry> result = new ArrayList<>(entries.size());
        for (Entry entry : entries) {
            if (entry == null) {
                continue;
            }
            try {
                if (entry.getPosition().compareTo(maxVisiblePosition) > 0) {
                    entry.release();
                    continue;
                }
                ByteBuf metadataAndPayload = entry.getDataBuffer();
                int readerIndex = metadataAndPayload.readerIndex();
                MessageMetadata metadata = Commands.peekAndCopyMessageMetadata(
                        metadataAndPayload, topic.getName(), -1);
                metadataAndPayload.readerIndex(readerIndex);
                if (metadata == null) {
                    entry.release();
                    throw new BrokerServiceException.PersistenceException(
                            "Failed to parse message metadata at " + entry.getPosition());
                }
                if (Markers.isServerOnlyMarker(metadata) || Markers.isTxnMarker(metadata)) {
                    entry.release();
                    continue;
                }
                if (readCommitted && metadata.hasTxnidMostBits() && metadata.hasTxnidLeastBits()
                        && topic.isTxnAborted(new TxnID(metadata.getTxnidMostBits(),
                        metadata.getTxnidLeastBits()), entry.getPosition())) {
                    entry.release();
                    continue;
                }
                result.add(entry);
            } catch (BrokerServiceException.PersistenceException e) {
                throw FutureUtil.wrapToCompletionException(e);
            } catch (Throwable t) {
                entry.release();
                throw FutureUtil.wrapToCompletionException(t);
            }
        }
        return result;
    }

    public static ServerError toServerError(Throwable cause) {
        if (cause instanceof ManagedLedgerException) {
            if (cause instanceof ManagedLedgerException.ManagedLedgerFencedException
                    || cause instanceof ManagedLedgerException.ManagedLedgerAlreadyClosedException
                    || cause instanceof ManagedLedgerException.OffloadInProgressException) {
                return ServerError.ServiceNotReady;
            }
            if (cause instanceof ManagedLedgerException.ManagedLedgerNotFoundException) {
                return ServerError.TopicNotFound;
            }
            if (cause instanceof ManagedLedgerException.ManagedLedgerTerminatedException) {
                return ServerError.TopicTerminatedError;
            }
            if (cause instanceof ManagedLedgerException.NonRecoverableLedgerException) {
                return ServerError.PersistenceError;
            }
            // Generic fallback: conservative mapping to PersistenceError
            return ServerError.PersistenceError;
        }
        return BrokerServiceException.getClientErrorCode(cause);
    }
}
