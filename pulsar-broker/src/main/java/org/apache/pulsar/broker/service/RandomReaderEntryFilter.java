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
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.Position;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.protocol.Markers;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.client.api.transaction.TxnID;

public final class RandomReaderEntryFilter {

    private RandomReaderEntryFilter() {
    }

    public static List<Entry> filterEntries(PersistentTopic topic, List<Entry> entries,
                                            Position maxVisiblePosition, boolean readCommitted) {
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
}
