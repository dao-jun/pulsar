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

final class RandomReaders {
    private final ManagedLedgerImpl ledger;
    // Lifecycle mutations are synchronized; volatile reads keep add and read paths lock-free.
    private volatile int activeReaders;
    private volatile boolean closed;

    RandomReaders(ManagedLedgerImpl ledger) {
        this.ledger = ledger;
    }

    synchronized RandomReaderImpl create() {
        if (closed) {
            throw new IllegalStateException("Managed ledger is already closed");
        }
        RandomReaderImpl reader = new RandomReaderImpl(ledger, this);
        activeReaders++;
        return reader;
    }

    synchronized void unregister() {
        if (activeReaders > 0) {
            activeReaders--;
        }
    }

    boolean hasActiveReaders() {
        return activeReaders > 0;
    }

    int size() {
        return activeReaders;
    }

    boolean isClosed() {
        return closed;
    }

    synchronized void closeAll() {
        closed = true;
        activeReaders = 0;
    }
}
