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
package org.apache.pulsar.client.api;

import static org.testng.Assert.assertEquals;
import org.apache.pulsar.client.impl.BatchMessageIdImpl;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.testng.annotations.Test;

public class MessageIdTest {
    @Test
    public void messageIdTest() {
        MessageId mId = new MessageIdImpl(1, 2, 3);
        assertEquals(mId.toString(), "1:2:3");

        mId = new BatchMessageIdImpl(0, 2, 3, 4);
        assertEquals(mId.toString(), "0:2:3:4");

        mId = new BatchMessageIdImpl(-1, 2, -3, 4);
        assertEquals(mId.toString(), "-1:2:-3:4");

        mId = new MessageIdImpl(0, -23, 3);
        assertEquals(mId.toString(), "0:-23:3");
    }
}
