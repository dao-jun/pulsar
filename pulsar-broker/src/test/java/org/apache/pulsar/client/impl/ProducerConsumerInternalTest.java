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
package org.apache.pulsar.client.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.service.ServerCnx;
import org.apache.pulsar.client.api.BatcherBuilder;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.api.proto.CommandCloseProducer;
import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Different with {@link org.apache.pulsar.client.api.SimpleProducerConsumerTest}, this class can visit the variables
 * of {@link ConsumerImpl} or {@link ProducerImpl} which have protected or default access modifiers.
 */
@Slf4j
@Test(groups = "broker-impl")
public class ProducerConsumerInternalTest extends ProducerConsumerBase {

    @BeforeClass(alwaysRun = true)
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testSameProducerRegisterTwice() throws Exception {
        final String topicName = BrokerTestUtil.newUniqueName("persistent://my-property/my-ns/tp_");
        admin.topics().createNonPartitionedTopic(topicName);

        // Create producer using default producerName.
        ProducerImpl producer = (ProducerImpl) pulsarClient.newProducer().topic(topicName).create();
        ServiceProducer serviceProducer = getServiceProducer(producer, topicName);

        // Remove producer maintained by server cnx. To make it can register the second time.
        removeServiceProducerMaintainedByServerCnx(serviceProducer);

        // Trigger the client producer reconnect.
        CommandCloseProducer commandCloseProducer = new CommandCloseProducer();
        commandCloseProducer.setProducerId(producer.producerId);
        producer.getClientCnx().handleCloseProducer(commandCloseProducer);

        // Verify the reconnection will be success.
        Awaitility.await().untilAsserted(() -> {
            assertEquals(producer.getState().toString(), "Ready");
        });
    }

    @Test
    public void testSameProducerRegisterTwiceWithSpecifiedProducerName() throws Exception {
        final String topicName = BrokerTestUtil.newUniqueName("persistent://my-property/my-ns/tp_");
        final String pName = "p1";
        admin.topics().createNonPartitionedTopic(topicName);

        // Create producer using default producerName.
        ProducerImpl producer = (ProducerImpl) pulsarClient.newProducer().producerName(pName).topic(topicName).create();
        ServiceProducer serviceProducer = getServiceProducer(producer, topicName);

        // Remove producer maintained by server cnx. To make it can register the second time.
        removeServiceProducerMaintainedByServerCnx(serviceProducer);

        // Trigger the client producer reconnect.
        CommandCloseProducer commandCloseProducer = new CommandCloseProducer();
        commandCloseProducer.setProducerId(producer.producerId);
        producer.getClientCnx().handleCloseProducer(commandCloseProducer);

        // Verify the reconnection will be success.
        Awaitility.await().untilAsserted(() -> {
            assertEquals(producer.getState().toString(), "Ready", "The producer registration failed");
        });
    }

    private void removeServiceProducerMaintainedByServerCnx(ServiceProducer serviceProducer) {
        ServerCnx serverCnx = (ServerCnx) serviceProducer.getServiceProducer().getCnx();
        serverCnx.removedProducer(serviceProducer.getServiceProducer());
        Awaitility.await().untilAsserted(() -> {
            assertFalse(serverCnx.getProducers().containsKey(serviceProducer.getServiceProducer().getProducerId()));
        });
    }

    @Test(groups = "flaky")
    public void testExclusiveConsumerWillAlwaysRetryEvenIfReceivedConsumerBusyError() throws Exception {
        final String topicName = BrokerTestUtil.newUniqueName("persistent://my-property/my-ns/tp_");
        final String subscriptionName = "subscription1";
        admin.topics().createNonPartitionedTopic(topicName);

        final ConsumerImpl consumer = (ConsumerImpl) pulsarClient.newConsumer().topic(topicName.toString())
                .subscriptionType(SubscriptionType.Exclusive).subscriptionName(subscriptionName).subscribe();

        ClientCnx clientCnx = consumer.getClientCnx();
        ServerCnx serverCnx = (ServerCnx) pulsar.getBrokerService()
                .getTopic(topicName, false).join().get().getSubscription(subscriptionName)
                .getDispatcher().getConsumers().get(0).cnx();

        // Make a disconnect to trigger broker remove the consumer which related this connection.
        // Make the second subscribe runs after the broker removing the old consumer, then it will receive
        // an error: "Exclusive consumer is already connected"
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        serverCnx.execute(() -> {
            try {
                countDownLatch.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        clientCnx.close();
        Thread.sleep(1000);
        countDownLatch.countDown();

        // Verify the consumer will always retry subscribe event received ConsumerBusy error.
        Awaitility.await().untilAsserted(() -> {
            assertEquals(consumer.getState(), HandlerState.State.Ready);
        });

        // cleanup.
        consumer.close();
        admin.topics().delete(topicName, false);
    }

    @DataProvider(name = "containerBuilder")
    public Object[][] containerBuilderProvider() {
        return new Object[][] {
                { BatcherBuilder.DEFAULT },
                { BatcherBuilder.KEY_BASED }
        };
    }

    @Test(timeOut = 30000, dataProvider = "containerBuilder")
    public void testSendTimerCheckForBatchContainer(BatcherBuilder batcherBuilder) throws Exception {
        final String topicName = BrokerTestUtil.newUniqueName("persistent://my-property/my-ns/tp_");
        @Cleanup Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName)
                .batcherBuilder(batcherBuilder)
                .sendTimeout(1, TimeUnit.SECONDS)
                .batchingMaxPublishDelay(100, TimeUnit.MILLISECONDS)
                .batchingMaxMessages(1000)
                .create();

        log.info("Before sendAsync msg-0: {}", System.nanoTime());
        CompletableFuture<MessageId> future = producer.sendAsync("msg-0".getBytes());
        future.thenAccept(msgId -> log.info("msg-0 done: {} (msgId: {})", System.nanoTime(), msgId));
        future.get(); // t: the current time point

        ((ProducerImpl<byte[]>) producer).triggerSendTimer(); // t+1000ms && t+2000ms: run() will be called again

        Thread.sleep(1950); // t+2050ms: the batch timer is expired, which happens after run() is called
        log.info("Before sendAsync msg-1: {}", System.nanoTime());
        future = producer.sendAsync("msg-1".getBytes());
        future.thenAccept(msgId -> log.info("msg-1 done: {} (msgId: {})", System.nanoTime(), msgId));
        future.get();
    }


    @Test
    public void testRetentionPolicyByProducingMessages() throws Exception {
        // Setup: configure the entries per ledger and retention polices.
        final int maxEntriesPerLedger = 10, messagesCount = 10;
        final String topicName = BrokerTestUtil.newUniqueName("persistent://my-property/my-ns/tp_");
        pulsar.getConfiguration().setManagedLedgerMaxEntriesPerLedger(maxEntriesPerLedger);
        pulsar.getConfiguration().setManagedLedgerMinLedgerRolloverTimeMinutes(0);
        pulsar.getConfiguration().setDefaultRetentionTimeInMinutes(0);
        pulsar.getConfiguration().setDefaultRetentionSizeInMB(0);

        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer().topic(topicName)
                .sendTimeout(1, TimeUnit.SECONDS)
                .enableBatching(false)
                .create();

        @Cleanup
        Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName)
                .subscriptionName("my-sub")
                .subscribe();
        // Act: prepare a full ledger data and ack them.
        for (int i = 0; i < messagesCount; i++) {
            producer.newMessage().sendAsync();
        }
        for (int i = 0; i < messagesCount; i++) {
            Message<byte[]> message = consumer.receive();
            assertNotNull(message);
            consumer.acknowledge(message);
        }
        // Verify: a new empty ledger will be created after the current ledger is fulled.
        // And the previous consumed ledgers will be deleted
        Awaitility.await().untilAsserted(() -> {
            admin.topics().trimTopic(topicName);
            PersistentTopicInternalStats internalStats = admin.topics().getInternalStatsAsync(topicName).get();
            assertEquals(internalStats.currentLedgerEntries, 0);
            assertEquals(internalStats.ledgers.size(), 1);
        });
    }


    @Test
    public void testProducerCompressionMinMsgBodySize() throws PulsarClientException {
        byte[] msg1024 = new byte[1024];
        byte[] msg1025 = new byte[1025];
        final String topicName = BrokerTestUtil.newUniqueName("persistent://my-property/my-ns/tp_");
        @Cleanup
        ProducerImpl<byte[]> producer = (ProducerImpl<byte[]>) pulsarClient.newProducer()
                .topic(topicName)
                .producerName("producer")
                .compressionType(CompressionType.LZ4)
                .compressionMinMsgBodySize(1024)
                .create();
        @Cleanup
        Consumer<byte[]> consumer = pulsarClient.newConsumer()
                .topic(topicName)
                .subscriptionName("sub")
                .subscribe();

        // disable batch
        producer.conf.setBatchingEnabled(false);
        producer.newMessage().value(msg1024).send();
        MessageImpl<byte[]> message = (MessageImpl<byte[]>) consumer.receive();
        CompressionType compressionType = message.getCompressionType();
        assertEquals(compressionType, CompressionType.NONE);
        producer.newMessage().value(msg1025).send();
        message = (MessageImpl<byte[]>) consumer.receive();
        compressionType = message.getCompressionType();
        assertEquals(compressionType, CompressionType.LZ4);

        // enable batch
        producer.conf.setBatchingEnabled(true);
        producer.newMessage().value(msg1024).send();
        message = (MessageImpl<byte[]>) consumer.receive();
        compressionType = message.getCompressionType();
        assertEquals(compressionType, CompressionType.NONE);
        producer.newMessage().value(msg1025).send();
        message = (MessageImpl<byte[]>) consumer.receive();
        compressionType = message.getCompressionType();
        assertEquals(compressionType, CompressionType.LZ4);

        // Verify data integrity
        String data = "compression test message";
        producer.conf.setBatchingEnabled(true);
        producer.getConfiguration().setCompressMinMsgBodySize(1);
        producer.newMessage().value(data.getBytes()).send();
        message = (MessageImpl<byte[]>) consumer.receive();
        assertEquals(new String(message.getData()), data);

        producer.conf.setBatchingEnabled(false);
        producer.getConfiguration().setCompressMinMsgBodySize(1);
        producer.newMessage().value(data.getBytes()).send();
        message = (MessageImpl<byte[]>) consumer.receive();
        assertEquals(new String(message.getData()), data);

    }
}
