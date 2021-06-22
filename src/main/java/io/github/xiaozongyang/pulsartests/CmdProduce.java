/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.github.xiaozongyang.pulsartests;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.internal.Lists;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.pulsar.shade.org.apache.commons.lang3.StringUtils.isBlank;

/**
 * pulsar-client produce command implementation.
 *
 */
@Parameters(commandDescription = "Produce messages to a specified topic")
public class CmdProduce {

    private static final Logger LOG = LoggerFactory.getLogger(PulsarClientTool.class);
    private static final int MAX_MESSAGES = 10000000;
    /**
     * Digit 0 - 9
     * */
    private static final byte[] BODY_BYTES = {
        0x30,
        0x31,
        0x32,
        0x33,
        0x34,
        0x35,
        0x36,
        0x37,
        0x38,
        0x39,
    };

    @Parameter(names = {"--message-body-size-bytes"}, description = "Message body size in bytes, defalut value is 100")
    private int messageBodyBytes = 100;

    @Parameter(names = {"--threads"}, description = "Number of threads for sending message, default is 2 * numberOfAvailableProcessors")
    private int threads = 2 * Runtime.getRuntime().availableProcessors();

    @Parameter(names = {"--deliver-after-seconds"}, description = "seconds of deliverAfter method, " +
        "when it is greater then 0 deliverAfter method will be applied for every message default value is 0.")

    private long deliverAfterSeconds = 0;

    @Parameter(names = {"-n", "--num-produce"},
        description = "Number of times to send message(s), the count of messages/files * num-produce " +
            "should below than " + MAX_MESSAGES + ".")
    private int numTimesProduce = 10000;

    @Parameter(names = {"-p", "--properties"}, description = "Properties to add, Comma separated "
        + "key=value string, like k1=v1,k2=v2.")
    private List<String> properties = Lists.newArrayList();

    @Parameter(names = {"-t", "--topic"}, description = "topic name")
    private String topic;

    private ClientBuilder clientBuilder;
    private Authentication authentication;
    private String serviceURL;
    private CountDownLatch latch;

    public CmdProduce() {
        // Do nothing
    }

    /**
     * Set Pulsar client configuration.
     *
     */
    public void updateConfig(ClientBuilder newBuilder, Authentication authentication, String serviceURL) {
        this.clientBuilder = newBuilder;
        this.authentication = authentication;
        this.serviceURL = serviceURL;
    }

    /*
     * Generate a list of message bodies which can be used to build messages
     *
     * @param stringMessages List of strings to send
     *
     * @param messageFileNames List of file names to read and send
     *
     * @return list of message bodies
     */
    private byte[] generateMessageBody(int bodySize) {
        byte[] body = new byte[bodySize];

        for (int i = 0; i < bodySize; i++) {
            body[i] = BODY_BYTES[i % 10];
        }

        return body;
    }

    /**
     * Run the producer.
     *
     * @return 0 for success, < 0 otherwise
     * @throws Exception
     */
    public int run() throws PulsarClientException {
        if (isBlank(topic)) {
            throw (new ParameterException("Please provide one and only one topic name."));
        }
        if (this.numTimesProduce <= 0) {
            throw (new ParameterException("Number of times need to be positive number."));
        }

        latch = new CountDownLatch(threads);

        ScheduledExecutorService workerPool = Executors.newScheduledThreadPool(threads);
        for (int i = 0; i < threads; i++) {
            workerPool.execute(() -> publish(topic, numTimesProduce / threads));
        }

        try {
            latch.await();
        } catch (InterruptedException e) {
            throw new RuntimeException("produce latch interrupted", e);
        }

        return 0;
    }

    private int publish(String topic, int numTimesProduce) {
        int numMessagesSent = 0;
        int returnCode = 0;

        try {
            PulsarClient client = clientBuilder.build();
            ProducerBuilder<byte[]> producerBuilder = client.newProducer().topic(topic);
            Producer<byte[]> producer = producerBuilder.create();

            byte[] body = generateMessageBody(messageBodyBytes);

            Map<String, String> kvMap = new HashMap<>();
            for (String property : properties) {
                String[] kv = property.split("=");
                kvMap.put(kv[0], kv[1]);
            }


            TypedMessageBuilder<byte[]> message = producer.newMessage();
            message.value(body);

            for (int i = 0; i < numTimesProduce; i++) {
                if (deliverAfterSeconds > 0) {
                    message.deliverAfter(deliverAfterSeconds, TimeUnit.SECONDS);
                }

                if (!kvMap.isEmpty()) {
                    message.properties(kvMap);
                }

                MessageId msgId = message.send();
                LOG.info("ledgerId={} entryId={} paritionIndex={}", ((MessageIdImpl) msgId).getLedgerId(), ((MessageIdImpl) msgId).getEntryId(),((MessageIdImpl) msgId).getPartitionIndex());

                numMessagesSent++;
            }
            client.close();
        } catch (Exception e) {
            LOG.error("Error while producing messages");
            LOG.error(e.getMessage(), e);
            returnCode = -1;
        } finally {
            LOG.info("{} messages successfully produced", numMessagesSent);
        }

        latch.countDown();

        return returnCode;
    }
}
