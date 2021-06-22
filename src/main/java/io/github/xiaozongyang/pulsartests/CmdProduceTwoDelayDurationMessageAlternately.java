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
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.pulsar.shade.org.apache.commons.lang3.StringUtils.isBlank;

/**
 * pulsar-client produce command implementation.
 *
 */
@Parameters(commandDescription = "Produce two kind of messages to a specified topic alternately, e.g. a,b,a,b,a,b")
public class CmdProduceTwoDelayDurationMessageAlternately {

    private static final Logger LOG = LoggerFactory.getLogger(PulsarClientTool.class);
    private static final int MAX_MESSAGES = 10000000;
    /**
     * Digit 0 - 9
     * */
    private static final byte[] DIGIT_BODY_BYTES = {
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

    private static final byte[] LETTER_BODY_BYTES = {
        0x61,
        0x62,
        0x63,
        0x64,
        0x65,
        0x66,
        0x67,
        0x68,
        0x69,
        0x6a,
        0x6b,
        0x6c,
        0x6d,
        0x6e,
        0x6f,
        0x70,
        0x71,
        0x72,
        0x73,
        0x74,
        0x75,
        0x76,
        0x77,
        0x78,
        0x79,
        0x7a,
    };

    @Parameter(names = {"--first-message-body-size-bytes"}, description = "First message body size in bytes, defalut value is 100")
    private int firstMessageBodyBytes = 10;

    @Parameter(names = {"--first-message-body-type"}, description = "First message body type")
    private MessageBodyType firstMessageBodyType = MessageBodyType.NUMBER;

    @Parameter(names = {"--seconds-message-body-size-bytes"}, description = "Second message body size in bytes, defalut value is 100")
    private int secondMessageBodyBytes = 10;

    @Parameter(names = {"--second-message-body-type"}, description = "Second message body type")
    private MessageBodyType secondsMessageBodyType = MessageBodyType.STRING;

    @Parameter(names = {"--threads"}, description = "Number of threads for sending message, default is 2 * numberOfAvailableProcessors")
    private int threads = 2 * Runtime.getRuntime().availableProcessors();

    @Parameter(names = {"--first-deliver-after-seconds"}, description = "first deliverAfter seconds, " +
        "when it is greater then 0 deliverAfter method will be applied for every message default value is 0.")
    private long firstDeliverAfterSeconds = 0;

    @Parameter(names = {"--second-deliver-after-seconds"}, description = "seconds deliverAfter seconds, " +
        "when it is greater then 0 deliverAfter method will be applied for every message default value is 0.")
    private long secondDeliverAfterSeconds = 0;

    @Parameter(names = {"-n", "--num-produce"},
        description = "Number of times to send message(s), the count of messages/files * num-produce " +
            "should below than " + MAX_MESSAGES + ".")
    private int numTimesProduce = 10000;


    @Parameter(names = {"-t", "--topic"}, description = "topic name")
    private String topic;

    private ClientBuilder clientBuilder;
    private Authentication authentication;
    private String serviceURL;
    private CountDownLatch latch;

    public CmdProduceTwoDelayDurationMessageAlternately() {
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
    private byte[] generateMessageBody(int bodySize, MessageBodyType type) {
        byte[] body = new byte[bodySize];

        byte []dict = type == MessageBodyType.NUMBER ? DIGIT_BODY_BYTES : LETTER_BODY_BYTES;

        for (int i = 0; i < bodySize; i++) {
            body[i] = dict[i % dict.length];
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

            byte[] body1 = generateMessageBody(firstMessageBodyBytes, firstMessageBodyType);
            byte[] body2 = generateMessageBody(secondMessageBodyBytes, secondsMessageBodyType);

            TypedMessageBuilder<byte[]> msg1 = producer.newMessage();
            TypedMessageBuilder<byte[]> msg2 = producer.newMessage();

            msg1.value(body1);
            msg2.value(body2);

            for (int i = 0; i < numTimesProduce; i++) {
                if (firstDeliverAfterSeconds > 0) {
                    msg1.deliverAfter(firstDeliverAfterSeconds, TimeUnit.SECONDS);
                }
                if (secondDeliverAfterSeconds > 0) {
                    msg2.deliverAfter(secondDeliverAfterSeconds, TimeUnit.SECONDS);
                }

                msg1.send();
                msg2.send();

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

    public static enum MessageBodyType {
        NUMBER("number"),
        STRING("string");

        private String name;

        MessageBodyType(String name) {
            this.name = name;
        }

        public static MessageBodyType fromName(String name) {
            for (MessageBodyType t : values()) {
                if (t.name.equals(name)) {
                    return t;
                }
            }
            throw new IllegalArgumentException("Unknown messageBodyType `" + name + "`");
        }
    }
}
