/**
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
package io.github.xiaozongyang.pulsartests;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionMode;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.shade.com.google.common.util.concurrent.RateLimiter;
import org.apache.pulsar.shade.org.apache.commons.io.HexDump;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

/**
 * pulsar-client consume command implementation.
 *
 */
@Parameters(commandDescription = "Consume messages from a specified topic")
public class CmdConsume {

    private static final Logger LOG = LoggerFactory.getLogger(PulsarClientTool.class);
    private static final String MESSAGE_BOUNDARY = "----- got message -----";

    @Parameter(description = "TopicName", required = true)
    private List<String> mainOptions = new ArrayList<String>();

    @Parameter(names = { "-t", "--subscription-type" }, description = "Subscription type.")
    private SubscriptionType subscriptionType = SubscriptionType.Exclusive;

    @Parameter(names = { "-m", "--subscription-mode" }, description = "Subscription mode.")
    private SubscriptionMode subscriptionMode = SubscriptionMode.Durable;

    @Parameter(names = { "-p", "--subscription-position" }, description = "Subscription position.")
    private SubscriptionInitialPosition subscriptionInitialPosition = SubscriptionInitialPosition.Latest;

    @Parameter(names = { "-s", "--subscription-name" }, required = true, description = "Subscription name.")
    private String subscriptionName;

    @Parameter(names = { "-n",
            "--num-messages" }, description = "Number of messages to consume, 0 means to consume forever.")
    private int numMessagesToConsume = 1;

    @Parameter(names = { "--hex" }, description = "Display binary messages in hex.")
    private boolean displayHex = false;

    @Parameter(names = { "-r", "--rate" }, description = "Rate (in msg/sec) at which to consume, "
            + "value 0 means to consume messages as fast as possible.")
    private double consumeRate = 0;

    @Parameter(names = { "--regex" }, description = "Indicate the topic name is a regex pattern")
    private boolean isRegex = false;
    
    @Parameter(names = { "-q", "--queue-size" }, description = "Consumer receiver queue size.")
    private int receiverQueueSize = 0;

    @Parameter(names = { "-mc", "--max_chunked_msg" }, description = "Max pending chunk messages")
    private int maxPendingChuckedMessage = 0;

    @Parameter(names = { "-ac",
            "--auto_ack_chunk_q_full" }, description = "Auto ack for oldest message on queue is full")
    private boolean autoAckOldestChunkedMessageOnQueueFull = false;

    @Parameter(names = { "-rc", "--read-compacted"}, description = "Read compacted or not")
    private boolean readCompacted = false;
    
    private ClientBuilder clientBuilder;
    private Authentication authentication;
    private String serviceURL;

    public CmdConsume() {
        // Do nothing
    }

    /**
     * Set client configuration.
     *
     */
    public void updateConfig(ClientBuilder clientBuilder, Authentication authentication, String serviceURL) {
        this.clientBuilder = clientBuilder;
        this.authentication = authentication;
        this.serviceURL = serviceURL;
    }

    /**
     * Interprets the message to create a string representation
     *
     * @param message
     *            The message to interpret
     * @param displayHex
     *            Whether to display BytesMessages in hexdump style, ignored for simple text messages
     * @return String representation of the message
     */
    private String interpretMessage(Message<byte[]> message, boolean displayHex) throws IOException {
        StringBuilder sb = new StringBuilder();

        String properties = Arrays.toString(message.getProperties().entrySet().toArray());

        String data;
        byte[] msgData = message.getData();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        if (!displayHex) {
            data = new String(msgData);
        } else {
            HexDump.dump(msgData, 0, out, 0);
            data = new String(out.toByteArray());
        }

        String key = null;
        if (message.hasKey()) {
            key = message.getKey();
        }

        sb.append("key:[").append(key).append("], ");
        sb.append("properties:").append(properties).append(", ");
        sb.append("content:").append(data);

        return sb.toString();
    }

    /**
     * Run the consume command.
     *
     * @return 0 for success, < 0 otherwise
     */
    public int run() throws PulsarClientException, IOException {
        if (mainOptions.size() != 1)
            throw (new ParameterException("Please provide one and only one topic name."));
        if (this.subscriptionName == null || this.subscriptionName.isEmpty())
            throw (new ParameterException("Subscription name is not provided."));
        if (this.numMessagesToConsume < 0)
            throw (new ParameterException("Number of messages should be zero or positive."));

        String topic = this.mainOptions.get(0);
        return consume(topic);
    }

    private int consume(String topic) {
        int numMessagesConsumed = 0;
        int returnCode = 0;

        try {
            PulsarClient client = clientBuilder.build();
            ConsumerBuilder<byte[]> builder = client.newConsumer()
                    .subscriptionName(this.subscriptionName)
                    .subscriptionType(subscriptionType)
                    .subscriptionMode(subscriptionMode)
                    .subscriptionInitialPosition(subscriptionInitialPosition);

            if (isRegex) {
                builder.topicsPattern(Pattern.compile(topic));
            } else {
                builder.topic(topic);
            }

            if (this.maxPendingChuckedMessage > 0) {
                builder.maxPendingChuckedMessage(this.maxPendingChuckedMessage);
            }
            if (this.receiverQueueSize > 0) {
                builder.receiverQueueSize(this.receiverQueueSize);
            }

            builder.autoAckOldestChunkedMessageOnQueueFull(this.autoAckOldestChunkedMessageOnQueueFull);
            builder.readCompacted(readCompacted);

            Consumer<byte[]> consumer = builder.subscribe();

            RateLimiter limiter = (this.consumeRate > 0) ? RateLimiter.create(this.consumeRate) : null;
            while (this.numMessagesToConsume == 0 || numMessagesConsumed < this.numMessagesToConsume) {
                if (limiter != null) {
                    limiter.acquire();
                }

                Message<byte[]> msg = consumer.receive(5, TimeUnit.SECONDS);
                if (msg == null) {
                    LOG.info("No message to consume after waiting for 5 seconds.");
                } else {
                    numMessagesConsumed += 1;
                    System.out.println(MESSAGE_BOUNDARY);
                    String output = this.interpretMessage(msg, displayHex);
                    System.out.println(System.currentTimeMillis() + "\t" + output);
                    consumer.acknowledge(msg);
                }
            }
            client.close();
        } catch (Exception e) {
            LOG.error("Error while consuming messages");
            LOG.error(e.getMessage(), e);
            returnCode = -1;
        } finally {
            LOG.info("{} messages successfully consumed", numMessagesConsumed);
        }

        return returnCode;

    }
}
