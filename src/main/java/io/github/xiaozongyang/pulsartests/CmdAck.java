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
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.shade.org.apache.commons.io.HexDump;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author xiaozongyang
 */
@Parameters(commandDescription = "Ack message of specified topic")
public class CmdAck {

    private static final Logger LOG = LoggerFactory.getLogger(PulsarClientTool.class);

    @Parameter(description = "TopicName", required = true)
    private List<String> mainOptions = new ArrayList<String>();

    @Parameter(names = { "-t", "--subscription-type" }, description = "Subscription type.")
    private SubscriptionType subscriptionType = SubscriptionType.Exclusive;

    @Parameter(names = { "-m", "--subscription-mode" }, description = "Subscription mode.")
    private SubscriptionMode subscriptionMode = SubscriptionMode.Durable;

    @Parameter(names = { "-s", "--subscription-name" }, required = true, description = "Subscription name.")
    private String subscriptionName;

    @Parameter(names = { "--ledger-id"}, required = true, description = "ledgerId of messageId")
    private long ledgerId;
    @Parameter(names = { "--entry-id"}, required = true, description = "entryId of messageId")
    private long entryId;
    @Parameter(names = { "--partition-index"}, required = true, description = "partitionIndex of messageId")
    private int partitionIndex;

    private ClientBuilder clientBuilder;
    private Authentication authentication;
    private String serviceURL;

    public CmdAck() {
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

        String topic = this.mainOptions.get(0);
        return consume(topic);
    }

    private int consume(String topic) {
        int returnCode = 0;

        try {
            PulsarClient client = clientBuilder.build();
            ConsumerBuilder<byte[]> builder = client.newConsumer()
                .subscriptionName(this.subscriptionName)
                .subscriptionType(subscriptionType)
                .subscriptionMode(subscriptionMode)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest);

            builder.topic(topic);

            Consumer<byte[]> consumer = builder.subscribe();

            consumer.acknowledge(new MessageIdImpl(ledgerId, entryId, partitionIndex));
            LOG.info("message acked!");

            client.close();
        } catch (Exception e) {
            LOG.error("Error while consuming messages");
            LOG.error(e.getMessage(), e);
            returnCode = -1;
        }

        return returnCode;
    }
}
