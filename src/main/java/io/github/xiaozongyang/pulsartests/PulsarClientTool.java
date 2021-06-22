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


import com.beust.jcommander.DefaultUsageFormatter;
import com.beust.jcommander.IUsageFormatter;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import org.apache.pulsar.PulsarVersion;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.ProxyProtocol;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException.UnsupportedAuthenticationException;
import org.apache.pulsar.shade.org.apache.commons.lang3.StringUtils;

import static org.apache.pulsar.shade.org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.pulsar.shade.org.apache.commons.lang3.StringUtils.isNotBlank;

@Parameters(commandDescription = "Produce or consume messages on a specified topic")
public class PulsarClientTool {

    @Parameter(names = {"--url"}, description = "Broker URL to which to connect.")
    String serviceURL = "pulsar://localhost:6650/";

    @Parameter(names = {"--proxy-url"}, description = "Proxy-server URL to which to connect.")
    String proxyServiceURL = null;

    @Parameter(names = {"--proxy-protocol"}, description = "Proxy protocol to select type of routing at proxy.")
    ProxyProtocol proxyProtocol = null;

    @Parameter(names = {"--auth-plugin"}, description = "Authentication plugin class name.")
    String authPluginClassName = null;

    @Parameter(names = {"--listener-name"}, description = "Listener name for the broker.")
    String listenerName = null;

    @Parameter(
        names = {"--auth-params"},
        description = "Authentication parameters, whose format is determined by the implementation " +
            "of method `configure` in authentication plugin class, for example \"key1:val1,key2:val2\" " +
            "or \"{\"key1\":\"val1\",\"key2\":\"val2\"}.")
    String authParams = null;

    @Parameter(names = {"-v", "--version"}, description = "Get version of pulsar client")
    boolean version;

    @Parameter(names = {"-h", "--help",}, help = true, description = "Show this help.")
    boolean help;

    boolean tlsAllowInsecureConnection = false;
    boolean tlsEnableHostnameVerification = false;
    String tlsTrustCertsFilePath = null;

    // for tls with keystore type config
    boolean useKeyStoreTls = false;
    String tlsTrustStoreType = "JKS";
    String tlsTrustStorePath = null;
    String tlsTrustStorePassword = null;

    JCommander commandParser;
    IUsageFormatter usageFormatter;
    CmdProduce produceCommand;
    CmdConsume consumeCommand;
    CmdProduceTwoDelayDurationMessageAlternately produceAlternatelyCommand;
    CmdAck ackCommand;

    public PulsarClientTool() {
        produceCommand = new CmdProduce();
        consumeCommand = new CmdConsume();
        produceAlternatelyCommand = new CmdProduceTwoDelayDurationMessageAlternately();
        ackCommand = new CmdAck();

        this.commandParser = new JCommander();
        this.usageFormatter = new DefaultUsageFormatter(this.commandParser);

        commandParser.setProgramName("pulsar-client");
        commandParser.addObject(this);
        commandParser.addCommand("produce", produceCommand);
        commandParser.addCommand("consume", consumeCommand);
        commandParser.addCommand("produce-alternately", produceAlternatelyCommand);
        commandParser.addCommand("ack", ackCommand);
    }

    public static void main(String[] args) throws Exception {
        PulsarClientTool clientTool = new PulsarClientTool();
        int exit_code = clientTool.run(args);

        System.exit(exit_code);

    }

    private void updateConfig() throws UnsupportedAuthenticationException {
        ClientBuilder clientBuilder = PulsarClient.builder();
        Authentication authentication = null;
        if (isNotBlank(this.authPluginClassName)) {
            authentication = AuthenticationFactory.create(authPluginClassName, authParams);
            clientBuilder.authentication(authentication);
        }
        if (isNotBlank(this.listenerName)) {
            clientBuilder.listenerName(this.listenerName);
        }
        clientBuilder.allowTlsInsecureConnection(this.tlsAllowInsecureConnection);
        clientBuilder.tlsTrustCertsFilePath(this.tlsTrustCertsFilePath);
        clientBuilder.enableTlsHostnameVerification(this.tlsEnableHostnameVerification);
        clientBuilder.serviceUrl(serviceURL);

        clientBuilder.useKeyStoreTls(useKeyStoreTls)
            .tlsTrustStoreType(tlsTrustStoreType)
            .tlsTrustStorePath(tlsTrustStorePath)
            .tlsTrustStorePassword(tlsTrustStorePassword);

        if (StringUtils.isNotBlank(proxyServiceURL)) {
            if (proxyProtocol == null) {
                System.out.println("proxy-protocol must be provided with proxy-url");
                System.exit(-1);
            }
            clientBuilder.proxyServiceUrl(proxyServiceURL, proxyProtocol);
        }
        this.produceCommand.updateConfig(clientBuilder, authentication, this.serviceURL);
        this.consumeCommand.updateConfig(clientBuilder, authentication, this.serviceURL);
        this.produceAlternatelyCommand.updateConfig(clientBuilder, authentication, this.serviceURL);
        this.ackCommand.updateConfig(clientBuilder, authentication, this.serviceURL);
    }

    public int run(String[] args) {
        try {
            commandParser.parse(args);

            if (isBlank(this.serviceURL)) {
                commandParser.usage();
                return -1;
            }

            if (version) {
                System.out.println("Current version of pulsar client is: " + PulsarVersion.getVersion());
                return 0;
            }

            if (help) {
                commandParser.usage();
                return 0;
            }

            try {
                this.updateConfig(); // If the --url, --auth-plugin, or --auth-params parameter are not specified,
                // it will default to the values passed in by the constructor
            } catch (UnsupportedAuthenticationException exp) {
                System.out.println("Failed to load an authentication plugin");
                exp.printStackTrace();
                return -1;
            }

            String chosenCommand = commandParser.getParsedCommand();
            if ("produce".equals(chosenCommand)) {
                return produceCommand.run();
            } else if ("consume".equals(chosenCommand)) {
                return consumeCommand.run();
            } else if ("produce-alternately".equals(chosenCommand)) {
                return produceAlternatelyCommand.run();
            } else if ("ack".equals(chosenCommand)) {
                return ackCommand.run();
            } else{
                System.out.println("Unknown command " + chosenCommand);
                commandParser.usage();
                return -1;
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
            String chosenCommand = commandParser.getParsedCommand();
            if (e instanceof ParameterException) {
                usageFormatter.usage(chosenCommand);
            } else {
                e.printStackTrace();
            }
            return -1;
        }
    }
}
