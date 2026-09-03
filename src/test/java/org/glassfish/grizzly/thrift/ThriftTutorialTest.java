/*
 * Copyright (c) 2026 Contributors to the Eclipse Foundation.
 * Copyright (c) 2011, 2017 Oracle and/or its affiliates. All rights reserved.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License v. 2.0, which is available at
 * http://www.eclipse.org/legal/epl-2.0.
 *
 * This Source Code may also be made available under the following Secondary
 * Licenses when the conditions for such availability set forth in the
 * Eclipse Public License v. 2.0 are satisfied: GNU General Public License,
 * version 2 with the GNU Classpath Exception, which is available at
 * https://www.gnu.org/software/classpath/license.html.
 *
 * SPDX-License-Identifier: EPL-2.0 OR GPL-2.0 WITH Classpath-exception-2.0
 */

package org.glassfish.grizzly.thrift;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TTransport;
import org.glassfish.grizzly.Connection;
import org.glassfish.grizzly.Grizzly;
import org.glassfish.grizzly.IOStrategy;
import org.glassfish.grizzly.SocketConnectorHandler;
import org.glassfish.grizzly.filterchain.FilterChain;
import org.glassfish.grizzly.filterchain.FilterChainBuilder;
import org.glassfish.grizzly.filterchain.TransportFilter;
import org.glassfish.grizzly.http.HttpClientFilter;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.grizzly.http.server.NetworkListener;
import org.glassfish.grizzly.nio.transport.TCPNIOConnectorHandler;
import org.glassfish.grizzly.nio.transport.TCPNIOTransport;
import org.glassfish.grizzly.nio.transport.TCPNIOTransportBuilder;
import org.glassfish.grizzly.strategies.LeaderFollowerNIOStrategy;
import org.glassfish.grizzly.strategies.SameThreadIOStrategy;
import org.glassfish.grizzly.strategies.SimpleDynamicNIOStrategy;
import org.glassfish.grizzly.strategies.WorkerThreadIOStrategy;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.glassfish.grizzly.thrift.http.ThriftHttpClientFilter;
import org.glassfish.grizzly.thrift.http.ThriftHttpHandler;
import shared.SharedStruct;
import tutorial.Calculator;
import tutorial.InvalidOperation;
import tutorial.Operation;
import tutorial.Work;

import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runner.RunWith;

import static java.time.Duration.ofSeconds;
import static org.junit.Assert.*;

/**
 * Thrift's tutorial test.
 *
 * @author Bongjae Chang
 */
@RunWith(Parameterized.class)
@SuppressWarnings("unchecked")
public class ThriftTutorialTest {
    private static final int PORT = 7790;
    private final static Logger logger = Grizzly.logger(ThriftTutorialTest.class);

    private final IOStrategy strategy;

    @Parameters
    public static Collection<Object[]> getIOStrategy() {
        return Arrays.asList(new Object[][]{
                {WorkerThreadIOStrategy.getInstance()},
                {LeaderFollowerNIOStrategy.getInstance()},
                {SameThreadIOStrategy.getInstance()},
                {SimpleDynamicNIOStrategy.getInstance()}});
    }

    @Before
    public void before() throws Exception {
        Grizzly.setTrackingThreadCache(true);
    }

    public ThriftTutorialTest(final IOStrategy strategy) {
        this.strategy = strategy;
    }


    @Test
    public void testSimplePackets() throws Exception {
        final int clientsNum = Runtime.getRuntime().availableProcessors();
        final Integer executionNum = 2;

        logger.log(Level.INFO, "** IOStrategy = {0}, clientsNum = {1}, executionNum = {2}", new Object[]{strategy, clientsNum, executionNum});

        Connection connection = null;

        // CalculatorHandler class is thrift's tutorial code.
        // shared.* and tutorial.*' classes are thrift's generated codes based on shared.thrift and tutorial.thrift files in thrift tutorial.
        final CalculatorHandler handler = new CalculatorHandler();
        final Calculator.Processor tprocessor = new Calculator.Processor(handler);

        final FilterChainBuilder filterChainBuilder = FilterChainBuilder.stateless();
        filterChainBuilder.add(new TransportFilter());
        filterChainBuilder.add(new ThriftFrameFilter());
        filterChainBuilder.add(new ThriftServerFilter(tprocessor));
        final TCPNIOTransport transport = TCPNIOTransportBuilder.newInstance().setIOStrategy(strategy).build();
        transport.setProcessor(filterChainBuilder.build());

        try {
            TestUtils.retryUntilSuccess(() -> transport.bind(PORT), ofSeconds(10), ofSeconds(1));
            transport.start();

            for (int i = 0; i < clientsNum; i++) {
                final FilterChainBuilder clientFilterChainBuilder = FilterChainBuilder.stateless();
                clientFilterChainBuilder.add(new TransportFilter());
                clientFilterChainBuilder.add(new ThriftFrameFilter());
                clientFilterChainBuilder.add(new ThriftClientFilter());

                final FilterChain clientChain = clientFilterChainBuilder.build();
                final SocketConnectorHandler connectorHandler =
                        TCPNIOConnectorHandler.builder(transport)
                                .processor(clientChain)
                                .build();

                final Future<Connection> connectFuture = connectorHandler.connect(
                        new InetSocketAddress("localhost", PORT));

                connection = connectFuture.get(10, TimeUnit.SECONDS);
                assertTrue(connection != null);

                final TTransport ttransport = TGrizzlyClientTransport.create(connection);
                final TProtocol tprotocol = new TBinaryProtocol(ttransport);
                final Calculator.Client client = new Calculator.Client(tprotocol);

                for (int j = 0; j < executionNum; j++) {
                    try {
                        perform(client);
                    } catch (TException te) {
                        logger.warning(te.getMessage());
                        fail();
                    }
                }
                ttransport.close();
                connection.closeSilently();
                connection = null;
            }
        } finally {
            if (connection != null) {
                connection.closeSilently();
            }

            transport.shutdownNow();
        }
    }

    @Test
    public void testHttpSimplePackets() throws Exception {
        final int clientsNum = Runtime.getRuntime().availableProcessors();
        final Integer executionNum = 2;

        logger.log(Level.INFO, "** IOStrategy = {0}, clientsNum = {1}, executionNum = {2}", new Object[]{strategy, clientsNum, executionNum});

        Connection connection = null;

        // CalculatorHandler class is thrift's tutorial code.
        // shared.* and tutorial.*' classes are thrift's generated codes based on shared.thrift and tutorial.thrift files in thrift tutorial.
        final CalculatorHandler handler = new CalculatorHandler();
        final Calculator.Processor tprocessor = new Calculator.Processor(handler);
        final HttpServer server = new HttpServer();
        final NetworkListener listener = new NetworkListener("grizzly-thrift-http", NetworkListener.DEFAULT_NETWORK_HOST, PORT);
        server.addListener(listener);
        server.getServerConfiguration().addHttpHandler(new ThriftHttpHandler(tprocessor), "/test");
        server.start();

        final TCPNIOTransport transport = TCPNIOTransportBuilder.newInstance().setIOStrategy(strategy).build();
        try {
            transport.start();

            for (int i = 0; i < clientsNum; i++) {
                final FilterChainBuilder clientFilterChainBuilder = FilterChainBuilder.stateless();
                clientFilterChainBuilder.add(new TransportFilter());
                clientFilterChainBuilder.add(new HttpClientFilter());
                clientFilterChainBuilder.add(new ThriftHttpClientFilter("/test"));
                clientFilterChainBuilder.add(new ThriftClientFilter());

                final FilterChain clientChain = clientFilterChainBuilder.build();
                final SocketConnectorHandler connectorHandler =
                        TCPNIOConnectorHandler.builder(transport)
                                .processor(clientChain)
                                .build();

                final Future<Connection> connectFuture = connectorHandler.connect(
                        new InetSocketAddress("localhost", PORT));

                connection = connectFuture.get(10, TimeUnit.SECONDS);
                assertTrue(connection != null);

                final TTransport ttransport = TGrizzlyClientTransport.create(connection);
                final TProtocol tprotocol = new TBinaryProtocol(ttransport);
                final Calculator.Client client = new Calculator.Client(tprotocol);

                for (int j = 0; j < executionNum; j++) {
                    try {
                        perform(client);
                    } catch (TException te) {
                        logger.warning(te.getMessage());
                        fail();
                    }
                }
                ttransport.close();
                connection.closeSilently();
                connection = null;
            }
        } finally {
            if (connection != null) {
                connection.closeSilently();
            }

            transport.shutdownNow();
            server.shutdownNow();
        }
    }

    private void perform(Calculator.Client client) throws TException {
        client.ping();
        logger.info("ping()");

        int sum = client.add(1, 1);
        logger.log(Level.INFO, "1+1={0}", sum);

        Work work = new Work();

        work.op = Operation.DIVIDE;
        work.num1 = 1;
        work.num2 = 0;
        try {
            client.calculate(1, work);
            logger.info("Whoa we can divide by 0");
            fail();
        } catch (InvalidOperation io) {
            logger.log(Level.INFO, "Invalid operation: {0}", io.why);
        }

        work.op = Operation.SUBTRACT;
        work.num1 = 15;
        work.num2 = 10;
        try {
            int diff = client.calculate(1, work);
            logger.log(Level.INFO, "15-10={0}", diff);
        } catch (InvalidOperation io) {
            logger.log(Level.WARNING, "Invalid operation: {0}", io.why);
            fail();
        }

        SharedStruct log = client.getStruct(1);
        logger.log(Level.INFO, "Check log: {0}", log.value);
    }
}
