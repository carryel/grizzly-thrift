/*
 * Copyright (c) 2026 Contributors to the Eclipse Foundation.
 * Copyright (c) 2014, 2017 Oracle and/or its affiliates. All rights reserved.
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

package org.glassfish.grizzly.thrift.client;

import org.apache.thrift.TProcessor;
import org.glassfish.grizzly.filterchain.FilterChainBuilder;
import org.glassfish.grizzly.filterchain.FilterChainContext;
import org.glassfish.grizzly.filterchain.NextAction;
import org.glassfish.grizzly.filterchain.TransportFilter;
import org.glassfish.grizzly.nio.transport.TCPNIOTransport;
import org.glassfish.grizzly.nio.transport.TCPNIOTransportBuilder;
import org.glassfish.grizzly.thrift.CalculatorHandler;
import org.glassfish.grizzly.thrift.TestUtils;
import org.glassfish.grizzly.thrift.ThriftFrameFilter;
import org.glassfish.grizzly.thrift.ThriftServerFilter;
import org.junit.Assert;
import org.junit.Test;
import tutorial.Calculator;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static java.time.Duration.ofSeconds;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * GrizzlyThriftClient's failover test
 *
 * @author Flutia
 */
public class GrizzlyThriftClientFailoverTest {

    private static final int PORT = 7795;
    private static final int INVALID_PORT1 = 7796;
    private static final int FAILOVER_PORT = 7797;

    @Test
    public void testServerRemains() throws Exception {
        // create manager
        final GrizzlyThriftClientManager manager = new GrizzlyThriftClientManager.Builder().build();

        // create builder
        final GrizzlyThriftClient.Builder<Calculator.Client> builder = manager.createThriftClientBuilder("Calculator", new Calculator.Client.Factory());
        builder.connectTimeoutInMillis(1000L);
        builder.responseTimeoutInMillis(500L);
        builder.minConnectionPerServer(10);
        builder.retainLastServer(true);
        // create client
        final ThriftClient<Calculator.Client> calculatorThriftClient = builder.build();
        calculatorThriftClient.addServer(new InetSocketAddress("localhost", PORT));

        final ThriftClientCallback<Calculator.Client, Integer> clientCallback = new ThriftClientCallback<Calculator.Client, Integer>() {
            @Override
            public Integer call(Calculator.Client client) throws Exception {
                return client.add(1, 2);
            }
        };

        TCPNIOTransport thriftServer = null;
        try {
            try {
                calculatorThriftClient.execute(clientCallback);
                Assert.fail();
            } catch (IOException e) {
                assertTrue(e.getMessage().contains("failed to get the valid client"));
            }

            // start server
            Calculator.Processor processor = new Calculator.Processor(new CalculatorHandler());
            thriftServer = createThriftServer(PORT, new ThriftServerFilter(processor));
            Thread.sleep(100);

            // success
            Integer result = calculatorThriftClient.execute(clientCallback);
            assertTrue(result == 3);

        } finally {
            manager.removeThriftClient("Calculator");
            manager.shutdown();

            if (thriftServer != null) {
                thriftServer.shutdown();
            }
        }
    }

    @Test
    public void testFailover() throws Exception {
        // create manager
        final GrizzlyThriftClientManager manager = new GrizzlyThriftClientManager.Builder().build();

        // create builder
        final GrizzlyThriftClient.Builder<Calculator.Client> builder = manager.createThriftClientBuilder("Calculator", new Calculator.Client.Factory());
        builder.connectTimeoutInMillis(1000L);
        builder.responseTimeoutInMillis(500L);
        builder.retainLastServer(true);
        builder.minConnectionPerServer(10);
        // create client
        final ThriftClient<Calculator.Client> calculatorThriftClient = builder.build();
        calculatorThriftClient.addServer(new InetSocketAddress("localhost", PORT));

        final ThriftClientCallback<Calculator.Client, Integer> clientCallback = new ThriftClientCallback<Calculator.Client, Integer>() {
            @Override
            public Integer call(Calculator.Client client) throws Exception {
                return client.add(1, 2);
            }
        };

        // start server
        TCPNIOTransport firstServer = createThriftServer(PORT, new ThriftServerFilter(new Calculator.Processor(new CalculatorHandler())));
        Thread.sleep(100);

        try {
            // success
            Integer result = calculatorThriftClient.execute(clientCallback);
            assertTrue(result == 3);

            // must fail
            firstServer.shutdown();
            try {
                calculatorThriftClient.execute(clientCallback);
                Assert.fail();
            } catch(IOException e) {
                assertTrue(e.getMessage().contains("failed to get the valid client"));
            }

            // start another server
            TCPNIOTransport secondServer = createThriftServer(PORT, new ThriftServerFilter(new Calculator.Processor(new CalculatorHandler())));
            Thread.sleep(100);

            result = calculatorThriftClient.execute(clientCallback);
            assertTrue(result == 3);

            secondServer.shutdown();

        } finally {
            manager.removeThriftClient("Calculator");
            manager.shutdown();
        }
    }

    @Test
    public void testFailoverWithMultiClient() throws Exception {
        // create manager
        final GrizzlyThriftClientManager manager = new GrizzlyThriftClientManager.Builder().build();

        // create builder
        final GrizzlyThriftClient.Builder<Calculator.Client> builder = manager.createThriftClientBuilder("Calculator", new Calculator.Client.Factory());
        // builder.validationCheckMethodName("ping");
        // builder.borrowValidation(true);
        builder.connectTimeoutInMillis(1000L);
        builder.responseTimeoutInMillis(500L);
        builder.minConnectionPerServer(10);
        builder.retainLastServer(true);
        // create client
        final ThriftClient<Calculator.Client> calculatorThriftClient = builder.build();
        calculatorThriftClient.addServer(new InetSocketAddress("localhost", PORT));
        calculatorThriftClient.addServer(new InetSocketAddress("localhost", FAILOVER_PORT));
        calculatorThriftClient.addServer(new InetSocketAddress("localhost", INVALID_PORT1));

        // create server
        final TCPNIOTransport thriftServer = createThriftServer(PORT, new ThriftServerFilter(new Calculator.Processor(new CalculatorHandler())));
        final TCPNIOTransport thriftServerForFailover = createThriftServer(FAILOVER_PORT, new ThriftServerFilter(new Calculator.Processor(new CalculatorHandler())));

        final int clientsNum = Runtime.getRuntime().availableProcessors() * 4;
        final Integer executionNum = 20;
        final CountDownLatch allFinished = new CountDownLatch(clientsNum);
        final ThriftClientCallback<Calculator.Client, Integer> clientCallback = new ThriftClientCallback<Calculator.Client, Integer>() {
            @Override
            public Integer call(Calculator.Client client) throws Exception {
                return client.add(1, 2);
            }
        };

        final AtomicInteger counter = new AtomicInteger(0);
        try {
            for (int i = 0; i < clientsNum; i++) {
                new Thread() {
                    public void run() {
                        try {
                            for (int j = 0; j < executionNum; j++) {
                                try {
                                    Integer result = calculatorThriftClient.execute(clientCallback);
                                    assertTrue(result == 3);
                                    counter.incrementAndGet();

                                } catch (Exception t) {
                                    t.printStackTrace();
                                    Assert.fail();
                                }
                            }
                        } finally {
                            allFinished.countDown();
                        }
                    }
                }.start();
            }
            allFinished.await();
            assertEquals(clientsNum * executionNum, counter.get());

        } finally {
            manager.removeThriftClient("Calculator");
            manager.shutdown();

            if (thriftServer != null) {
                thriftServer.shutdown();
            }
            if (thriftServerForFailover != null) {
                thriftServerForFailover.shutdown();
            }
        }
    }

    @Test
    public void testFailoverWithMultiClientAndServerClose() throws Exception {
        // create manager
        final GrizzlyThriftClientManager manager = new GrizzlyThriftClientManager.Builder().build();

        // create builder
        final GrizzlyThriftClient.Builder<Calculator.Client> builder = manager.createThriftClientBuilder("Calculator", new Calculator.Client.Factory());
        // builder.validationCheckMethodName("ping");
        // builder.borrowValidation(true);
        builder.connectTimeoutInMillis(1000L);
        builder.responseTimeoutInMillis(500L);
        builder.minConnectionPerServer(10);
        builder.retainLastServer(true);
        // create client
        final ThriftClient<Calculator.Client> calculatorThriftClient = builder.build();
        calculatorThriftClient.addServer(new InetSocketAddress("localhost", PORT));
        calculatorThriftClient.addServer(new InetSocketAddress("localhost", FAILOVER_PORT));

        // create server
        TestThriftServerFilter filter = new TestThriftServerFilter(new Calculator.Processor(new CalculatorHandler()));
        final TCPNIOTransport thriftServer = createThriftServer(PORT, filter);
        final TCPNIOTransport thriftServerForFailover = createThriftServer(FAILOVER_PORT, new ThriftServerFilter(new Calculator.Processor(new CalculatorHandler())));

        final int clientsNum = Runtime.getRuntime().availableProcessors() * 4;
        final Integer executionNum = 20;
        final CountDownLatch allFinished = new CountDownLatch(clientsNum);
        final ThriftClientCallback<Calculator.Client, Integer> clientCallback = new ThriftClientCallback<Calculator.Client, Integer>() {
            @Override
            public Integer call(Calculator.Client client) throws Exception {
                return client.add(1, 2);
            }
        };

        final AtomicInteger counter = new AtomicInteger(0);
        try {
            for (int i = 0; i < clientsNum; i++) {
                if (counter.get() == 1) {
                    filter.setDisconnectFlag();
                }

                new Thread() {
                    public void run() {
                        try {
                            for (int j = 0; j < executionNum; j++) {
                                try {
                                    Integer result = calculatorThriftClient.execute(clientCallback);
                                    assertTrue(result == 3);
                                    counter.incrementAndGet();

                                } catch (Exception t) {
                                    t.printStackTrace();
                                    Assert.fail();
                                }
                            }
                        } finally {
                            allFinished.countDown();
                        }
                    }
                }.start();
            }
            allFinished.await();
            assertEquals(clientsNum * executionNum, counter.get());

        } finally {
            manager.removeThriftClient("Calculator");
            manager.shutdown();

            if (thriftServer != null) {
                thriftServer.shutdown();
            }
            if (thriftServerForFailover != null) {
                thriftServerForFailover.shutdown();
            }
        }
    }

    private static TCPNIOTransport createThriftServer(final int port, final ThriftServerFilter filter) throws Exception {
        final FilterChainBuilder filterChainBuilder = FilterChainBuilder.stateless();
        filterChainBuilder.add(new TransportFilter());
        filterChainBuilder.add(new ThriftFrameFilter());
        filterChainBuilder.add(filter);
        final TCPNIOTransport transport = TCPNIOTransportBuilder.newInstance().build();
        transport.setProcessor(filterChainBuilder.build());
        TestUtils.retryUntilSuccess(() -> transport.bind(port), ofSeconds(10), ofSeconds(1));
        transport.start();
        return transport;
    }

    private static class TestThriftServerFilter extends ThriftServerFilter {
        private volatile boolean closeFlag;

        public void setDisconnectFlag() {
            closeFlag = true;
        }

        public TestThriftServerFilter(TProcessor processor) {
            super(processor);
        }

        @Override
        public NextAction handleRead(FilterChainContext ctx) throws IOException {
            if (closeFlag) {
                ctx.getConnection().closeSilently();
                closeFlag = false;
                return ctx.getStopAction();
            }

            return super.handleRead(ctx);
        }
    }
}
