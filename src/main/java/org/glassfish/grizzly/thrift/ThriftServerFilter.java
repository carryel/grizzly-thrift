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

import java.io.IOException;

import org.apache.thrift.TConfiguration;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.glassfish.grizzly.Buffer;
import org.glassfish.grizzly.filterchain.BaseFilter;
import org.glassfish.grizzly.filterchain.FilterChainContext;
import org.glassfish.grizzly.filterchain.NextAction;
import org.glassfish.grizzly.memory.MemoryManager;
import org.glassfish.grizzly.utils.BufferOutputStream;

/**
 * ThriftServerFilter is a server-side filter for Thrift RPC processors.
 * <p>
 * You can set the specific response size by constructor for optimal
 * performance.
 * <p>
 * Usages:
 *
 * <pre>
 * {@code
 * final FilterChainBuilder serverFilterChainBuilder = FilterChainBuilder.stateless();
 * final user-generated.thrift.Processor tprocessor = new user-generated.thrift.Processor(new user-generated.thrift.Handler);
 * <p>
 * serverFilterChainBuilder.add(new TransportFilter()).add(new ThriftFrameFilter()).add(new ThriftServerFilter(tprocessor));
 * <p>
 * final TCPNIOTransport transport = TCPNIOTransportBuilder.newInstance().build();
 * transport.setProcessor(serverFilterChainBuilder.build());
 * transport.bind(port);
 * transport.start();
 * // release
 * //...
 * }
 * </pre>
 *
 * @author Bongjae Chang
 */
public class ThriftServerFilter extends BaseFilter {

    private static final int THRIFT_DEFAULT_RESPONSE_BUFFER_SIZE = 40 * 1024; // 40k;

    private final TProcessor processor;
    private final TProtocolFactory protocolFactory;
    private final TConfiguration config;
    private final int responseSize;

    public ThriftServerFilter(final TProcessor processor) {
        this(processor, new TBinaryProtocol.Factory(), null, THRIFT_DEFAULT_RESPONSE_BUFFER_SIZE);
    }

    public ThriftServerFilter(final TProcessor processor, final TProtocolFactory protocolFactory) {
        this(processor, protocolFactory, null, THRIFT_DEFAULT_RESPONSE_BUFFER_SIZE);
    }

    public ThriftServerFilter(final TProcessor processor, final int responseSize) {
        this(processor, new TBinaryProtocol.Factory(), null, responseSize);
    }

    public ThriftServerFilter(final TProcessor processor, final TConfiguration config) {
        this(processor, new TBinaryProtocol.Factory(), config, THRIFT_DEFAULT_RESPONSE_BUFFER_SIZE);
    }

    public ThriftServerFilter(final TProcessor processor, final TProtocolFactory protocolFactory, final int responseSize) {
        this(processor, protocolFactory, null, responseSize);
    }

    public ThriftServerFilter(final TProcessor processor, final TProtocolFactory protocolFactory, final TConfiguration config) {
        this(processor, protocolFactory, config, THRIFT_DEFAULT_RESPONSE_BUFFER_SIZE);
    }

    public ThriftServerFilter(final TProcessor processor, final TProtocolFactory protocolFactory, final TConfiguration config, final int responseSize) {
        this.processor = processor;
        if (protocolFactory == null) {
            this.protocolFactory = new TBinaryProtocol.Factory();
        } else {
            this.protocolFactory = protocolFactory;
        }
        if (config == null) {
            this.config = TConfiguration.DEFAULT;
        } else {
            this.config = config;
        }
        if (responseSize < THRIFT_DEFAULT_RESPONSE_BUFFER_SIZE) {
            this.responseSize = THRIFT_DEFAULT_RESPONSE_BUFFER_SIZE;
        } else {
            this.responseSize = responseSize;
        }
    }

    @Override
    public NextAction handleRead(FilterChainContext ctx) throws IOException {
        if (processor == null) {
            throw new IllegalArgumentException("TProcessor could not be null");
        }
        final Buffer input = ctx.getMessage();
        if (input == null) {
            throw new IOException("input message could not be null");
        }
        if (!input.hasRemaining()) {
            return ctx.getStopAction();
        }

        final MemoryManager memoryManager = ctx.getMemoryManager();
        final BufferOutputStream outputStream = new BufferOutputStream(memoryManager, memoryManager.allocate(responseSize));
        final TTransport ttransport;
        try {
            ttransport = new TGrizzlyServerTransport(input, outputStream, config);
        } catch (TTransportException tte) {
            input.dispose();
            outputStream.getBuffer().dispose();
            throw new IOException(tte);
        }
        final TProtocol protocol = protocolFactory.getProtocol(ttransport);
        try {
            processor.process(protocol, protocol);
        } catch (TException te) {
            ttransport.close();
            input.dispose();
            outputStream.getBuffer().dispose();
            throw new IOException(te);
        }
        input.dispose();
        final Buffer output = outputStream.getBuffer();
        output.trim();
        output.allowBufferDispose(true);
        ctx.write(ctx.getAddress(), output, null);
        try {
            outputStream.close();
        } catch (IOException ignore) {
        }
        ttransport.close();
        return ctx.getStopAction();
    }
}
