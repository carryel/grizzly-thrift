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

package org.glassfish.grizzly.thrift.http;

import java.io.IOException;

import org.apache.thrift.TConfiguration;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TTransport;
import org.glassfish.grizzly.Buffer;
import org.glassfish.grizzly.http.server.HttpHandler;
import org.glassfish.grizzly.http.server.Request;
import org.glassfish.grizzly.http.server.Response;
import org.glassfish.grizzly.memory.MemoryManager;
import org.glassfish.grizzly.thrift.TGrizzlyServerTransport;
import org.glassfish.grizzly.utils.BufferOutputStream;

/**
 * ThriftHttpHandler is a server-side http handler for Thrift RPC processors.
 * <p>
 * You can set the specific response size by constructor for optimal
 * performance.
 * <p>
 * Usages:
 *
 * <pre>
 * {@code
 * final user-generated.thrift.Processor tprocessor = new user-generated.thrift.Processor(new user-generated.thrift.Handler);
 * final HttpServer server = new HttpServer();
 * final NetworkListener listener = new NetworkListener("yourServerName", yourHost, yourPort);
 * server.addListener(listener);
 * server.getServerConfiguration().addHttpHandler(new ThriftHttpHandler(tprocessor), "/yourUriPath");
 * server.start();
 * // release
 * //...
 * }
 * </pre>
 *
 * @author Bongjae Chang
 */
public class ThriftHttpHandler extends HttpHandler {

    private static final int THRIFT_DEFAULT_RESPONSE_BUFFER_SIZE = 40 * 1024; // 40k;
    private static final String THRIFT_HTTP_CONTENT_TYPE = "application/x-thrift";

    private final TProcessor processor;
    private final TProtocolFactory protocolFactory;
    private final TConfiguration config;
    private final int responseSize;

    public ThriftHttpHandler(final TProcessor processor) {
        this(processor, new TBinaryProtocol.Factory(), null, THRIFT_DEFAULT_RESPONSE_BUFFER_SIZE);
    }

    public ThriftHttpHandler(final TProcessor processor, final TProtocolFactory protocolFactory) {
        this(processor, protocolFactory, null, THRIFT_DEFAULT_RESPONSE_BUFFER_SIZE);
    }

    public ThriftHttpHandler(final TProcessor processor, final TProtocolFactory protocolFactory, final TConfiguration config) {
        this(processor, protocolFactory, config, THRIFT_DEFAULT_RESPONSE_BUFFER_SIZE);
    }

    public ThriftHttpHandler(final TProcessor processor, final int responseSize) {
        this(processor, new TBinaryProtocol.Factory(), null, responseSize);
    }

    public ThriftHttpHandler(final TProcessor processor, final TProtocolFactory protocolFactory, final int responseSize) {
        this(processor, protocolFactory, null, responseSize);
    }

    public ThriftHttpHandler(final TProcessor processor, final TProtocolFactory protocolFactory, final TConfiguration config, final int responseSize) {
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
    public void service(Request request, Response response) throws Exception {
        if (processor == null) {
            throw new IllegalStateException("TProcessor should not be null");
        }

        final Buffer inputBuffer = request.getInputBuffer().getBuffer();
        if (inputBuffer == null) {
            throw new IOException("input buffer should not be null");
        }
        if (!inputBuffer.hasRemaining()) {
            throw new IOException("input buffer doesn't have the remaining data");
        }

        final MemoryManager memoryManager = request.getContext().getMemoryManager();
        final BufferOutputStream outputStream = new BufferOutputStream(memoryManager, memoryManager.allocate(responseSize));
        final TTransport ttransport = new TGrizzlyServerTransport(inputBuffer, outputStream, config);
        final TProtocol protocol = protocolFactory.getProtocol(ttransport);
        try {
            processor.process(protocol, protocol);
        } catch (TException te) {
            ttransport.close();
            inputBuffer.dispose();
            outputStream.getBuffer().dispose();
            throw new IOException(te);
        }
        inputBuffer.dispose();

        final Buffer outputBuffer = outputStream.getBuffer();
        outputBuffer.trim();
        outputBuffer.allowBufferDispose(true);
        response.setContentType(THRIFT_HTTP_CONTENT_TYPE);
        response.setContentLength(outputBuffer.remaining());
        response.getNIOOutputStream().write(outputBuffer);
        try {
            outputStream.close();
        } catch (IOException ignore) {
        }

        ttransport.close();
    }
}
