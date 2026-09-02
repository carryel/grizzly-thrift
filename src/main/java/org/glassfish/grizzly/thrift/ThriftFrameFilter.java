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
import org.glassfish.grizzly.Buffer;
import org.glassfish.grizzly.Connection;
import org.glassfish.grizzly.Grizzly;
import org.glassfish.grizzly.attributes.Attribute;
import org.glassfish.grizzly.filterchain.BaseFilter;
import org.glassfish.grizzly.filterchain.FilterChainContext;
import org.glassfish.grizzly.filterchain.NextAction;
import org.glassfish.grizzly.memory.Buffers;
import org.glassfish.grizzly.memory.MemoryManager;

/**
 * ThriftFrameFilter supports TFramedTranport that ensures a fully read message
 * by preceding messages with a 4-byte frame size.
 * <p>
 * If the frame size exceeds the max size which you can set by constructor's
 * parameter, exception will be thrown.
 *
 * @author Bongjae Chang
 */
public class ThriftFrameFilter extends BaseFilter {

    private static final int THRIFT_FRAME_HEADER_LENGTH = 4;
    private static final int DEFAULT_MAX_THRIFT_FRAME_LENGTH = 512 * 1024;
    private final int maxFrameLength;

    private final Attribute<Integer> lengthAttribute = Grizzly.DEFAULT_ATTRIBUTE_BUILDER.createAttribute("ThriftFilter.FrameSize");

    public ThriftFrameFilter() {
        this(TConfiguration.DEFAULT_MAX_FRAME_SIZE);
    }

    public ThriftFrameFilter(final int maxFrameLength) {
        if (maxFrameLength < DEFAULT_MAX_THRIFT_FRAME_LENGTH) {
            this.maxFrameLength = DEFAULT_MAX_THRIFT_FRAME_LENGTH;
        } else {
            this.maxFrameLength = maxFrameLength;
        }
    }

    @Override
    public NextAction handleRead(FilterChainContext ctx) throws IOException {
        final Buffer input = ctx.getMessage();
        if (input == null) {
            throw new IOException("input message could not be null");
        }
        if (!input.hasRemaining()) {
            return ctx.getStopAction();
        }
        final Connection connection = ctx.getConnection();
        if (connection == null) {
            throw new IOException("connection could not be null");
        }

        Integer frameLength = lengthAttribute.get(connection);
        if (frameLength == null) {
            if (input.remaining() < THRIFT_FRAME_HEADER_LENGTH) {
                return ctx.getStopAction(input);
            }
            frameLength = input.getInt();
            if (frameLength > maxFrameLength) {
                throw new IOException("current frame length(" + frameLength + ") exceeds the max frame length(" + maxFrameLength + ")");
            }
            lengthAttribute.set(connection, frameLength);
        }

        final int inputBufferLength = input.remaining();
        if (inputBufferLength < frameLength) {
            return ctx.getStopAction(input);
        }
        lengthAttribute.remove(connection);

        // Check if the input buffer has more than 1 complete thrift message
        // If yes - split up the first message and the remainder
        final Buffer remainder = inputBufferLength > frameLength ? input.split(frameLength) : null;
        ctx.setMessage(input);

        // Instruct FilterChain to store the remainder (if any) and continue execution
        return ctx.getInvokeAction(remainder);
    }

    @Override
    public NextAction handleWrite(FilterChainContext ctx) throws IOException {
        final Buffer body = ctx.getMessage();
        if (body == null) {
            throw new IOException("Input message could not be null");
        }
        if (!body.hasRemaining()) {
            return ctx.getStopAction();
        }

        final MemoryManager memoryManager = ctx.getMemoryManager();

        final int frameLength = body.remaining();
        final Buffer header = memoryManager.allocate(THRIFT_FRAME_HEADER_LENGTH);
        header.allowBufferDispose(true);
        header.putInt(frameLength);
        header.trim();

        final Buffer resultBuffer = Buffers.appendBuffers(memoryManager, header, body);
        resultBuffer.allowBufferDispose(true);
        ctx.setMessage(resultBuffer);
        return ctx.getInvokeAction();
    }
}
