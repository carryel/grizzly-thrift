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

import org.apache.thrift.TConfiguration;
import org.apache.thrift.transport.TTransportException;
import org.glassfish.grizzly.Buffer;
import org.glassfish.grizzly.utils.BufferOutputStream;

/**
 * TGrizzlyServerTransport is the server-side TTransport.
 * <p>
 * Input and output buffers are already allocated by Grizzly or
 * ThriftServerFilter.
 *
 * @author Bongjae Chang
 */
public class TGrizzlyServerTransport extends AbstractTGrizzlyTransport {

    private final Buffer input;
    private final BufferOutputStream outputStream;

    public TGrizzlyServerTransport(final Buffer input, final BufferOutputStream outputStream, final TConfiguration config) throws TTransportException {
        super(config);
        this.input = input;
        this.outputStream = outputStream;
    }

    @Override
    public boolean isOpen() {
        return true;
    }

    @Override
    public void close() {
    }

    @Override
    public void flush() throws TTransportException {
        resetConsumedMessageSize(-1L);
    }

    @Override
    protected Buffer getInputBuffer() throws TTransportException {
        return input;
    }

    @Override
    protected BufferOutputStream getOutputStream() {
        return outputStream;
    }
}
