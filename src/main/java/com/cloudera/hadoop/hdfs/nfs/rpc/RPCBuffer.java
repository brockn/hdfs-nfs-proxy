/**
 * Copyright 2011 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.cloudera.hadoop.hdfs.nfs.rpc;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IOUtils;
import org.apache.log4j.Logger;

import com.cloudera.hadoop.hdfs.nfs.Bytes;
import com.google.common.base.Charsets;

/**
 * Class implements the basic RPC protocol. We do not have unsigned integers in
 * java so we wrap those calls in signed int/long but check to make sure they
 * have not rolled, throwing exception if they have. This works for now, if we
 * find that the signed space is not large enough, we might have to handle this
 * differently.
 */
public class RPCBuffer {

  protected static final Logger LOGGER = Logger.getLogger(RPCBuffer.class);
  /**
   * XDR requires all data to be a multiple of 4 bytes thus if a write is not,
   * we need to pad it
   */
  protected static final int XDR_UNIT_SIZE = 4;
  /**
   * In RPC terms this is the last `packet' for this RPC all.
   */
  protected static final int RPC_LAST_FRAGEMANT = 0x80000000;
  /**
   * Remaining bits are the size.
   */
  protected static final int RPC_SIZE_MASK = 0x7fffffff;
  protected static final int DEFAULT_BUFFER_SIZE = 1024;

  /**
   * This method will read an RPC request/response from InputStream and store
   * the resulting data in the buffer
   *
   * @param in
   * @throws IOException
   */
  public static RPCBuffer from(InputStream in) throws IOException {
    List<byte[]> buffers = new ArrayList<byte[]>(1);
    byte[] header = new byte[4];
    int size = 0;
    boolean last = false;
    while (!last) {
      readFully(in, header, 0, header.length);
      int raw = Bytes.toInt(header);
      size = raw;
      last = (size & RPC_LAST_FRAGEMANT) != 0;
      size &= RPC_SIZE_MASK;
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Read Header for Packet of Size " + size + ", last = " + last);
      }
      byte[] buffer = new byte[size];
      IOUtils.readFully(in, buffer, 0, size);
      buffers.add(buffer);
    }
    byte[] buffer = Bytes.merge(buffers); // XXX this absolutely bad
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("From InputStream " + Bytes.asHex(buffer));
    }
    return new RPCBuffer(buffer, buffer.length);
  }

  /**
   * Exactly the same as IOUtils.readFully except this throws EOFException on
   * EOF
   *
   * @param in
   * @param buf
   * @param off
   * @param len
   * @throws IOException
   */
  protected static void readFully(InputStream in, byte buf[],
      int off, int len) throws IOException {
    int toRead = len;
    while (toRead > 0) {
      int ret = in.read(buf, off, toRead);
      if (ret < 0) {
        throw new EOFException("Premature EOF from inputStream: off = " + off + ", len = " + len + ", toRead = " + toRead);
      }
      toRead -= ret;
      off += ret;
    }
  }
  protected ByteBuffer mBuffer;

  /**
   * Create a new buffer, for writing with the default buffer size.
   */
  public RPCBuffer() {
    this(DEFAULT_BUFFER_SIZE);
  }

  /**
   * Create a buffer, likely for reading with the specified byte array
   */
  public RPCBuffer(byte[] buffer) {
    this(buffer, buffer.length);
  }

  /**
   * Create a buffer, likely for reading with the specified byte array and
   * length
   *
   * @param buffer
   * @param length
   */
  public RPCBuffer(byte[] buffer, int length) {
    this.mBuffer = ByteBuffer.wrap(buffer, 0, length);
  }

  /**
   * Create a buffer, for writing, with size bytes capacity
   *
   * @param size
   */
  public RPCBuffer(int size) {
    mBuffer = ByteBuffer.allocate(Math.max(size, DEFAULT_BUFFER_SIZE));
  }

  protected void ensureCapacity(int length) {
    if (mBuffer.position() + length > mBuffer.capacity()) {
      length = Math.max(length * 2, DEFAULT_BUFFER_SIZE) + mBuffer.capacity();
      ByteBuffer buffer = ByteBuffer.allocate(mBuffer.capacity() + length);
      mBuffer.flip();
      buffer.put(mBuffer);
      mBuffer = buffer;
    }
  }

  public void skip(int length) {
    writeBytes(new byte[length]);
  }

  public int length() {
    return mBuffer.limit();
  }

  public void putInt(int index, int value) {
    mBuffer.putInt(index, value);
  }

  public boolean readBoolean() {
    int value = readInt();
    if (!(value == 1 || value == 0)) {
      throw new RuntimeException("readBoolean read " + value);
    }
    return value == 1;
  }

  /**
   * Read a 4 byte integer and then that number of bytes.
   *
   * @return
   */
  public byte[] readBytes() {
    return readBytes(readUint32());
  }

  public byte[] readBytes(int length) {
    if (length < 0) {
      throw new RuntimeException("Cannot create " + length + " sized array");
    }
    byte[] bytes = new byte[length];
    mBuffer.get(bytes, 0, bytes.length);
    align();
    return bytes;
  }

  public int readInt() {
    int value = mBuffer.getInt();
    align();
    return value;
  }

  public long readLong() {
    long value = mBuffer.getLong();
    align();
    return value;
  }

  /**
   * Read a 4 byte integer length and associated bytes converting the bytes to
   * a String.
   *
   * @return
   */
  public String readString() {
    byte[] bytes = readBytes();
    String s;
    try {
      s = new String(bytes, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
    return s;
  }

  public int readUint32() {
    int i = readInt();
    if (i < 0) {
      throw new NumberFormatException("Unsigned rolled over " + Integer.toBinaryString(i));
    }
    return i;
  }

  public long readUint64() {
    long l = readLong();
    if (l < 0) {
      throw new NumberFormatException("Unsigned rolled over " + Long.toHexString(l));
    }
    return l;
  }

  public int position() {
    return mBuffer.position();
  }
  
  public int limit() {
    return mBuffer.limit();
  }

  /**
   * XDR RFC requires we write data aligned by a unit size. This method aligns
   * the current buffer to that unit size.
   */
  void align() {
    int remainder = mBuffer.position() % XDR_UNIT_SIZE;
    if (remainder > 0) {
      mBuffer.put(new byte[XDR_UNIT_SIZE - remainder]);
    }
  }

  /**
   * Write the lenght of the buffer and then the buffer itself to the
   * outputstream.
   *
   * @param out
   * @throws IOException if the outputstream thows an IOException during the
   * write
   */
  public void write(OutputStream out) throws IOException {
    putInt(0, RPC_LAST_FRAGEMANT | limit() - 4);
    out.write(mBuffer.array(), 0, limit());
  }

  public void writeBoolean(boolean bool) {
    writeInt(bool ? 1 : 0);
  }

  public void writeBytes(byte[] bytes) {
    writeBytes(bytes, 0, bytes.length);
  }

  public void writeBytes(byte[] bytes, int offset, int length) {
    ensureCapacity(length);
    mBuffer.put(bytes, offset, length);
    align();
  }

  public void writeInt(int value) {
    ensureCapacity(4);
    mBuffer.putInt(value);
    align();
  }

  public void writeLong(long value) {
    ensureCapacity(8);
    mBuffer.putLong(value);
    align();
  }

  public void writeString(String s) {
    byte[] bytes = s.getBytes(Charsets.UTF_8);
    writeInt(bytes.length);
    writeBytes(bytes);
  }

  public void writeUint32(int i) {
    if (i < 0) {
      throw new NumberFormatException("Unsigned rolled over " + Long.toBinaryString(i));
    }
    writeInt(i);
  }

  public void writeUint64(long l) {
    if (l < 0) {
      throw new NumberFormatException("Unsigned rolled over " + l + ", binary = " + Long.toBinaryString(l));
    }
    writeLong(l);
  }
  public void writeRPCBUffer(RPCBuffer buffer) {
    int length = buffer.length();
    byte[] bytes = buffer.readBytes(length);
    writeBytes(bytes);
  }


  public void flip() {
    mBuffer.flip();
  }
}
