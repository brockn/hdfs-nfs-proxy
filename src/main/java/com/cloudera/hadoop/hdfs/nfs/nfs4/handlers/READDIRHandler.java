/**
 * Copyright 2011 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloudera.hadoop.hdfs.nfs.nfs4.handlers;

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.hadoop.hdfs.nfs.Bytes;
import com.cloudera.hadoop.hdfs.nfs.Pair;
import com.cloudera.hadoop.hdfs.nfs.nfs4.Bitmap;
import com.cloudera.hadoop.hdfs.nfs.nfs4.DirectoryEntry;
import com.cloudera.hadoop.hdfs.nfs.nfs4.DirectoryList;
import com.cloudera.hadoop.hdfs.nfs.nfs4.NFS4Exception;
import com.cloudera.hadoop.hdfs.nfs.nfs4.NFS4Handler;
import com.cloudera.hadoop.hdfs.nfs.nfs4.OpaqueData8;
import com.cloudera.hadoop.hdfs.nfs.nfs4.Session;
import com.cloudera.hadoop.hdfs.nfs.nfs4.attrs.Attribute;
import com.cloudera.hadoop.hdfs.nfs.nfs4.requests.READDIRRequest;
import com.cloudera.hadoop.hdfs.nfs.nfs4.responses.READDIRResponse;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

public class READDIRHandler extends OperationRequestHandler<READDIRRequest, READDIRResponse> {
  protected static final Logger LOGGER = LoggerFactory.getLogger(READDIRHandler.class);

  @Override
  protected READDIRResponse doHandle(NFS4Handler server, Session session,
      READDIRRequest request) throws NFS4Exception, IOException {
    if(session.getCurrentFileHandle() == null) {
      throw new NFS4Exception(NFS4ERR_NOFILEHANDLE);
    }
    Path path = server.getPath(session.getCurrentFileHandle());
    FileSystem fs = session.getFileSystem();
    FileStatus fileStatus = fs.getFileStatus(path);
    if(!fileStatus.isDir()) {
      throw new NFS4Exception(NFS4ERR_NOTDIR);
    }
    
    FileStatus[] fileStati = fs.listStatus(path);
    if(fileStati == null) {
      // we have already check the dir exists, this means it's empty
      fileStati = new FileStatus[0];
    }
    Arrays.sort(fileStati);
    // low cookie numbers are "special" in the linux kernel
    // since we don't return . and .. they fake them with low #s
    long verifer = fileStati.length;
    long cookie = request.getCookie();
    if(cookie == 0) {
      cookie += NFS4_COOKIE_OFFSET;
    } else {
      cookie++;
    }
    long requestVerifer = Bytes.toLong(request.getCookieVerifer().getData());
    if((requestVerifer > 0 && verifer !=requestVerifer) 
        || (cookie > 0 && cookie > (fileStati.length + NFS4_COOKIE_OFFSET))) {
      LOGGER.warn("BAD COOKIE verifier = " + verifer + ", request.getVerifier() = " + 
          requestVerifer + ", cookie = " + cookie + ", dirLength = " + fileStati.length);
      throw new NFS4Exception(NFS4ERR_BAD_COOKIE);
    }
    // TODO improve this guess
    // we can only send maxCount bytes including xdr overhead
    // save 100 bytes for the readDir header and for RPC header
    // I saw about 100 bytes in wireshark for linux and pulled
    // the RPC number out of my arse. I guessed high.
    int messageSize = 100 + 150;
    int maxMessageSize = request.getMaxCount();
    // TODO this check should be after we add the first entry to the response
    if(messageSize > maxMessageSize) {
      throw new NFS4Exception(NFS4ERR_TOOSMALL);
    }
    List<DirectoryEntry>  entries = Lists.newArrayList();    
    for (; cookie < (fileStati.length + NFS4_COOKIE_OFFSET); cookie++) {
      fileStatus = fileStati[(int)(cookie - NFS4_COOKIE_OFFSET)];
      // we have to force creation of a file handle because that creates
      // a fileid which is required later in the getAttrs.
      server.createFileHandle(fileStatus.getPath());
      DirectoryEntry entry = readAttrs(server, session, request.getAttrs(), fs, fileStatus);
      entry.setName(fileStatus.getPath().getName());
      entry.setCookie(cookie);
      
      // If this entry is more than we can send
      // break out and the rest will be handled
      // in a future call
      int entryLength = entry.getWireSize();
      if(messageSize + entryLength >= maxMessageSize) {
        break;
      }
      messageSize += entryLength;
      entries.add(entry);
      server.incrementMetric("NFS_READDIR_ENTRIES", 1);
    }
    DirectoryList entryList = new DirectoryList();
    entryList.setDirEntries(entries);
    entryList.setEOF(cookie == (fileStati.length + NFS4_COOKIE_OFFSET));
    
    READDIRResponse response = createResponse();
    response.setStatus(NFS4_OK);
    response.setCookieVerifer(new OpaqueData8(verifer));
    response.setDirectoryList(entryList);
    return response;
  }
  protected DirectoryEntry readAttrs(NFS4Handler server, Session session, Bitmap requestedAttrs, FileSystem fs, FileStatus fileStatus) throws NFS4Exception, IOException {
    Pair<Bitmap, ImmutableList<Attribute>> pair = Attribute.getAttrs(server, session, requestedAttrs, fs, fileStatus);
    DirectoryEntry entry = new DirectoryEntry();
    entry.setAttrs(pair.getFirst());
    entry.setAttrValues(pair.getSecond());
    return entry;
  }
    @Override
  protected READDIRResponse createResponse() {
    return new READDIRResponse();
  }
}
