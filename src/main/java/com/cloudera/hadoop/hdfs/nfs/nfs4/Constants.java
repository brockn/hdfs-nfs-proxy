/**
 * Copyright 2012 The Apache Software Foundation
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
package com.cloudera.hadoop.hdfs.nfs.nfs4;

import java.net.InetAddress;
import java.net.UnknownHostException;


public class Constants {

  public static final String ANONYMOUS_USERNAME = "nobody";


  public static final int RPC_MESSAGE_TYPE_CALL = 0;
  public static final int RPC_MESSAGE_TYPE_REPLY = 1;


  public static final int RPC_ACCEPT_STATE_ACCEPT = 0;



  public static final int RPC_AUTH_NULL = 0;
  public static final int RPC_AUTH_UNIX = 1;
  public static final int RPC_AUTH_GSS = 6;

  public static final int RPC_VERIFIER_NULL = 0;

  public static final int RPC_VERIFIER_GSS = 6;


  /*
   * Reply Status
   */
  public static final int RPC_REPLY_STATE_ACCEPT = 0;
  public static final int RPC_REPLY_STATE_DENIED = 1;

  // only used in com.sun
  public static final int RPC_ACCEPTED = 0;
  public static final int RPC_DENIED = 1;

  /*
   * Accept Status
   */
  public static final int RPC_ACCEPT_SUCCESS = 0;
  public static final int RPC_ACCEPT_PROG_UNAVAIL = 1;
  public static final int RPC_ACCEPT_PROG_MISMATCH = 2;
  public static final int RPC_ACCEPT_PROC_UNAVAIL = 3;
  public static final int RPC_ACCEPT_GARBAGE_ARGS = 4;
  public static final int RPC_ACCEPT_SYSTEM_ERR = 5;

  /*
   * Reject Status
   */
  public static final int RPC_REJECT_MISMATCH = 0;
  public static final int RPC_REJECT_AUTH_ERROR = 1;

  // for future reference
  public static final int RPC_AUTH_STATUS_BADCRED      = 1;  /* bad credentials (seal broken) */
  public static final int RPC_AUTH_STATUS_REJECTEDCRED = 2;  /* client must begin new session */
  public static final int RPC_AUTH_STATUS_BADVERF      = 3;  /* bad verifier (seal broken)    */
  public static final int RPC_AUTH_STATUS_REJECTEDVERF = 4;  /* verifier expired or replayed  */
  public static final int RPC_AUTH_STATUS_TOOWEAK      = 5;   /* rejected for security reasons */


  public static final int RPC_OPAQUE_AUTH_MAX = 400;

  public static final int RPC_VERSION = 2;



  public final static int FHSIZE = 32;



  public final static int RPCSEC_GSS_VERSION = 1;

  public final static int RPCSEC_GSS_DATA = 0;
  public final static int RPCSEC_GSS_INIT = 1;
  public final static int RPCSEC_GSS_CONTINUE_INIT = 2;
  public final static int RPCSEC_GSS_DESTROY = 3;


  public final static int RPCSEC_GSS_SERVICE_NONE = 1;
  public final static int RPCSEC_GSS_SERVICE_INTEGRITY = 2;
  public final static int RPCSEC_GSS_SERVICE_PRIVACY = 3;






  public static final int NFS_PROG = 100003;
  public static final int NFS_VERSION = 4;


  public final static int NFS_PROC_NULL = 0;
  public final static int NFS_PROC_COMPOUND = 1;

  public final static int NFS4_MAX_RWSIZE = 32768 * 2;
  public final static int NFS4_MAX_NAME = 8000;
  public final static long NFS4_MAX_FILE_SIZE = Long.MAX_VALUE;



  /*
   * ACCESS bits
   */
  public final static int NFS_ACCESS_READ = 0x0001;
  public final static int NFS_ACCESS_LOOKUP = 0x0002;
  public final static int NFS_ACCESS_MODIFY = 0x0004;
  public final static int NFS_ACCESS_EXTEND = 0x0008;
  public final static int NFS_ACCESS_DELETE = 0x0010;
  public final static int NFS_ACCESS_EXECUTE = 0x0020;

  public static final int NFS4_OPAQUE_LIMIT = 1024;
  public static final int NFS4_REG = 1;
  public static final int NFS4_DIR = 2;


  public static final int NFS4_OK                 = 0;    /* everything is okay      */
  public static final int NFS4ERR_PERM            = 1;    /* caller not privileged   */
  public static final int NFS4ERR_NOENT           = 2;    /* no such file/directory  */
  public static final int NFS4ERR_IO              = 5;    /* hard I/O error          */
  public static final int NFS4ERR_NXIO            = 6;    /* no such device          */
  public static final int NFS4ERR_ACCESS          = 13;   /* access denied           */
  public static final int NFS4ERR_EXIST           = 17;   /* file already exists     */
  public static final int NFS4ERR_XDEV            = 18;   /* different filesystems   */
  /* Unused/reserved        19 */
  public static final int NFS4ERR_NOTDIR          = 20;   /* should be a directory   */
  public static final int NFS4ERR_ISDIR           = 21;   /* should not be directory */
  public static final int NFS4ERR_INVAL           = 22;   /* invalid argument        */
  public static final int NFS4ERR_FBIG            = 27;   /* file exceeds server max */
  public static final int NFS4ERR_NOSPC           = 28;   /* no space on filesystem  */
  public static final int NFS4ERR_ROFS            = 30;   /* read-only filesystem    */
  public static final int NFS4ERR_MLINK           = 31;   /* too many hard links     */
  public static final int NFS4ERR_NAMETOOLONG     = 63;   /* name exceeds server max */
  public static final int NFS4ERR_NOTEMPTY        = 66;   /* directory not empty     */
  public static final int NFS4ERR_DQUOT           = 69;   /* hard quota limit reached*/
  public static final int NFS4ERR_STALE           = 70;   /* file no longer exists   */
  public static final int NFS4ERR_BADHANDLE       = 10001;/* Illegal filehandle      */
  public static final int NFS4ERR_BAD_COOKIE      = 10003;/* READDIR cookie is stale */
  public static final int NFS4ERR_NOTSUPP         = 10004;/* operation not supported */
  public static final int NFS4ERR_TOOSMALL        = 10005;/* response limit exceeded */
  public static final int NFS4ERR_SERVERFAULT     = 10006;/* undefined server error  */
  public static final int NFS4ERR_BADTYPE         = 10007;/* type invalid for CREATE */
  public static final int NFS4ERR_DELAY           = 10008;/* file "busy" - retry     */
  public static final int NFS4ERR_SAME            = 10009;/* nverify says attrs same */
  public static final int NFS4ERR_DENIED          = 10010;/* lock unavailable        */
  public static final int NFS4ERR_EXPIRED         = 10011;/* lock lease expired      */
  public static final int NFS4ERR_LOCKED          = 10012;/* I/O failed due to lock  */
  public static final int NFS4ERR_GRACE           = 10013;/* in grace period         */
  public static final int NFS4ERR_FHEXPIRED       = 10014;/* filehandle expired      */
  public static final int NFS4ERR_SHARE_DENIED    = 10015;/* share reserve denied    */
  public static final int NFS4ERR_WRONGSEC        = 10016;/* wrong security flavor   */
  public static final int NFS4ERR_CLID_INUSE      = 10017;/* clientid in use         */

  public static final int NFS4ERR_RESOURCE        = 10018;/* resource exhaustion     */
  public static final int NFS4ERR_MOVED           = 10019;/* filesystem relocated    */
  public static final int NFS4ERR_NOFILEHANDLE    = 10020;/* current FH is not set   */
  public static final int NFS4ERR_MINOR_VERS_MISMATCH = 10021;/* minor vers not supp */
  public static final int NFS4ERR_STALE_CLIENTID  = 10022;/* server has rebooted     */
  public static final int NFS4ERR_STALE_STATEID   = 10023;/* server has rebooted     */
  public static final int NFS4ERR_OLD_STATEID     = 10024;/* state is out of sync    */
  public static final int NFS4ERR_BAD_STATEID     = 10025;/* incorrect stateid       */
  public static final int NFS4ERR_BAD_SEQID       = 10026;/* request is out of seq.  */
  public static final int NFS4ERR_NOT_SAME        = 10027;/* verify - attrs not same */
  public static final int NFS4ERR_LOCK_RANGE      = 10028;/* lock range not supported*/
  public static final int NFS4ERR_SYMLINK         = 10029;/* should be file/directory*/
  public static final int NFS4ERR_RESTOREFH       = 10030;/* no saved filehandle     */
  public static final int NFS4ERR_LEASE_MOVED     = 10031;/* some filesystem moved   */
  public static final int NFS4ERR_ATTRNOTSUPP     = 10032;/* recommended attr not sup*/
  public static final int NFS4ERR_NO_GRACE        = 10033;/* reclaim outside of grace*/
  public static final int NFS4ERR_RECLAIM_BAD     = 10034;/* reclaim error at server */
  public static final int NFS4ERR_RECLAIM_CONFLICT = 10035;/* conflict on reclaim    */
  public static final int NFS4ERR_BADXDR          = 10036;/* XDR decode failed       */
  public static final int NFS4ERR_LOCKS_HELD      = 10037;/* file locks held at CLOSE*/
  public static final int NFS4ERR_OPENMODE        = 10038;/* conflict in OPEN and I/O*/
  public static final int NFS4ERR_BADOWNER        = 10039;/* owner translation bad   */
  public static final int NFS4ERR_BADCHAR         = 10040;/* utf-8 char not supported*/
  public static final int NFS4ERR_BADNAME         = 10041;/* name not supported      */
  public static final int NFS4ERR_BAD_RANGE       = 10042;/* lock range not supported*/
  public static final int NFS4ERR_LOCK_NOTSUPP    = 10043;/* no atomic up/downgrade  */
  public static final int NFS4ERR_OP_ILLEGAL      = 10044;/* undefined operation     */
  public static final int NFS4ERR_DEADLOCK        = 10045;/* file locking deadlock   */
  public static final int NFS4ERR_FILE_OPEN       = 10046;/* open file blocks op.    */
  public static final int NFS4ERR_ADMIN_REVOKED   = 10047;/* lockowner state revoked */
  public static final int NFS4ERR_CB_PATH_DOWN    = 10048; /* callback path down      */



  public static final int NFS4_OP_ACCESS               = 3;
  public static final int NFS4_OP_CLOSE                = 4;
  public static final int NFS4_OP_COMMIT               = 5;
  public static final int NFS4_OP_CREATE               = 6;
  public static final int NFS4_OP_DELEGPURGE           = 7;
  public static final int NFS4_OP_DELEGRETURN          = 8;
  public static final int NFS4_OP_GETATTR              = 9;
  public static final int NFS4_OP_GETFH                = 10;
  public static final int NFS4_OP_LINK                 = 11;
  public static final int NFS4_OP_LOCK                 = 12;
  public static final int NFS4_OP_LOCKT                = 13;
  public static final int NFS4_OP_LOCKU                = 14;
  public static final int NFS4_OP_LOOKUP               = 15;
  public static final int NFS4_OP_LOOKUPP              = 16;
  public static final int NFS4_OP_NVERIFY              = 17;
  public static final int NFS4_OP_OPEN                 = 18;
  public static final int NFS4_OP_OPENATTR             = 19;
  public static final int NFS4_OP_OPEN_CONFIRM         = 20;
  public static final int NFS4_OP_OPEN_DOWNGRADE       = 21;
  public static final int NFS4_OP_PUTFH                = 22;
  public static final int NFS4_OP_PUTPUBFH             = 23;
  public static final int NFS4_OP_PUTROOTFH            = 24;
  public static final int NFS4_OP_READ                 = 25;
  public static final int NFS4_OP_READDIR              = 26;
  public static final int NFS4_OP_READLINK             = 27;
  public static final int NFS4_OP_REMOVE               = 28;
  public static final int NFS4_OP_RENAME               = 29;
  public static final int NFS4_OP_RENEW                = 30;
  public static final int NFS4_OP_RESTOREFH            = 31;
  public static final int NFS4_OP_SAVEFH               = 32;
  public static final int NFS4_OP_SECINFO              = 33;
  public static final int NFS4_OP_SETATTR              = 34;
  public static final int NFS4_OP_SETCLIENTID          = 35;
  public static final int NFS4_OP_SETCLIENTID_CONFIRM  = 36;
  public static final int NFS4_OP_VERIFY               = 37;
  public static final int NFS4_OP_WRITE                = 38;
  public static final int NFS4_OP_RELEASE_LOCKOWNER    = 39;


  public static final int NFS4_FATTR4_SUPPORTED_ATTRS    = 0;
  public static final int NFS4_FATTR4_TYPE               = 1;
  public static final int NFS4_FATTR4_FH_EXPIRE_TYPE     = 2;
  public static final int NFS4_FATTR4_CHANGE             = 3;
  public static final int NFS4_FATTR4_SIZE               = 4;
  public static final int NFS4_FATTR4_LINK_SUPPORT       = 5;
  public static final int NFS4_FATTR4_SYMLINK_SUPPORT    = 6;
  public static final int NFS4_FATTR4_NAMED_ATTR         = 7;
  public static final int NFS4_FATTR4_FSID               = 8;
  public static final int NFS4_FATTR4_UNIQUE_HANDLES     = 9;
  public static final int NFS4_FATTR4_LEASE_TIME         = 10;
  public static final int NFS4_FATTR4_RDATTR_ERROR       = 11;
  public static final int NFS4_FATTR4_ACL_SUPPORT         = 13;
  public static final int NFS4_FATTR4_FILEHANDLE         = 19;

  public static final int NFS4_FATTR4_CASE_INSENSITIVE   = 16;
  public static final int NFS4_FATTR4_CASE_PRESERVING    = 17;
  public static final int NFS4_FATTR4_CHOWN_RESTRICTED   = 18;
  public static final int NFS4_FATTR4_FILEID             = 20;
  public static final int NFS4_FATTR4_FILES_AVAIL        = 21;
  public static final int NFS4_FATTR4_FILES_FREE         = 22;
  public static final int NFS4_FATTR4_FILES_TOTAL        = 23;
  public static final int NFS4_FATTR4_FS_LOCATIONS       = 24;
  public static final int NFS4_FATTR4_HIDDEN             = 25;
  public static final int NFS4_FATTR4_HOMOGENEOUS        = 26;
  public static final int NFS4_FATTR4_MAXFILESIZE        = 27;
  public static final int NFS4_FATTR4_MAXLINK            = 28;
  public static final int NFS4_FATTR4_MAXNAME            = 29;
  public static final int NFS4_FATTR4_MAXREAD            = 30;
  public static final int NFS4_FATTR4_MAXWRITE           = 31;
  public static final int NFS4_FATTR4_MIMETYPE           = 32;
  public static final int NFS4_FATTR4_MODE               = 33;
  public static final int NFS4_FATTR4_NO_TRUNC           = 34;
  public static final int NFS4_FATTR4_NUMLINKS           = 35;
  public static final int NFS4_FATTR4_OWNER              = 36;
  public static final int NFS4_FATTR4_OWNER_GROUP        = 37;
  public static final int NFS4_FATTR4_QUOTA_AVAIL_HARD   = 38;
  public static final int NFS4_FATTR4_QUOTA_AVAIL_SOFT   = 39;
  public static final int NFS4_FATTR4_QUOTA_USED         = 40;
  public static final int NFS4_FATTR4_RAWDEV             = 41;
  public static final int NFS4_FATTR4_SPACE_AVAIL        = 42;
  public static final int NFS4_FATTR4_SPACE_FREE         = 43;
  public static final int NFS4_FATTR4_SPACE_TOTAL        = 44;
  public static final int NFS4_FATTR4_SPACE_USED         = 45;
  public static final int NFS4_FATTR4_SYSTEM             = 46;
  public static final int NFS4_FATTR4_TIME_ACCESS        = 47;
  public static final int NFS4_FATTR4_TIME_ACCESS_SET    = 48;
  public static final int NFS4_FATTR4_TIME_BACKUP        = 49;
  public static final int NFS4_FATTR4_TIME_CREATE        = 50;
  public static final int NFS4_FATTR4_TIME_DELTA         = 51;
  public static final int NFS4_FATTR4_TIME_METADATA      = 52;
  public static final int NFS4_FATTR4_TIME_MODIFY        = 53;
  public static final int NFS4_FATTR4_TIME_MODIFY_SET    = 54;
  public static final int NFS4_FATTR4_MOUNTED_ON_FILEID  = 55;


  public static final int NFS4_OPEN4_NOCREATE  = 0;
  public static final int NFS4_OPEN4_CREATE    = 1;


  public static final int NFS4_CREATE_UNCHECKED4      = 0;
  public static final int NFS4_CREATE_GUARDED4        = 1;
  public static final int NFS4_CREATE_EXCLUSIVE4      = 2;

  public static final int NFS4_CLAIM_NULL              = 0;
  public static final int NFS4_CLAIM_PREVIOUS          = 1;
  public static final int NFS4_CLAIM_DELEGATE_CUR      = 2;
  public static final int NFS4_CLAIM_DELEGATE_PREV     = 3;

  public static final int NFS4_OPEN4_SHARE_ACCESS_READ   = 0x00000001;
  public static final int NFS4_OPEN4_SHARE_ACCESS_WRITE  = 0x00000002;
  public static final int NFS4_OPEN4_SHARE_ACCESS_BOTH   = 0x00000003;

  public static final int NFS4_OPEN4_SHARE_DENY_NONE     = 0x00000000;
  public static final int NFS4_OPEN4_SHARE_DENY_READ     = 0x00000001;
  public static final int NFS4_OPEN4_SHARE_DENY_WRITE    = 0x00000002;
  public static final int NFS4_OPEN4_SHARE_DENY_BOTH     = 0x00000003;


  /* Client must confirm open */
  public static final int  NFS4_OPEN4_RESULT_CONFIRM      = 0x00000002;
  /* Type of file locking behavior at the server */
  public static final int  NFS4_OPEN4_RESULT_LOCKTYPE_POSIX = 0x00000004;


  public static final int    NFS4_FH4_PERSISTENT          = 0x00000000;
  public static final int    NFS4_FH4_NOEXPIRE_WITH_OPEN  = 0x00000001;
  public static final int    NFS4_FH4_VOLATILE_ANY        = 0x00000002;
  public static final int    NFS4_FH4_VOL_MIGRATION       = 0x00000004;
  public static final int    NFS4_FH4_VOL_RENAME          = 0x00000008;

  public static final InetAddress LOCALHOST;

  static {
    try {
      LOCALHOST = InetAddress.getByName("localhost");
    } catch (UnknownHostException e) {
      throw new RuntimeException("Unable to find localhost", e);
    }
  }
  
  public static final long ONE_MB = 1024L * 1024L;

  public static final int ROOT_USER_UID = 0;
  public static final long NFS4_COOKIE_OFFSET = Integer.MAX_VALUE;

  public static final int NFS4_COMMIT_UNSTABLE4       = 0;
  public static final int NFS4_COMMIT_DATA_SYNC4      = 1;
  public static final int NFS4_COMMIT_FILE_SYNC4      = 2;

  public static final int NFS4_SET_TO_SERVER_TIME4 = 0;
  public static final int NFS4_SET_TO_CLIENT_TIME4 = 1;


  public static final String USER_ID_MAPPER_CACHE = "hdfs.nfs.useridmapper.cache";
  public static final String USER_ID_MAPPER_CLASS = "hdfs.nfs.useridmapper";
  public static final String NFS_OWNER_DOMAIN = "hdfs.nfs.nfs4.owner.domain";

  public static final String NFS_FILEHANDLE_STORE_CLASS = "hdfs.nfs.nfs4.filehandle.store.class";
  public static final String NFS_FILEHANDLE_STORE_FILE = "hdfs.nfs.nfs4.filehandle.store.file";

  public static final String DEFAULT_NFS_FILEHANDLE_STORE_FILE = "hdfs.nfs.nfs4.filehandle.store.file";

  public static final String RPC_MAX_THREADS = "hdfs.nfs.rpcserver.max.threads";

  public static final String RPC_MAX_PENDING_REQUESTS = "hdfs.nfs.rpcworker.max.pending.requests";

  public static final String RPC_RETRANSMIT_PENALTY_THRESHOLD = "hdfs.nfs.rpcworker.restransmit.penalty.threshold";

  public static final String TEMP_DIRECTORIES = "hdfs.nfs.temp.dirs";

}
