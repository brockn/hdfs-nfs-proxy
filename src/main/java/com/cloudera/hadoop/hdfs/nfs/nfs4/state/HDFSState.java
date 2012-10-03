package com.cloudera.hadoop.hdfs.nfs.nfs4.state;

import static com.cloudera.hadoop.hdfs.nfs.PathUtils.realPath;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.NFS4ERR_DENIED;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.NFS4ERR_FILE_OPEN;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.NFS4ERR_ISDIR;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.NFS4ERR_OLD_STATEID;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.NFS4ERR_PERM;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.NFS4ERR_SERVERFAULT;
import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.NFS4ERR_STALE;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.log4j.Logger;

import com.cloudera.hadoop.hdfs.nfs.nfs4.FileHandle;
import com.cloudera.hadoop.hdfs.nfs.nfs4.FileHandleStore;
import com.cloudera.hadoop.hdfs.nfs.nfs4.FileHandleStoreEntry;
import com.cloudera.hadoop.hdfs.nfs.nfs4.Metrics;
import com.cloudera.hadoop.hdfs.nfs.nfs4.NFS4Exception;
import com.cloudera.hadoop.hdfs.nfs.nfs4.StateID;
import com.cloudera.hadoop.hdfs.nfs.nfs4.WriteOrderHandler;
import com.google.common.collect.Maps;

public class HDFSState {
  protected static final Logger LOGGER = Logger.getLogger(HDFSState.class);
  protected final Map<String, HDFSFile> mPathMap = Maps.newConcurrentMap();
  protected final Map<FileHandle, HDFSFile> mFileHandleMap = Maps.newConcurrentMap();
  protected final long mStartTime = System.currentTimeMillis();
  protected final Configuration mConfiguration;
  protected final ClientFactory mClientFactory = new ClientFactory();
  protected static final AtomicLong FILEID = new AtomicLong(0L);
  protected static final Random RANDOM = new Random();
  protected final Map<FSDataOutputStream, WriteOrderHandler> mWriteOrderHandlerMap = Maps.newConcurrentMap();
  protected final FileHandleStore mFileHandleStore;
  private final  Metrics mMetrics;
  
  public HDFSState(Configuration configuration, Metrics metrics) {
    mConfiguration = configuration;
    mMetrics = metrics;
    mFileHandleStore = FileHandleStore.get(mConfiguration);

    for (FileHandleStoreEntry entry : mFileHandleStore.getAll()) {
      FileHandle fileHandle = new FileHandle(entry.getFileHandle());
      HDFSFile holder = new HDFSFile(fileHandle, entry.getPath(), entry.getFileID());
      putFileHandle(fileHandle, entry.getPath(), holder);
    }
    
  }
  /**
   * Files which are in the process of being created need to exist from a NFS
   * perspective even if they do not exist from an HDFS perspective. This
   * method intercepts requests for files that are open and calls
   * FileSystem.exists for other files.
   *
   * @param fs
   * @param path
   * @return true if the file exists or is open for write
   * @throws IOException
   */
  public synchronized boolean fileExists(FileSystem fs, Path path)
      throws IOException {
    HDFSFile fileHolder = mPathMap.get(realPath(path));
    if (fileHolder != null && fileHolder.isOpenForWrite()) {
      return true;
    }
    return fs.exists(path);
  }

  /**
   * Return a FileHandle if it exists or create a new one.
   *
   * @param path
   * @return a FileHandle
   * @throws IOException
   */
  public synchronized FileHandle createFileHandle(Path path) throws IOException {
    String realPath = realPath(path);
    HDFSFile file = mPathMap.get(realPath);
    if (file != null) {
      return file.getFileHandle();
    }
    String s = UUID.randomUUID().toString().replace("-", "");
    byte[] bytes = s.getBytes();
    FileHandle fileHandle = new FileHandle(bytes);
    HDFSFile holder = new HDFSFile(fileHandle, realPath, getNextFileID());
    FileHandleStoreEntry storeEntry = new FileHandleStoreEntry(bytes, realPath, holder.getFileID());
    mFileHandleStore.storeFileHandle(storeEntry);
    putFileHandle(fileHandle, realPath, holder);
    return fileHandle;
  }

  protected synchronized void putFileHandle(FileHandle fileHandle, String path, HDFSFile holder) {
    mPathMap.put(path, holder);
    mFileHandleMap.put(fileHandle, holder);
    mMetrics.incrementMetric("FILE_HANDLES_CREATED", 1);
  }

  /**
   * The NFS fileid is translated to an inode on the host. As such we need to
   * have a unique fileid for each path. Return the file id for a given path
   * or throw a NFSException(STALE).
   *
   * @param path
   * @return fileid
   * @throws NFS4Exception if the file does not have a FileHandle/
   */
  public synchronized long getFileID(Path path) throws NFS4Exception {
    HDFSFile file = mPathMap.get(realPath(path));
    if (file != null) {
      return file.getFileID();
    }
    throw new NFS4Exception(NFS4ERR_STALE, "Path " + realPath(path));
  }
  public boolean closeWouldBlock(FileHandle fileHandle) throws IOException {
    HDFSFile fileHolder = null;
    WriteOrderHandler writeOrderHandler = null;
    OpenResource<?> file = null;
    synchronized (this) {
      fileHolder = mFileHandleMap.get(fileHandle);
      if (fileHolder != null) {
        if (fileHolder.isOpenForWrite()) {
          file = fileHolder.getFSDataOutputStream();
          synchronized (mWriteOrderHandlerMap) {
            writeOrderHandler = mWriteOrderHandlerMap.get(file.get());
          }
        }
      }
    }
    if(writeOrderHandler != null) {
      return writeOrderHandler.closeWouldBlock();
    }
    return false;
  }
  /**
   * Close the resources allocated in the server, block until the writes for
   * this file have been processed and close the underlying stream. No lock is
   * held while we wait for the writes to be processed.
   *
   * @param sessionID
   * @param stateID
   * @param seqID
   * @param fileHandle
   * @return and updated StateID
   * @throws NFS4Exception if the file is open or the stateid is old
   * @throws IOException if the underlying streams throw an excpetion
   */
  public StateID close(String sessionID, StateID stateID, int seqID,
      FileHandle fileHandle) throws NFS4Exception, IOException {
    HDFSFile fileHolder = null;
    WriteOrderHandler writeOrderHandler = null;
    OpenResource<?> file = null;
    synchronized (this) {
      fileHolder = mFileHandleMap.get(fileHandle);
      if (fileHolder != null) {
        if (fileHolder.isOpenForWrite()) {
          LOGGER.info(sessionID + " Closing " + fileHolder.getPath()
              + " for write");
          file = fileHolder.getFSDataOutputStream();
        } else {
          LOGGER.info(sessionID + " Closing " + fileHolder.getPath()
              + " for read");
          file = fileHolder.getFSDataInputStream(stateID);
        }
        if (file == null) {
          throw new NFS4Exception(NFS4ERR_OLD_STATEID);
        }
        if (!file.isOwnedBy(stateID)) {
          throw new NFS4Exception(NFS4ERR_FILE_OPEN);
        }
        file.setSequenceID(seqID);
        synchronized (mWriteOrderHandlerMap) {
          writeOrderHandler = mWriteOrderHandlerMap.remove(file.get());
        }
      } else {
        throw new NFS4Exception(NFS4ERR_STALE);
      }
    }
    if (writeOrderHandler != null) {
      writeOrderHandler.close(); // blocks
      LOGGER.info(sessionID + " Closed WriteOrderHandler for "
          + fileHolder.getPath());
    }
    synchronized (this) {
      file.close();
      mMetrics.incrementMetric("FILES_CLOSED", 1);
      return file.getStateID();
    }
  }

  /**
   * Confirm a file in accordance with NFS OPEN_CONFIRM.
   *
   * @param stateID
   * @param seqID
   * @param fileHandle
   * @return the stateid associated with the open file
   * @throws NFS4Exception if the stateid is old or open file is owned by
   * another stateid
   */
  public synchronized StateID confirm(StateID stateID, int seqID,
      FileHandle fileHandle) throws NFS4Exception {
    HDFSFile fileHolder = mFileHandleMap.get(fileHandle);
    OpenResource<?> file = null;
    if (fileHolder != null) {
      if (fileHolder.isOpenForWrite()) {
        file = fileHolder.getFSDataOutputStream();
      } else {
        file = fileHolder.getFSDataInputStream(stateID);
      }
      if (file == null) {
        throw new NFS4Exception(NFS4ERR_OLD_STATEID);
      }
      if (!file.isOwnedBy(stateID)) {
        throw new NFS4Exception(NFS4ERR_FILE_OPEN); // TODO lock unavailable
        // should be _LOCK?
      }
      file.setConfirmed(true);
      file.setSequenceID(seqID);
      return file.getStateID();
    }
    throw new NFS4Exception(NFS4ERR_STALE);
  }

  /**
   * Returns true if the file is open.
   *
   * @param path
   * @return true if the file is open for read or write
   */
  public synchronized boolean isFileOpen(Path path) {
    HDFSFile fileHolder = mPathMap.get(realPath(path));
    if (fileHolder != null) {
      return fileHolder.isOpen();
    }
    return false;
  }

  /**
   * Open if not open or obtain the input stream opened by the StateID.
   *
   * @param stateID
   * @param fs
   * @param fileHandle
   * @return FSDataInputStream for reading
   * @throws NFS4Exception if the file is already open for write, the open is
   * not confirmed or the file handle is stale.
   * @throws IOException if the file open throws an IOException
   */
  public synchronized FSDataInputStream forRead(StateID stateID, FileSystem fs,
      FileHandle fileHandle) throws NFS4Exception, IOException {
    HDFSFile fileHolder = mFileHandleMap.get(fileHandle);
    if (fileHolder != null) {
      if (fileHolder.isOpenForWrite()) {
        throw new NFS4Exception(NFS4ERR_FILE_OPEN); // TODO lock unavailable
        // should be _LOCK?
      }
      Path path = new Path(fileHolder.getPath());
      OpenResource<FSDataInputStream> file = fileHolder.getFSDataInputStream(stateID);
      if (file != null) {
        if (!file.isConfirmed()) {
          throw new NFS4Exception(NFS4ERR_DENIED);
        }
        return file.get();
      }
      FileStatus status = fs.getFileStatus(path);
      if (status.isDir()) {
        throw new NFS4Exception(NFS4ERR_ISDIR);
      }
      FSDataInputStream in = fs.open(path);
      mMetrics.incrementMetric("FILES_OPENED_READ", 1);
      fileHolder.putFSDataInputStream(stateID, in);
      return in;
    }
    throw new NFS4Exception(NFS4ERR_STALE);
  }

  /**
   * Create or return the a WriteOrderHandler for a given FSDataOutputStream.
   *
   * @param name
   * @param out
   * @return the WriteOrderHandler
   * @throws IOException of the output stream throws an IO Exception while
   * creating the WriteOrderHandler.
   */
  public WriteOrderHandler getWriteOrderHandler(String name,
      FSDataOutputStream out) throws IOException {
    WriteOrderHandler writeOrderHandler;
    synchronized (mWriteOrderHandlerMap) {
      writeOrderHandler = mWriteOrderHandlerMap.get(out);
      if (writeOrderHandler == null) {
        writeOrderHandler = new WriteOrderHandler(out);
        writeOrderHandler.setDaemon(true);
        writeOrderHandler.setName("WriteOrderHandler-" + name);
        writeOrderHandler.start();
        mWriteOrderHandlerMap.put(out, writeOrderHandler);
      }
    }
    return writeOrderHandler;
  }

  /**
   * Get the WriteOrderHandler for a commit operation.
   *
   * @param fs
   * @param fileHandle
   * @return WriteOrderHandler
   * @throws NFS4Exception if the file handle is stale
   */
  public synchronized WriteOrderHandler forCommit(FileSystem fs,
      FileHandle fileHandle) throws NFS4Exception {
    HDFSFile fileHolder = mFileHandleMap.get(fileHandle);
    if (fileHolder != null) {
      OpenResource<FSDataOutputStream> file = fileHolder.getFSDataOutputStream();
      if (file != null) {
        synchronized (mWriteOrderHandlerMap) {
          if (mWriteOrderHandlerMap.containsKey(file.get())) {
            return mWriteOrderHandlerMap.get(file.get());
          }
        }
      }
    }
    throw new NFS4Exception(NFS4ERR_STALE);
  }

  /**
   *
   * @param stateID
   * @param fs
   * @param fileHandle
   * @param overwrite
   * @return
   * @throws NFS4Exception
   * @throws IOException
   */
  public synchronized FSDataOutputStream forWrite(StateID stateID,
      FileSystem fs, FileHandle fileHandle, boolean overwrite)
          throws NFS4Exception, IOException {
    HDFSFile fileHolder = mFileHandleMap.get(fileHandle);
    if (fileHolder != null) {
      OpenResource<FSDataOutputStream> file = fileHolder.getFSDataOutputStream();
      if (file != null) {
        if (file.isOwnedBy(stateID)) {
          return file.get();
        }
        throw new NFS4Exception(NFS4ERR_FILE_OPEN);
      }
      Path path = new Path(fileHolder.getPath());
      boolean exists = fs.exists(path);
      // If overwrite = false, fs.create throws IOException which
          // is useless. In case of IOE do we always return EXIST?
      // doesn't seem to make sense. As such, I am mitigating the issue
      // even if there is a known race between the exists and create
      if (!overwrite && exists) {
        // append to a file
        // We used to be NFS4ERR_EXIST here but the linux client behaved 
        // rather oddly. It would open the file with overwrite=true but 
        // then send the data which was to be appended at offset 0
        throw new NFS4Exception(NFS4ERR_PERM,
            "File Exists and overwrite = false", true);
      }
      if (path.getParent() != null) {
        // TODO bad perms will fail with IOException, perhaps we should check
        // that file can be created before trying to so we can return the
        // correct error perm denied
        // check(user, groups, status, access);
      }
      if (exists && fs.getFileStatus(path).isDir()) {
        throw new NFS4Exception(NFS4ERR_ISDIR);
      }
      FSDataOutputStream out = fs.create(path, overwrite);
      mMetrics.incrementMetric("FILES_OPENED_WRITE", 1);
      fileHolder.setFSDataOutputStream(stateID, out);
      return out;
    }
    throw new NFS4Exception(NFS4ERR_STALE);
  }

  protected boolean check(String user, List<String> groups, FileStatus
      status, FsAction access) {
    FsPermission mode = status.getPermission();
    if (user.equals(status.getOwner())) { // user class
      if (mode.getUserAction().implies(access)) {
        return true;
      }
    } else if (groups.contains(status.getGroup())) { // group class
      if (mode.getGroupAction().implies(access)) {
        return true;
      }
    } else { // other class
      if (mode.getOtherAction().implies(access)) {
        return true;
      }
    }
    return false;
  }
  /**
   * Get the Path for a FileHandle
   *
   * @param fileHandle
   * @return Path for FileHandler
   * @throws NFS4Exception if FileHandle is stale
   */
  public Path getPath(FileHandle fileHandle) throws NFS4Exception {
    HDFSFile file = mFileHandleMap.get(fileHandle);
    if (file != null) {
      return new Path(file.getPath());
    }
    throw new NFS4Exception(NFS4ERR_STALE);
  }

  /**
   * Get the FileHandle for a Path
   *
   * @param path
   * @return FileHandle for path
   * @throws NFS4Exception if the FileHandle for the Path is stale
   */
  public FileHandle getFileHandle(Path path) throws NFS4Exception {
    HDFSFile file = mPathMap.get(realPath(path));
    if (file != null) {
      return file.getFileHandle();
    }
    throw new NFS4Exception(NFS4ERR_STALE);
  }

  /**
   * Files open for write will have an unreliable length according to the name
   * node. As such, this call intercepts calls for open files and returns the
   * length of the as reported by the output stream.
   *
   * @param status
   * @return the current file length including data written to the output
   * stream
   * @throws NFS4Exception if the getPos() call of the output stream throws
   * IOException
   */
  public long getFileSize(FileStatus status) throws NFS4Exception {
    HDFSFile fileHolder = mPathMap.get(realPath(status.getPath()));
    if (fileHolder != null) {
      OpenResource<FSDataOutputStream> file = fileHolder.getFSDataOutputStream();
      if (file != null) {
        try {
          FSDataOutputStream out = file.get();
          return out.getPos();
        } catch (IOException e) {
          throw new NFS4Exception(NFS4ERR_SERVERFAULT, e);
        }
      }
    }
    return status.getLen();
  }

  /**
   * @return the ClientFactory in use by the NFS4 Handler
   */
  public ClientFactory getClientFactory() {
    return mClientFactory;
  }
  
  public void close() throws IOException {
    mFileHandleStore.close();
  }

  public void incrementMetric(String name, long count) {
    mMetrics.incrementMetric(name, count);
  }

  private long getNextFileID() {
    synchronized (RANDOM) {
      return FILEID.addAndGet(RANDOM.nextInt(20) + 1);
    }
  }
  public long getStartTime() {
    return mStartTime;
  }
}
