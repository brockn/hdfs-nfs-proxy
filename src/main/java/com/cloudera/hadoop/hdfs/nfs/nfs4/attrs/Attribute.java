package com.cloudera.hadoop.hdfs.nfs.nfs4.attrs;

import static com.cloudera.hadoop.hdfs.nfs.nfs4.Constants.*;

import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;

import com.cloudera.hadoop.hdfs.nfs.Pair;
import com.cloudera.hadoop.hdfs.nfs.nfs4.Bitmap;
import com.cloudera.hadoop.hdfs.nfs.nfs4.Identifiable;
import com.cloudera.hadoop.hdfs.nfs.nfs4.MessageBase;
import com.cloudera.hadoop.hdfs.nfs.nfs4.NFS4Exception;
import com.cloudera.hadoop.hdfs.nfs.nfs4.NFS4Handler;
import com.cloudera.hadoop.hdfs.nfs.nfs4.Session;
import com.cloudera.hadoop.hdfs.nfs.nfs4.StateID;
import com.cloudera.hadoop.hdfs.nfs.rpc.RPCBuffer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

public abstract class Attribute implements MessageBase, Identifiable {
  protected static final Logger LOGGER = LoggerFactory.getLogger(Attribute.class);
  

  static class AttributeHolder {
    Class<? extends Attribute> clazz;
    AttributeHandler<? extends Attribute> handler;
    public AttributeHolder(Class<? extends Attribute> clazz,
        AttributeHandler<? extends Attribute> handler) {
      super();
      this.clazz = clazz;
      this.handler = handler;
    }
    
  }
  
  
  static ImmutableMap<Integer, AttributeHolder> attributes = 
      ImmutableMap.<Integer, AttributeHolder>builder()
            .put(NFS4_FATTR4_ACL_SUPPORT, new AttributeHolder(ACLSupport.class, new ACLSupportHandler()))
            .put(NFS4_FATTR4_CHANGE, new AttributeHolder(ChangeID.class, new ChangeIDHandler()))
            .put(NFS4_FATTR4_CASE_INSENSITIVE, new AttributeHolder(CaseInsensitive.class, new CaseInsensitiveHandler()))
            .put(NFS4_FATTR4_CASE_PRESERVING, new AttributeHolder(CasePreserving.class, new CasePreservingHandler()))
            .put(NFS4_FATTR4_CHOWN_RESTRICTED, new AttributeHolder(ChownRestricted.class, new ChownRestrictedHandler()))
            .put(NFS4_FATTR4_TIME_CREATE, new AttributeHolder(CreateTime.class, new CreateTimeHandler()))
            .put(NFS4_FATTR4_FILEID, new AttributeHolder(FileID.class, new FileIDHandler()))
            .put(NFS4_FATTR4_FILEHANDLE, new AttributeHolder(FileHandle.class, new FileHandleHandler()))
            .put(NFS4_FATTR4_FILES_AVAIL, new AttributeHolder(FilesAvailable.class, new FilesAvailableHandler()))
            .put(NFS4_FATTR4_FILES_FREE, new AttributeHolder(FilesFree.class, new FilesFreeHandler()))
            .put(NFS4_FATTR4_FILES_TOTAL, new AttributeHolder(FilesTotal.class, new FilesTotalHandler()))
            .put(NFS4_FATTR4_FH_EXPIRE_TYPE, new AttributeHolder(FHExpireType.class, new FHExpireTypeHandler()))
            .put(NFS4_FATTR4_FSID, new AttributeHolder(FileSystemID.class, new FileSystemIDHandler()))
            .put(NFS4_FATTR4_HOMOGENEOUS, new AttributeHolder(Homogeneous.class, new HomogeneousHandler()))
            .put(NFS4_FATTR4_LINK_SUPPORT, new AttributeHolder(LinkSupport.class, new LinkSupportHandler()))
            .put(NFS4_FATTR4_LEASE_TIME, new AttributeHolder(LeaseTime.class, new LeaseTimeHandler()))
            .put(NFS4_FATTR4_UNIQUE_HANDLES, new AttributeHolder(UniqueHandlesSupport.class, new UniqueHandlesSupportHandler()))
            .put(NFS4_FATTR4_MAXFILESIZE, new AttributeHolder(MaxFile.class, new MaxFileHandler()))
            .put(NFS4_FATTR4_MAXLINK, new AttributeHolder(MaxLink.class, new MaxLinkHandler()))
            .put(NFS4_FATTR4_MAXNAME, new AttributeHolder(MaxName.class, new MaxNameHandler()))
            .put(NFS4_FATTR4_MAXREAD, new AttributeHolder(MaxRead.class, new MaxReadHandler()))
            .put(NFS4_FATTR4_MAXWRITE, new AttributeHolder(MaxWrite.class, new MaxWriteHandler()))
            .put(NFS4_FATTR4_MODE, new AttributeHolder(Mode.class, new ModeHandler()))
            .put(NFS4_FATTR4_NAMED_ATTR, new AttributeHolder(NamedAttrSupport.class, new NamedAttrSupportHandler()))
            .put(NFS4_FATTR4_NUMLINKS, new AttributeHolder(NumLink.class, new NumLinkHandler()))
            .put(NFS4_FATTR4_NO_TRUNC, new AttributeHolder(NoTruncate.class, new NoTruncateHandler()))
            .put(NFS4_FATTR4_OWNER, new AttributeHolder(Owner.class, new OwnerHandler()))
            .put(NFS4_FATTR4_OWNER_GROUP, new AttributeHolder(OwnerGroup.class, new OwnerGroupHandler()))
            .put(NFS4_FATTR4_RAWDEV, new AttributeHolder(RawDevice.class, new RawDeviceHandler()))
            .put(NFS4_FATTR4_SIZE, new AttributeHolder(Size.class, new SizeHandler()))
            .put(NFS4_FATTR4_SUPPORTED_ATTRS, new AttributeHolder(SupportedAttributes.class, new SupportedAttributesHandler()))
            .put(NFS4_FATTR4_SPACE_AVAIL, new AttributeHolder(SpaceAvailable.class, new SpaceAvailableHandler()))
            .put(NFS4_FATTR4_SPACE_FREE, new AttributeHolder(SpaceFree.class, new SpaceFreeHandler()))
            .put(NFS4_FATTR4_SPACE_USED, new AttributeHolder(SpaceUsed.class, new SpaceUsedHandler()))
            .put(NFS4_FATTR4_SPACE_TOTAL, new AttributeHolder(SpaceTotal.class, new SpaceTotalHandler()))
            .put(NFS4_FATTR4_SYMLINK_SUPPORT, new AttributeHolder(SymLinkSupport.class, new SymLinkSupportHandler()))
            .put(NFS4_FATTR4_TIME_ACCESS, new AttributeHolder(AccessTime.class, new AccessTimeHandler()))
            .put(NFS4_FATTR4_TIME_ACCESS_SET, new AttributeHolder(SetAccessTime.class, new SetAccessTimeHandler()))
            .put(NFS4_FATTR4_TIME_METADATA, new AttributeHolder(MetadataTime.class, new MetadataTimeHandler()))
            .put(NFS4_FATTR4_TIME_MODIFY, new AttributeHolder(ModifyTime.class, new ModifyTimeHandler()))
            .put(NFS4_FATTR4_TIME_MODIFY_SET, new AttributeHolder(SetModifyTime.class, new SetModifyTimeHandler()))
            .put(NFS4_FATTR4_TYPE, new AttributeHolder(Type.class, new TypeHandler()))
            .build();

  public static Bitmap getSupported() {
    Bitmap attrs = new Bitmap();
    for(Integer id : attributes.keySet()) {
      attrs.set(id);
    }
    return attrs;
  }
  protected static boolean isSupported(int id) {
    return attributes.containsKey(id);
  }
  
  protected static void checkSupported(int id) {
    if(!isSupported(id)) {
      throw new UnsupportedOperationException("NFS Attribute " + id);
    }    
  }
  
  static Attribute parse(RPCBuffer buffer, int id) {
    checkSupported(id);
    try {
      Attribute attribute = attributes.get(id).clazz.newInstance();
      attribute.read(buffer);
      return attribute;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
  public static void writeAttrs(RPCBuffer buffer, Bitmap attrs, ImmutableList<Attribute> attrValues) {
    attrs.write(buffer);
    if(!attrs.isEmpty()) {
      int offset = buffer.position();
      buffer.writeUint32(Integer.MAX_VALUE); // save space
      for(Attribute attr : attrValues) {
        attr.write(buffer);
      }
      buffer.putInt(offset, buffer.position() - offset - 4); // current - start offset - 4 for length
    }
  }
  public static Pair<Bitmap, ImmutableList<Attribute>> readAttrs(RPCBuffer buffer) {
    Bitmap attrs = new Bitmap();
    attrs.read(buffer);
    List<Attribute> attrValues = Lists.newArrayList();
    if(!attrs.isEmpty()) {
      buffer.skip(4); // XXX skip the count cause we don't use this?
      int size = attrs.size();
      for (int bitIndex = 0; bitIndex < size; bitIndex++) {
        if(attrs.isSet(bitIndex)) {
          attrValues.add(parse(buffer, bitIndex));
        }
      }
    }
    return new Pair<Bitmap, ImmutableList<Attribute>>(attrs, ImmutableList.copyOf(attrValues));
  }
  
  public static Pair<Bitmap, ImmutableList<Attribute>> getAttrs(NFS4Handler server, Session session,
      Bitmap requestedAttrs, FileSystem fs, FileStatus fileStatus) throws NFS4Exception, IOException {
    Bitmap responseAttrs = new Bitmap();
    List<Attribute> attrValues = Lists.newArrayList();
    int size = requestedAttrs.size();
    for (int bitIndex = 0; bitIndex < size; bitIndex++) {
      if(requestedAttrs.isSet(bitIndex)) {
        if(isSupported(bitIndex)) {
          responseAttrs.set(bitIndex);
          AttributeHandler<Attribute> handler = getHandler(bitIndex);
          attrValues.add(handler.get(server, session, fs, fileStatus));          
        } else {
          LOGGER.info("getAttr Dropping attribute " + bitIndex);
          server.incrementMetric("GETATTR_DROPPED_ATTRS", 1);
        }
      }
    }
    return new Pair<Bitmap, ImmutableList<Attribute>>(responseAttrs, ImmutableList.copyOf(attrValues));
  }
  
  public static Bitmap setAttrs(NFS4Handler server, Session session,
      Bitmap requestedAttrs, ImmutableMap<Integer, Attribute> attrValues, FileSystem fs, FileStatus fileStatus, StateID stateID) 
          throws NFS4Exception, IOException {
    Bitmap responseAttrs = new Bitmap();
    int size = requestedAttrs.size();
    for (int bitIndex = 0; bitIndex < size; bitIndex++) {
      if(requestedAttrs.isSet(bitIndex)) {
        AttributeHandler<Attribute> handler = getHandler(bitIndex);
        if(handler.set(server, session, fs, fileStatus, stateID, attrValues.get(bitIndex))) {
          responseAttrs.set(bitIndex);          
        }
      }
    }
    return requestedAttrs;
  }
  
  @SuppressWarnings("unchecked")
  public static <T extends Attribute> AttributeHandler<T> getHandler(int id) {
    checkSupported(id);
    return (AttributeHandler<T>) attributes.get(id).handler;
  }
}