package com.cloudera.hadoop.hdfs.nfs.nfs4.state;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.io.Closeable;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.cloudera.hadoop.hdfs.nfs.nfs4.OpaqueData12;
import com.cloudera.hadoop.hdfs.nfs.nfs4.StateID;
import com.google.common.base.Charsets;

public class TestOpenResource {

  private StateID stateID;
  private Closeable closeable;
  private OpenResource<Closeable> res;
  
  @Before
  public void setup() throws Exception {
    stateID = new StateID();
    OpaqueData12 opaque = new OpaqueData12();
    opaque.setData("1".getBytes(Charsets.UTF_8));
    stateID.setData(opaque);
    closeable = mock(Closeable.class);
    res = new OpenResource<Closeable>(stateID, closeable);

  }
  @Test
  public void testGet() throws Exception {
    Assert.assertSame(closeable, res.get());
  }
  @Test
  public void testIsOwnedBy() throws Exception {
    Assert.assertTrue(res.isOwnedBy(stateID));
    Assert.assertFalse(res.isOwnedBy(new StateID()));
  }
  @Test
  public void testClose() throws Exception {
    res.close();
    verify(closeable).close();
  }
  @Test
  public void testSettersGettters() throws Exception {
    Assert.assertFalse(res.isConfirmed());
    res.setConfirmed(true);
    Assert.assertTrue(res.isConfirmed());
    res.setConfirmed(false);
    Assert.assertFalse(res.isConfirmed());

    Assert.assertEquals(stateID, res.getStateID());
    
    int seq = stateID.getSeqID() + 1;
    res.setSequenceID(seq);
    Assert.assertEquals(seq, stateID.getSeqID());
    
    long ts = res.getTimestamp() + 1;
    res.setTimestamp(ts);
    Assert.assertEquals(ts, res.getTimestamp());
  }
}
