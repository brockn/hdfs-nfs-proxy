package com.cloudera.hadoop.hdfs.nfs.nfs4;

import static org.junit.Assert.fail;

import java.util.Set;

import org.junit.Test;

import com.cloudera.hadoop.hdfs.nfs.nfs4.OperationFactory.Holder;

public class TestOperationFactory {

  @Test
  public void testIdentifiers() throws Exception{
    Set<Integer> ids = OperationFactory.operations.keySet();
    if(ids.isEmpty()) {
      throw new RuntimeException("No operations");
    }
    for(Integer id : ids) {
      Holder holder = OperationFactory.operations.get(id);
      Identifiable request = holder.requestClazz.newInstance();
      if(request.getID() != id) {
        fail(request.getClass().getName() + " has id " + request.getID() + " and not " + id);
      }
      Identifiable response = holder.responseClazz.newInstance();
      if(response.getID() != id) {
        fail(response.getClass().getName() + " has id " + response.getID() + " and not " + id);
      }
    }
  }

  @Test(expected=UnsupportedOperationException.class)
  public void testNotSupported() {
    OperationFactory.checkSupported(Integer.MIN_VALUE);
  }
}
