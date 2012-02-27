package com.cloudera.hadoop.hdfs.nfs.nfs4.attrs;

import static org.junit.Assert.*;

import java.util.Set;

import org.junit.Test;

import com.cloudera.hadoop.hdfs.nfs.nfs4.Identifiable;

public class TestAttribute {

  @Test
  public void testIdentifiers() throws Exception {
    Set<Integer> ids = Attribute.attributes.keySet();
    if(ids.isEmpty()) {
      fail("No attributes");
    }
    for(Integer id : ids) {
      Identifiable attribute = Attribute.attributes.get(id).clazz.newInstance();
      if(attribute.getID() != id) {
        fail(attribute.getClass().getName() + " has id " + attribute.getID() + " and not " + id);
      }
    }
  }

  @Test(expected=UnsupportedOperationException.class)
  public void testNotSupported() {
    Attribute.checkSupported(Integer.MIN_VALUE);
  }
}
