package com.cloudera.hadoop.hdfs.nfs.nfs4;

import java.util.Map;

import com.google.common.collect.Maps;

/**
 * Class responsible for keeping a global map of shorthand
 * clientids to Client objects.
 */
public class ClientFactory {
  protected static final Map<Long, Client> mShortHandMap = Maps.newHashMap();
  protected static final Map<OpaqueData, Client> mClientMap = Maps.newHashMap();
  
  public synchronized Client get(OpaqueData clientID) {
    return mClientMap.get(clientID);
  }
  public synchronized Client createIfNotExist(ClientID clientID) {
    if(mClientMap.containsKey(clientID.getOpaqueID())) {
      return null;
    }
    Client client = new Client(clientID);
    mClientMap.put(clientID.getOpaqueID(), client);
    mShortHandMap.put(client.getShorthandID(), client);
    return client;
  }

  public synchronized Client getByShortHand(Long shortHand) {
    return mShortHandMap.get(shortHand);
  }
}
