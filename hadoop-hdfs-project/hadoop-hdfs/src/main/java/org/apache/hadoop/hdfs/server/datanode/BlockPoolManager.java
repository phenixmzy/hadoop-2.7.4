/**
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
package org.apache.hadoop.hdfs.server.datanode;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.security.UserGroupInformation;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * Manages the BPOfferService objects for the data node.
 * Creation, removal, starting, stopping, shutdown on BPOfferService
 * objects must be done via APIs in this class.
 */
/**
 * BlockPoolManager负责管理本地DataNode上所有的BPOfferService实例,对外提供添加,删除,启动,停止,关闭BPOfferService类的接口.
 * 所有对BPOfferService的操作,都必须通过BlockPoolManager类提供的方法来执行.
 *
 * BlockPoolManager对象的初始化是在DataNode#startDataNode(),然后调用BlockPoolManager#refreshNamenodes()方法完成对
 * BlockPoolManager的构造.
 *
 * BLockPoolManager最重要的功能就是维护Datanode上所有BPOfferService对象的引用,同时对外提供多种检索BlockOfferService的方式.
 * bpByNameserviceId 与 bpByBlockPoolId 两个属性为最重要的两个属性.
 * */
@InterfaceAudience.Private
class BlockPoolManager {
  private static final Log LOG = DataNode.LOG;
  
  private final Map<String, BPOfferService> bpByNameserviceId =
    Maps.newHashMap(); // 命名空间id与BPOfferService的映射
  private final Map<String, BPOfferService> bpByBlockPoolId =
    Maps.newHashMap(); // 块池id与BPOfferService的映射
  private final List<BPOfferService> offerServices =
    Lists.newArrayList(); // 保存了所有的BPOfferService对象

  private final DataNode dn;

  //This lock is used only to ensure exclusion of refreshNamenodes
  private final Object refreshNamenodesLock = new Object();
  
  BlockPoolManager(DataNode dn) {
    this.dn = dn;
  }
  
  synchronized void addBlockPool(BPOfferService bpos) {
    Preconditions.checkArgument(offerServices.contains(bpos),
        "Unknown BPOS: %s", bpos);
    if (bpos.getBlockPoolId() == null) {
      throw new IllegalArgumentException("Null blockpool id");
    }
    bpByBlockPoolId.put(bpos.getBlockPoolId(), bpos);
  }
  
  /**
   * Returns the array of BPOfferService objects. 
   * Caution: The BPOfferService returned could be shutdown any time.
   */
  synchronized BPOfferService[] getAllNamenodeThreads() {
    BPOfferService[] bposArray = new BPOfferService[offerServices.size()];
    return offerServices.toArray(bposArray);
  }
      
  synchronized BPOfferService get(String bpid) {
    return bpByBlockPoolId.get(bpid);
  }
  
  synchronized void remove(BPOfferService t) {
    offerServices.remove(t);
    if (t.hasBlockPoolId()) {
      // It's possible that the block pool never successfully registered
      // with any NN, so it was never added it to this map
      bpByBlockPoolId.remove(t.getBlockPoolId());
    }
    
    boolean removed = false;
    for (Iterator<BPOfferService> it = bpByNameserviceId.values().iterator();
         it.hasNext() && !removed;) {
      BPOfferService bpos = it.next();
      if (bpos == t) {
        it.remove();
        LOG.info("Removed " + bpos);
        removed = true;
      }
    }
    
    if (!removed) {
      LOG.warn("Couldn't remove BPOS " + t + " from bpByNameserviceId map");
    }
  }
  
  void shutDownAll(BPOfferService[] bposArray) throws InterruptedException {
    if (bposArray != null) {
      for (BPOfferService bpos : bposArray) {
        bpos.stop(); //interrupts the threads
      }
      //now join
      for (BPOfferService bpos : bposArray) {
        bpos.join();
      }
    }
  }
  
  synchronized void startAll() throws IOException {
    try {
      UserGroupInformation.getLoginUser().doAs(
          new PrivilegedExceptionAction<Object>() {
            @Override
            public Object run() throws Exception {
              for (BPOfferService bpos : offerServices) {
                bpos.start();
              }
              return null;
            }
          });
    } catch (InterruptedException ex) {
      IOException ioe = new IOException();
      ioe.initCause(ex.getCause());
      throw ioe;
    }
  }
  
  void joinAll() {
    for (BPOfferService bpos: this.getAllNamenodeThreads()) {
      bpos.join();
    }
  }
  /**
   * 用于根据HDFS配置添加,删除,以及更新命名空间.在BlockPoolMananger实现中,就是对指定命名空间的BPOfferService引用进行更新.
   * 其分如下几步:
   * 1 构造三个队列,分别是:toRefresh, toAdd, toRemove
   * 2 处理toAdd队列
   * 3 处理toRemove队列
   * 4 处理toRefresh队列
   *
   * 以上所有步骤均 由doRefreshNamenodes方法实现.
   * */
  void refreshNamenodes(Configuration conf)
      throws IOException {
    LOG.info("Refresh request received for nameservices: " + conf.get
            (DFSConfigKeys.DFS_NAMESERVICES));

    Map<String, Map<String, InetSocketAddress>> newAddressMap = DFSUtil
            .getNNServiceRpcAddressesForCluster(conf);

    synchronized (refreshNamenodesLock) {
      doRefreshNamenodes(newAddressMap);
    }
  }
  
  private void doRefreshNamenodes(
      Map<String, Map<String, InetSocketAddress>> addrMap) throws IOException {
    assert Thread.holdsLock(refreshNamenodesLock);

    /**
     * 1 构造三个队列,分别是:toAdd, toRefresh,  toRemove
     * 三个队列分别表示添加,更新,删除BPOfferService. 函数的参数 addrMap变量保存了配置的命名空间列表,bpByNameServiceId变量
     * 则保存了当前BlockPoolMananger中已有的命名空间列表.
     * */
    Set<String> toRefresh = Sets.newLinkedHashSet();
    Set<String> toAdd = Sets.newLinkedHashSet();
    Set<String> toRemove;
    
    synchronized (this) {
      // Step 1. For each of the new nameservices, figure out whether
      // it's an update of the set of NNs for an existing NS,
      // or an entirely new nameservice.
      for (String nameserviceId : addrMap.keySet()) {
        if (bpByNameserviceId.containsKey(nameserviceId)) {
          toRefresh.add(nameserviceId);
        } else {
          toAdd.add(nameserviceId);
        }
      }
      
      // Step 2. Any nameservices we currently have but are no longer present
      // need to be removed.
      // 当前拥有的但不再存在的NameServices,需要removed.
      // difference(bpByNameserviceId.keySet(), addrMap.keySet())
      toRemove = Sets.newHashSet(Sets.difference(
          bpByNameserviceId.keySet(), addrMap.keySet()));
      
      assert toRefresh.size() + toAdd.size() ==
        addrMap.size() :
          "toAdd: " + Joiner.on(",").useForNull("<default>").join(toAdd) +
          "  toRemove: " + Joiner.on(",").useForNull("<default>").join(toRemove) +
          "  toRefresh: " + Joiner.on(",").useForNull("<default>").join(toRefresh);

      
      // Step 3. Start new nameservices
      if (!toAdd.isEmpty()) {
        LOG.info("Starting BPOfferServices for nameservices: " +
            Joiner.on(",").useForNull("<default>").join(toAdd));
      
        for (String nsToAdd : toAdd) {
          ArrayList<InetSocketAddress> addrs =
            Lists.newArrayList(addrMap.get(nsToAdd).values());
          // 调用createBPOS方法创建BPOfferService对象
          BPOfferService bpos = createBPOS(addrs);
          // 把新建的BPOfferService对象加入bpByNameserviceId映射中
          bpByNameserviceId.put(nsToAdd, bpos);
          offerServices.add(bpos);
        }
      }
      // 启动所有的BPOfferService,级联启动BPServiceActor的工作线程
      startAll();
    }

    // Step 4. Shut down old nameservices. This happens outside
    // of the synchronized(this) lock since they need to call
    // back to .remove() from another thread
    if (!toRemove.isEmpty()) {
      LOG.info("Stopping BPOfferServices for nameservices: " +
          Joiner.on(",").useForNull("<default>").join(toRemove));
      
      for (String nsToRemove : toRemove) {
        BPOfferService bpos = bpByNameserviceId.get(nsToRemove);
        bpos.stop();
        bpos.join();
        // they will call remove on their own
      }
    }
    
    // Step 5. Update nameservices whose NN list has changed
    if (!toRefresh.isEmpty()) {
      LOG.info("Refreshing list of NNs for nameservices: " +
          Joiner.on(",").useForNull("<default>").join(toRefresh));
      
      for (String nsToRefresh : toRefresh) {
        BPOfferService bpos = bpByNameserviceId.get(nsToRefresh);
        ArrayList<InetSocketAddress> addrs =
          Lists.newArrayList(addrMap.get(nsToRefresh).values());
        bpos.refreshNNList(addrs);
      }
    }
  }

  /**
   * Extracted out for test purposes.
   */
  protected BPOfferService createBPOS(List<InetSocketAddress> nnAddrs) {
    return new BPOfferService(nnAddrs, dn);
  }
}
