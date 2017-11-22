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

package org.apache.hadoop.ipc;

import static org.apache.hadoop.ipc.RpcConstants.*;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import javax.net.SocketFactory;
import javax.security.sasl.Sasl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryPolicy.RetryAction;
import org.apache.hadoop.ipc.ProtobufRpcEngine.RpcRequestMessageWrapper;
import org.apache.hadoop.ipc.RPC.RpcKind;
import org.apache.hadoop.ipc.Server.AuthProtocol;
import org.apache.hadoop.ipc.protobuf.IpcConnectionContextProtos.IpcConnectionContextProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcRequestHeaderProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcRequestHeaderProto.OperationProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto;
import org.apache.hadoop.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcStatusProto;
import org.apache.hadoop.net.ConnectTimeoutException;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.KerberosInfo;
import org.apache.hadoop.security.SaslRpcClient;
import org.apache.hadoop.security.SaslRpcServer.AuthMethod;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ProtoUtil;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Time;
import org.apache.htrace.Trace;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.CodedOutputStream;

/** A client for an IPC service.  IPC calls take a single {@link Writable} as a
 * parameter, and return a {@link Writable} as their value.  A service runs on
 * a port and is defined by a parameter class and a value class.
 * 
 * @see Server
 */
/**
 *一个IPC服务的客户端.IPC的call方法获得单个Writable类型的参数,处理后返回一个Writable类型的值.服务是运行在一个端口上并以参数类和值类定义.
 *
 *Client主要的内部类
 * 1. Call，表示一次rpc的调用请求
 * 2. Connection，表示一个client与server之间的连接，一个连接一个线程启动
 * 3. ConnectionId：连接的标记（包括server地址，协议，其他一些连接的配置项信息）
 * 4. ClientExecutorServiceFactory：ExecutorService实现的线程池
 *
 * Client，ConnectionId，Connection，Call的关系差不多如下：
 * 1. 一个Client里面维护着一个connections的hashtable，其中ConnectionId由用户组，服务器地址，协议来确定唯一性。
 * private Hashtable<ConnectionId, Connection> connections = new Hashtable<ConnectionId, Connection>();
 * 2. 一个Connection里可能有很多Call
 * private Hashtable<Integer, Call> calls = new Hashtable<Integer, Call>();
 *
 * Client的调用过程
 * 1 一个实际的调用.org.apache.hadoop.ipc.ProtobufRpcEngine#getProxy 方法生成一个代理,其中Invoker是一个实现了InvocationHandler 接口的类
 * 2 代理对象调用相应方法（invoke()）org.apache.hadoop.ipc.ProtobufRpcEngine.Invoker
 * Invocation 用于封装方法名和参数，作为数据传输层。远程调用的主要关键就是Invocation实现了Writable接口,Invocation在write(DataOutput out)函数中将调用的methodName写入到out，将调用方法的参数个数写入out ，同时逐个将参数的className写入out,最后将所有参数逐个写入out,这也就决定了通过RPC实现调用的方法中的参数要么是简单类型，要么是String,要么是实现了Writable接口的类（参数自己知道如何序列化到stream），要么是数组（数组的元素也必须为简单类型,String,实现了Writable接口的类）。
 * Invocation序列化参数的实现是通过如下函数实现的:org.apache.hadoop.io.ObjectWritable.writeObject
 * 3 调用client对象的call方法，向服务器发送请求（参数、方法）call() -> connection.sendRpcRequest(call);
 * 4 获得连接对象,通过connection发送Call对象 getConnection
 * 5 connection的线程等待接受结果.run() -> receiveRpcResponse()
 * 6 返回结果,通知client线程.  setRpcResponse(),callComplete()
 *
 *
 * 异步/同步模型
 * Hadoop的RPC对外的接口其实是同步的，但是，RPC的内部实现其实是异步消息机制。hadoop用线程wait/notify机制实现异步转同步，发送请求（call）之后wait请求处理完毕，接收完响应（connection.receiveResponse()）之后notify，notify()方法在setRpcResponse中。
 * 但现在有一个问题，一个connection有多个call。可能同时有多个call在等待接收消息，那么是当client接收到response后，怎样确认它到底是之前哪个request的response呢？这个就是依靠的connection中的一个HashTable<Integer, Call> calls了，其中的Integer是用来标识Call，是一个call.id，这样就可以将request和response对应上了。
 *
 * */
public class Client {
  
  public static final Log LOG = LogFactory.getLog(Client.class);

  /** A counter for generating call IDs. */
  private static final AtomicInteger callIdCounter = new AtomicInteger();

  private static final ThreadLocal<Integer> callId = new ThreadLocal<Integer>();
  private static final ThreadLocal<Integer> retryCount = new ThreadLocal<Integer>();

  /** Set call id and retry count for the next call. */
  public static void setCallIdAndRetryCount(int cid, int rc) {
    Preconditions.checkArgument(cid != RpcConstants.INVALID_CALL_ID);
    Preconditions.checkState(callId.get() == null);
    Preconditions.checkArgument(rc != RpcConstants.INVALID_RETRY_COUNT);

    callId.set(cid);
    retryCount.set(rc);
  }
  //一个Client里面维护着一个connections的Hashtable，其中ConnectionId由用户组，服务器地址，协议来确定唯一性。
  private Hashtable<ConnectionId, Connection> connections =
    new Hashtable<ConnectionId, Connection>();

  private Class<? extends Writable> valueClass;   // class of call values
  private AtomicBoolean running = new AtomicBoolean(true); // if client runs
  final private Configuration conf;

  private SocketFactory socketFactory;           // how to create sockets
  private int refCount = 1;

  private final int connectionTimeout;

  private final boolean fallbackAllowed;
  private final byte[] clientId;
  
  final static int CONNECTION_CONTEXT_CALL_ID = -3;
  
  /**
   * Executor on which IPC calls' parameters are sent.
   * Deferring the sending of parameters to a separate
   * thread isolates them from thread interruptions in the
   * calling code.
   */
  private final ExecutorService sendParamsExecutor;
  private final static ClientExecutorServiceFactory clientExcecutorFactory =
      new ClientExecutorServiceFactory();

  /**
   * 一个单例,获取和释放ExecutorService 线程池的方法都是粗粒度锁,并通过计数器executorRefCount   是否创建一个线程池或真正关闭线程池.如果获取和释放的计数不同步，会引起内存泄漏.同时通过计数器可以知道线程池是否有执行关闭操作.
   * */
  private static class ClientExecutorServiceFactory {
    private int executorRefCount = 0;
    private ExecutorService clientExecutor = null;
    
    /**
     * Get Executor on which IPC calls' parameters are sent.
     * If the internal reference counter is zero, this method
     * creates the instance of Executor. If not, this method
     * just returns the reference of clientExecutor.
     * 
     * @return An ExecutorService instance
     */
    synchronized ExecutorService refAndGetInstance() {
      if (executorRefCount == 0) {
        clientExecutor = Executors.newCachedThreadPool(
            new ThreadFactoryBuilder()
            .setDaemon(true)
            .setNameFormat("IPC Parameter Sending Thread #%d")
            .build());
      }
      executorRefCount++;
      
      return clientExecutor;
    }
    
    /**
     * Cleanup Executor on which IPC calls' parameters are sent.
     * If reference counter is zero, this method discards the
     * instance of the Executor. If not, this method
     * just decrements the internal reference counter.
     * 
     * @return An ExecutorService instance if it exists.
     *   Null is returned if not.
     */
    synchronized ExecutorService unrefAndCleanup() {
      executorRefCount--;
      assert(executorRefCount >= 0);
      
      if (executorRefCount == 0) {
        clientExecutor.shutdown();
        try {
          if (!clientExecutor.awaitTermination(1, TimeUnit.MINUTES)) {
            clientExecutor.shutdownNow();
          }
        } catch (InterruptedException e) {
          LOG.warn("Interrupted while waiting for clientExecutor" +
              " to stop");
          clientExecutor.shutdownNow();
          Thread.currentThread().interrupt();
        }
        clientExecutor = null;
      }
      
      return clientExecutor;
    }
  };
  
  /**
   * set the ping interval value in configuration
   * 
   * @param conf Configuration
   * @param pingInterval the ping interval
   */
  public static final void setPingInterval(Configuration conf,
      int pingInterval) {
    conf.setInt(CommonConfigurationKeys.IPC_PING_INTERVAL_KEY, pingInterval);
  }

  /**
   * Get the ping interval from configuration;
   * If not set in the configuration, return the default value.
   * 
   * @param conf Configuration
   * @return the ping interval
   */
  public static final int getPingInterval(Configuration conf) {
    return conf.getInt(CommonConfigurationKeys.IPC_PING_INTERVAL_KEY,
        CommonConfigurationKeys.IPC_PING_INTERVAL_DEFAULT);
  }

  /**
   * The time after which a RPC will timeout.
   * If ping is not enabled (via ipc.client.ping), then the timeout value is the 
   * same as the pingInterval.
   * If ping is enabled, then there is no timeout value.
   * 
   * @param conf Configuration
   * @return the timeout period in milliseconds. -1 if no timeout value is set
   */
  final public static int getTimeout(Configuration conf) {
    if (!conf.getBoolean(CommonConfigurationKeys.IPC_CLIENT_PING_KEY,
        CommonConfigurationKeys.IPC_CLIENT_PING_DEFAULT)) {
      return getPingInterval(conf);
    }
    return -1;
  }
  /**
   * set the connection timeout value in configuration
   * 
   * @param conf Configuration
   * @param timeout the socket connect timeout value
   */
  public static final void setConnectTimeout(Configuration conf, int timeout) {
    conf.setInt(CommonConfigurationKeys.IPC_CLIENT_CONNECT_TIMEOUT_KEY, timeout);
  }

  @VisibleForTesting
  public static final ExecutorService getClientExecutor() {
    return Client.clientExcecutorFactory.clientExecutor;
  }
  /**
   * Increment this client's reference count
   *
   */
  synchronized void incCount() {
    refCount++;
  }
  
  /**
   * Decrement this client's reference count
   *
   */
  synchronized void decCount() {
    refCount--;
  }
  
  /**
   * Return if this client has no reference
   * 
   * @return true if this client has no reference; false otherwise
   */
  synchronized boolean isZeroReference() {
    return refCount==0;
  }

  /** Check the rpc response header. */
  void checkResponse(RpcResponseHeaderProto header) throws IOException {
    if (header == null) {
      throw new EOFException("Response is null.");
    }
    if (header.hasClientId()) {
      // check client IDs
      final byte[] id = header.getClientId().toByteArray();
      if (!Arrays.equals(id, RpcConstants.DUMMY_CLIENT_ID)) {
        if (!Arrays.equals(id, clientId)) {
          throw new IOException("Client IDs not matched: local ID="
              + StringUtils.byteToHexString(clientId) + ", ID in response="
              + StringUtils.byteToHexString(header.getClientId().toByteArray()));
        }
      }
    }
  }

  Call createCall(RPC.RpcKind rpcKind, Writable rpcRequest) {
    return new Call(rpcKind, rpcRequest);
  }

  /** 
   * Class that represents an RPC call
   * 属性:
   * final int id; call id
   * final int retry; call重试次数　
   * final Writable rpcRequest;
   * Writable rpcResponse; 序列化的返回响应，如果有错误则是null，即Nullwritable
   * IOException error; 处理中的异常
   * final RPC.RpcKind rpcKind; rpc引擎采用的种类，主要有writable引擎方式，和protocolbuffer引擎方式，两种的序列化和rpc消息处理各不相同，writable是Hadoop创建之初自带的一种处理方式，protocolbuffer是google公司所采用的一种方式，目前Hadoop默认的采用方式是protocolbuffer方式，主要是平台化和速度上都要胜于writalble。
   * boolean done; true表示call已完成，判断call完成与否的依据

   */
  static class Call {
    final int id;               // call id
    final int retry;           // retry count
    final Writable rpcRequest;  // the serialized rpc request
    Writable rpcResponse;       // null if rpc has error
    IOException error;          // exception, null if success
    final RPC.RpcKind rpcKind;      // Rpc EngineKind
    boolean done;               // true when call is done

    private Call(RPC.RpcKind rpcKind, Writable param) {
      this.rpcKind = rpcKind;
      this.rpcRequest = param;

      final Integer id = callId.get();
      if (id == null) {
        this.id = nextCallId();
      } else {
        callId.set(null);
        this.id = id;
      }
      
      final Integer rc = retryCount.get();
      if (rc == null) {
        this.retry = 0;
      } else {
        this.retry = rc;
      }
    }

    /** Indicate when the call is complete and the
     * value or error are available.  Notifies by default.  */
    /**
     * 当返回结果,通知client线程
     * */
    protected synchronized void callComplete() {
      this.done = true;
      notify();                                 // notify caller
    }

    /** Set the exception when there is an error.
     * Notify the caller the call is done.
     * 
     * @param error exception thrown by the call; either local or remote
     */
    public synchronized void setException(IOException error) {
      this.error = error;
      callComplete();
    }
    
    /** Set the return value when there is no error. 
     * Notify the caller the call is done.
     * 
     * @param rpcResponse return value of the rpc call.
     */
    /**
     * 返回结果,通知client线程
     * */
    public synchronized void setRpcResponse(Writable rpcResponse) {
      this.rpcResponse = rpcResponse;
      callComplete();
    }
    
    public synchronized Writable getRpcResponse() {
      return rpcResponse;
    }
  }

  /** Thread that reads responses and notifies callers.  Each connection owns a
   * socket connected to a remote address.  Calls are multiplexed through this
   * socket: responses may be delivered out of order. */
  /**
   * Connection是一个线程类,用于读取response和通知call.
   * 每一个Connection都有其所属的连接到远端地址的socket.Call通过这个socket可以多路复用,但response可能不会按顺序返回.
   * 同一个Connection会被多个Call公用,其以Hashtable<Integer, Call>中为key为Call.id进行区别.
   * */
  private class Connection extends Thread {
    private InetSocketAddress server;             // server ip:port
    private final ConnectionId remoteId;                // connection id
    private AuthMethod authMethod; // authentication method
    private AuthProtocol authProtocol;
    private int serviceClass;
    private SaslRpcClient saslRpcClient;
    
    private Socket socket = null;                 // connected socket
    private DataInputStream in;
    private DataOutputStream out;
    private int rpcTimeout;
    private int maxIdleTime; //connections will be culled if it was idle for 
    //maxIdleTime msecs
    private final RetryPolicy connectionRetryPolicy;
    private final int maxRetriesOnSasl;
    private int maxRetriesOnSocketTimeouts;
    private boolean tcpNoDelay; // if T then disable Nagle's Algorithm
    private boolean doPing; //do we need to send ping message
    private int pingInterval; // how often sends ping to the server in msecs
    private ByteArrayOutputStream pingRequest; // ping message
    
    // currently active calls
    //一个Connection里可能有很多Call
    private Hashtable<Integer, Call> calls = new Hashtable<Integer, Call>();
    private AtomicLong lastActivity = new AtomicLong();// last I/O activity time
    private AtomicBoolean shouldCloseConnection = new AtomicBoolean();  // indicate if the connection is closed
    private IOException closeException; // close reason
    
    private final Object sendRpcRequestLock = new Object();

    public Connection(ConnectionId remoteId, int serviceClass) throws IOException {
      this.remoteId = remoteId;
      this.server = remoteId.getAddress();
      if (server.isUnresolved()) {
        throw NetUtils.wrapException(server.getHostName(),
            server.getPort(),
            null,
            0,
            new UnknownHostException());
      }
      this.rpcTimeout = remoteId.getRpcTimeout();
      this.maxIdleTime = remoteId.getMaxIdleTime();
      this.connectionRetryPolicy = remoteId.connectionRetryPolicy;
      this.maxRetriesOnSasl = remoteId.getMaxRetriesOnSasl();
      this.maxRetriesOnSocketTimeouts = remoteId.getMaxRetriesOnSocketTimeouts();
      this.tcpNoDelay = remoteId.getTcpNoDelay();
      this.doPing = remoteId.getDoPing();
      if (doPing) {
        // construct a RPC header with the callId as the ping callId
        pingRequest = new ByteArrayOutputStream();
        RpcRequestHeaderProto pingHeader = ProtoUtil
            .makeRpcRequestHeader(RpcKind.RPC_PROTOCOL_BUFFER,
                OperationProto.RPC_FINAL_PACKET, PING_CALL_ID,
                RpcConstants.INVALID_RETRY_COUNT, clientId);
        pingHeader.writeDelimitedTo(pingRequest);
      }
      this.pingInterval = remoteId.getPingInterval();
      this.serviceClass = serviceClass;
      if (LOG.isDebugEnabled()) {
        LOG.debug("The ping interval is " + this.pingInterval + " ms.");
      }

      UserGroupInformation ticket = remoteId.getTicket();
      // try SASL if security is enabled or if the ugi contains tokens.
      // this causes a SIMPLE client with tokens to attempt SASL
      boolean trySasl = UserGroupInformation.isSecurityEnabled() ||
                        (ticket != null && !ticket.getTokens().isEmpty());
      this.authProtocol = trySasl ? AuthProtocol.SASL : AuthProtocol.NONE;
      
      this.setName("IPC Client (" + socketFactory.hashCode() +") connection to " +
          server.toString() +
          " from " + ((ticket==null)?"an unknown user":ticket.getUserName()));
      this.setDaemon(true);
    }

    /** Update lastActivity with the current time. */
    private void touch() {
      lastActivity.set(Time.now());
    }

    /**
     * Add a call to this connection's call queue and notify
     * a listener; synchronized.
     * Returns false if called during shutdown.
     * @param call to add
     * @return true if the call was added.
     */
    private synchronized boolean addCall(Call call) {
      //shouldCloseConnection也是connection类的属性，当连接异常，或者客户端要断开连接是，它返回false，说明这个连接正在回收中，不能继续使用
      if (shouldCloseConnection.get())
        return false;
      calls.put(call.id, call);
      notify();
      return true;
    }

    /** This class sends a ping to the remote side when timeout on
     * reading. If no failure is detected, it retries until at least
     * a byte is read.
     */
    private class PingInputStream extends FilterInputStream {
      /* constructor */
      protected PingInputStream(InputStream in) {
        super(in);
      }

      /* Process timeout exception
       * if the connection is not going to be closed or 
       * is not configured to have a RPC timeout, send a ping.
       * (if rpcTimeout is not set to be 0, then RPC should timeout.
       * otherwise, throw the timeout exception.
       */
      private void handleTimeout(SocketTimeoutException e) throws IOException {
        if (shouldCloseConnection.get() || !running.get() || rpcTimeout > 0) {
          throw e;
        } else {
          sendPing();
        }
      }
      
      /** Read a byte from the stream.
       * Send a ping if timeout on read. Retries if no failure is detected
       * until a byte is read.
       * @throws IOException for any IO problem other than socket timeout
       */
      @Override
      public int read() throws IOException {
        do {
          try {
            return super.read();
          } catch (SocketTimeoutException e) {
            handleTimeout(e);
          }
        } while (true);
      }

      /** Read bytes into a buffer starting from offset <code>off</code>
       * Send a ping if timeout on read. Retries if no failure is detected
       * until a byte is read.
       * 
       * @return the total number of bytes read; -1 if the connection is closed.
       */
      @Override
      public int read(byte[] buf, int off, int len) throws IOException {
        do {
          try {
            return super.read(buf, off, len);
          } catch (SocketTimeoutException e) {
            handleTimeout(e);
          }
        } while (true);
      }
    }
    
    private synchronized void disposeSasl() {
      if (saslRpcClient != null) {
        try {
          saslRpcClient.dispose();
          saslRpcClient = null;
        } catch (IOException ignored) {
        }
      }
    }
    
    private synchronized boolean shouldAuthenticateOverKrb() throws IOException {
      UserGroupInformation loginUser = UserGroupInformation.getLoginUser();
      UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
      UserGroupInformation realUser = currentUser.getRealUser();
      if (authMethod == AuthMethod.KERBEROS && loginUser != null &&
      // Make sure user logged in using Kerberos either keytab or TGT
          loginUser.hasKerberosCredentials() &&
          // relogin only in case it is the login user (e.g. JT)
          // or superuser (like oozie).
          (loginUser.equals(currentUser) || loginUser.equals(realUser))) {
        return true;
      }
      return false;
    }
    
    private synchronized AuthMethod setupSaslConnection(final InputStream in2, 
        final OutputStream out2) throws IOException {
      // Do not use Client.conf here! We must use ConnectionId.conf, since the
      // Client object is cached and shared between all RPC clients, even those
      // for separate services.
      saslRpcClient = new SaslRpcClient(remoteId.getTicket(),
          remoteId.getProtocol(), remoteId.getAddress(), remoteId.conf);
      return saslRpcClient.saslConnect(in2, out2);
    }

    /**
     * Update the server address if the address corresponding to the host
     * name has changed.
     *
     * @return true if an addr change was detected.
     * @throws IOException when the hostname cannot be resolved.
     */
    private synchronized boolean updateAddress() throws IOException {
      // Do a fresh lookup with the old host name.
      InetSocketAddress currentAddr = NetUtils.createSocketAddrForHost(
                               server.getHostName(), server.getPort());

      if (!server.equals(currentAddr)) {
        LOG.warn("Address change detected. Old: " + server.toString() +
                                 " New: " + currentAddr.toString());
        server = currentAddr;
        return true;
      }
      return false;
    }

    /**
     * 创建socket连接,绑定地址,以及相应的Kerberos安全认证
     * */
    private synchronized void setupConnection() throws IOException {
      short ioFailures = 0;
      short timeoutFailures = 0;
      while (true) {
        try {
          this.socket = socketFactory.createSocket();
          this.socket.setTcpNoDelay(tcpNoDelay);
          this.socket.setKeepAlive(true);
          
          /*
           * Bind the socket to the host specified in the principal name of the
           * client, to ensure Server matching address of the client connection
           * to host name in principal passed.
           */
          UserGroupInformation ticket = remoteId.getTicket();
          if (ticket != null && ticket.hasKerberosCredentials()) {
            KerberosInfo krbInfo = 
              remoteId.getProtocol().getAnnotation(KerberosInfo.class);
            if (krbInfo != null && krbInfo.clientPrincipal() != null) {
              String host = 
                SecurityUtil.getHostFromPrincipal(remoteId.getTicket().getUserName());
              
              // If host name is a valid local address then bind socket to it
              InetAddress localAddr = NetUtils.getLocalInetAddress(host);
              if (localAddr != null) {
                this.socket.bind(new InetSocketAddress(localAddr, 0));
              }
            }
          }
          
          NetUtils.connect(this.socket, server, connectionTimeout);
          if (rpcTimeout > 0) {
            pingInterval = rpcTimeout;  // rpcTimeout overwrites pingInterval
          }
          this.socket.setSoTimeout(pingInterval);
          return;
        } catch (ConnectTimeoutException toe) {
          /* Check for an address change and update the local reference.
           * Reset the failure counter if the address was changed
           */
          if (updateAddress()) {
            timeoutFailures = ioFailures = 0;
          }
          handleConnectionTimeout(timeoutFailures++,
              maxRetriesOnSocketTimeouts, toe);
        } catch (IOException ie) {
          if (updateAddress()) {
            timeoutFailures = ioFailures = 0;
          }
          handleConnectionFailure(ioFailures++, ie);
        }
      }
    }

    /**
     * If multiple clients with the same principal try to connect to the same
     * server at the same time, the server assumes a replay attack is in
     * progress. This is a feature of kerberos. In order to work around this,
     * what is done is that the client backs off randomly and tries to initiate
     * the connection again. The other problem is to do with ticket expiry. To
     * handle that, a relogin is attempted.
     */
    /**
     * 如果多个client用相同的票据在同一时间尝试连接同一个server,那么server会认定为重复攻击.
     * 为了解决这个问题,client随机回退,重新初始化连接.其他问题是票据过期,以尝试重新登陆为处理.
     * 其会断开socket连接，重新登陆Kerberos安全认证
     * */
    private synchronized void handleSaslConnectionFailure(
        final int currRetries, final int maxRetries, final Exception ex,
        final Random rand, final UserGroupInformation ugi) throws IOException,
        InterruptedException {
      ugi.doAs(new PrivilegedExceptionAction<Object>() {
        @Override
        public Object run() throws IOException, InterruptedException {
          final short MAX_BACKOFF = 5000;
          closeConnection();
          disposeSasl();
          if (shouldAuthenticateOverKrb()) {
            if (currRetries < maxRetries) {
              if(LOG.isDebugEnabled()) {
                LOG.debug("Exception encountered while connecting to "
                    + "the server : " + ex);
              }
              // try re-login
              if (UserGroupInformation.isLoginKeytabBased()) {
                UserGroupInformation.getLoginUser().reloginFromKeytab();
              } else if (UserGroupInformation.isLoginTicketBased()) {
                UserGroupInformation.getLoginUser().reloginFromTicketCache();
              }
              // have granularity of milliseconds
              //we are sleeping with the Connection lock held but since this
              //connection instance is being used for connecting to the server
              //in question, it is okay
              Thread.sleep((rand.nextInt(MAX_BACKOFF) + 1));
              return null;
            } else {
              String msg = "Couldn't setup connection for "
                  + UserGroupInformation.getLoginUser().getUserName() + " to "
                  + remoteId;
              LOG.warn(msg, ex);
              throw (IOException) new IOException(msg).initCause(ex);
            }
          } else {
            LOG.warn("Exception encountered while connecting to "
                + "the server : " + ex);
          }
          if (ex instanceof RemoteException)
            throw (RemoteException) ex;
          throw new IOException(ex);
        }
      });
    }

    
    /** Connect to the server and set up the I/O streams. It then sends
     * a header to the server and starts
     * the connection thread that waits for responses.
     */
    /**
     * getConnection方法只是初始化了connection对象，并将要发送的请求call对象放入连接connection中，其实还并没有与客户端进行通信。
     * 开始通信的方法是setupIOstreams方法，此方法不仅建立与服务端通信的输入输出对象，还进行消息头的发送，判断能否与服务端进行连接，由于Hadoop有很多个版本，而且并不是每个版本之间都能进行完美通信的。
     * 所以不同版本是不能通信的，消息头就是负责这个任务的，消息头中也附带了通信的协议，
     * 说明到底是谁和谁之间通信（是client和namenode还是datanode和namenode，还是yarn中的resourceManage 和nodemanage等等)
     * */
    private synchronized void setupIOstreams(
        AtomicBoolean fallbackToSimpleAuth) {
      //在socket不为空的情况下就不用再初始化下面的内容了，这说明了，目前正在重用已有的connection，而shouldCloseConnection为true则表示当前的连接正要关闭状态，不可用因此下面的初始化也没有意义，要获取一个新的连接才可以
      if (socket != null || shouldCloseConnection.get()) {
        return;
      } 
      try {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Connecting to "+server);
        }
        if (Trace.isTracing()) {
          Trace.addTimelineAnnotation("IPC client connecting to " + server);
        }
        short numRetries = 0;
        Random rand = null;
        while (true) {
          setupConnection();//connection一些初始化信息，建立socket，初始socket等等操作
          InputStream inStream = NetUtils.getInputStream(socket);
          OutputStream outStream = NetUtils.getOutputStream(socket);
          writeConnectionHeader(outStream);//向服务端写消息头信息
          if (authProtocol == AuthProtocol.SASL) {
            final InputStream in2 = inStream;
            final OutputStream out2 = outStream;
            UserGroupInformation ticket = remoteId.getTicket();
            if (ticket.getRealUser() != null) {
              ticket = ticket.getRealUser();
            }
            try {
              authMethod = ticket
                  .doAs(new PrivilegedExceptionAction<AuthMethod>() {
                    @Override
                    public AuthMethod run()
                        throws IOException, InterruptedException {
                      return setupSaslConnection(in2, out2);
                    }
                  });
            } catch (Exception ex) {
              authMethod = saslRpcClient.getAuthMethod();
              if (rand == null) {
                rand = new Random();
              }
              handleSaslConnectionFailure(numRetries++, maxRetriesOnSasl, ex,
                  rand, ticket);
              continue;
            }
            if (authMethod != AuthMethod.SIMPLE) {
              // Sasl connect is successful. Let's set up Sasl i/o streams.
              inStream = saslRpcClient.getInputStream(inStream);
              outStream = saslRpcClient.getOutputStream(outStream);
              // for testing
              remoteId.saslQop =
                  (String)saslRpcClient.getNegotiatedProperty(Sasl.QOP);
              LOG.debug("Negotiated QOP is :" + remoteId.saslQop);
              if (fallbackToSimpleAuth != null) {
                fallbackToSimpleAuth.set(false);
              }
            } else if (UserGroupInformation.isSecurityEnabled()) {
              if (!fallbackAllowed) {
                throw new IOException("Server asks us to fall back to SIMPLE " +
                    "auth, but this client is configured to only allow secure " +
                    "connections.");
              }
              if (fallbackToSimpleAuth != null) {
                fallbackToSimpleAuth.set(true);
              }
            }
          }
        
          if (doPing) {
            inStream = new PingInputStream(inStream);
          }
          this.in = new DataInputStream(new BufferedInputStream(inStream));

          // SASL may have already buffered the stream
          if (!(outStream instanceof BufferedOutputStream)) {
            outStream = new BufferedOutputStream(outStream);
          }
          this.out = new DataOutputStream(outStream);
          
          writeConnectionContext(remoteId, authMethod);

          // update last activity time
          touch();//connection连接有一定的超时限制，touch方法进行时间更新将连接最新时间更新到现在。

          if (Trace.isTracing()) {
            Trace.addTimelineAnnotation("IPC client connected to " + server);
          }

          // start the receiver thread after the socket connection has been set
          // up
          start();
          return;
        }
      } catch (Throwable t) {
        if (t instanceof IOException) {
          markClosed((IOException)t);
        } else {
          markClosed(new IOException("Couldn't set up IO streams", t));
        }
        close(); //如果出现错误就关闭连接，
      }
    }
    
    private void closeConnection() {
      if (socket == null) {
        return;
      }
      // close the current connection
      try {
        socket.close();
      } catch (IOException e) {
        LOG.warn("Not able to close a socket", e);
      }
      // set socket to null so that the next call to setupIOstreams
      // can start the process of connect all over again.
      socket = null;
    }

    /* Handle connection failures due to timeout on connect
     *
     * If the current number of retries is equal to the max number of retries,
     * stop retrying and throw the exception; Otherwise backoff 1 second and
     * try connecting again.
     *
     * This Method is only called from inside setupIOstreams(), which is
     * synchronized. Hence the sleep is synchronized; the locks will be retained.
     *
     * @param curRetries current number of retries
     * @param maxRetries max number of retries allowed
     * @param ioe failure reason
     * @throws IOException if max number of retries is reached
     */
    /**
     * 处理连接超时.当连接次数大于等于最大次数,停止re-connect并抛出异常.它会被setupIOstreams 方法调用.
     * */
    private void handleConnectionTimeout(
        int curRetries, int maxRetries, IOException ioe) throws IOException {

      closeConnection();

      // throw the exception if the maximum number of retries is reached
      if (curRetries >= maxRetries) {
        throw ioe;
      }
      LOG.info("Retrying connect to server: " + server + ". Already tried "
          + curRetries + " time(s); maxRetries=" + maxRetries);
    }

    private void handleConnectionFailure(int curRetries, IOException ioe
        ) throws IOException {
      closeConnection();

      final RetryAction action;
      try {
        action = connectionRetryPolicy.shouldRetry(ioe, curRetries, 0, true);
      } catch(Exception e) {
        throw e instanceof IOException? (IOException)e: new IOException(e);
      }
      if (action.action == RetryAction.RetryDecision.FAIL) {
        if (action.reason != null) {
          LOG.warn("Failed to connect to server: " + server + ": "
              + action.reason, ioe);
        }
        throw ioe;
      }

      // Throw the exception if the thread is interrupted
      if (Thread.currentThread().isInterrupted()) {
        LOG.warn("Interrupted while trying for connection");
        throw ioe;
      }

      try {
        Thread.sleep(action.delayMillis);
      } catch (InterruptedException e) {
        throw (IOException)new InterruptedIOException("Interrupted: action="
            + action + ", retry policy=" + connectionRetryPolicy).initCause(e);
      }
      LOG.info("Retrying connect to server: " + server + ". Already tried "
          + curRetries + " time(s); retry policy is " + connectionRetryPolicy);
    }

    /**
     * Write the connection header - this is sent when connection is established
     * +----------------------------------+
     * |  "hrpc" 4 bytes                  |      
     * +----------------------------------+
     * |  Version (1 byte)                |
     * +----------------------------------+
     * |  Service Class (1 byte)          |
     * +----------------------------------+
     * |  AuthProtocol (1 byte)           |      
     * +----------------------------------+
     */
    private void writeConnectionHeader(OutputStream outStream)
        throws IOException {
      DataOutputStream out = new DataOutputStream(new BufferedOutputStream(outStream));
      // Write out the header, version and authentication method
      out.write(RpcConstants.HEADER.array());
      out.write(RpcConstants.CURRENT_VERSION);
      out.write(serviceClass);
      out.write(authProtocol.callId);
      out.flush();
    }
    
    /* Write the connection context header for each connection
     * Out is not synchronized because only the first thread does this.
     */
    /**
     * 向服务端写连接上下文.传完头信息就要继续传连接上下文，上下文信息主要是确定当前连接来自于那个客户端，正在处理的是当前客户端的那个call调用，等等信息以确保服务端能够准确的将应答消息发送给正确的客户端
     * */
    private void writeConnectionContext(ConnectionId remoteId,
                                        AuthMethod authMethod)
                                            throws IOException {
      // Write out the ConnectionHeader
      // 建立上下文，依据协议名称，connectionId所属用户组
      IpcConnectionContextProto message = ProtoUtil.makeIpcConnectionContext(
          RPC.getProtocolName(remoteId.getProtocol()),
          remoteId.getTicket(),
          authMethod);
      /**
       * 建立上下文头信息，包括
       * RpcKind.RPC_PROTOCOL_BUFFER说明消息采用的序列化方式，
       * CONNECTION_CONTEXT_CALL_ID应用的那个call，这里采用一个特殊的callId，
       * CONNECTION_CONTEXT_CALL_ID=-3，表示是一个上下文信息，没有请求需要处理，
       * RpcConstants.INVALID_RETRY_COUNT表示call的重试次数，远程调用肯定会出现调用失败，而失败可能是网络等问题，所以重试几次以确保最终能够获得返回结果，这里的RpcConstants.INVALID_RETRY_COUNT=-1，并不需要重试，因为没有请求需要处理，
       * clientId当前发出请求的客户端
       * */
      RpcRequestHeaderProto connectionContextHeader = ProtoUtil
          .makeRpcRequestHeader(RpcKind.RPC_PROTOCOL_BUFFER,
              OperationProto.RPC_FINAL_PACKET, CONNECTION_CONTEXT_CALL_ID,
              RpcConstants.INVALID_RETRY_COUNT, clientId);
      RpcRequestMessageWrapper request =
          new RpcRequestMessageWrapper(connectionContextHeader, message);
      
      // Write out the packet length
      /**写出消息到服务端，先写消息长度，然后是内容，这是固定的方式*/
      out.writeInt(request.getLength());
      request.write(out);
    }
    
    /* wait till someone signals us to start reading RPC response or
     * it is idle too long, it is marked as to be closed, 
     * or the client is marked as not running.
     * 
     * Return true if it is time to read a response; false otherwise.
     */
    private synchronized boolean waitForWork() {
      if (calls.isEmpty() && !shouldCloseConnection.get()  && running.get())  {
        long timeout = maxIdleTime-
              (Time.now()-lastActivity.get());
        if (timeout>0) {
          try {
            wait(timeout);
          } catch (InterruptedException e) {}
        }
      }
      
      if (!calls.isEmpty() && !shouldCloseConnection.get() && running.get()) {
        return true;
      } else if (shouldCloseConnection.get()) {
        return false;
      } else if (calls.isEmpty()) { // idle connection closed or stopped
        markClosed(null);
        return false;
      } else { // get stopped but there are still pending requests 
        markClosed((IOException)new IOException().initCause(
            new InterruptedException()));
        return false;
      }
    }

    public InetSocketAddress getRemoteAddress() {
      return server;
    }

    /* Send a ping to the server if the time elapsed 
     * since last I/O activity is equal to or greater than the ping interval
     */
    private synchronized void sendPing() throws IOException {
      long curTime = Time.now();
      if ( curTime - lastActivity.get() >= pingInterval) {
        lastActivity.set(curTime);
        synchronized (out) {
          out.writeInt(pingRequest.size());
          pingRequest.writeTo(out);
          out.flush();
        }
      }
    }

    @Override
    public void run() {
      if (LOG.isDebugEnabled())
        LOG.debug(getName() + ": starting, having connections " 
            + connections.size());

      try {
        while (waitForWork()) {//wait here for work - read or close connection
          receiveRpcResponse();
        }
      } catch (Throwable t) {
        // This truly is unexpected, since we catch IOException in receiveResponse
        // -- this is only to be really sure that we don't leave a client hanging
        // forever.
        LOG.warn("Unexpected error reading responses on connection " + this, t);
        markClosed(new IOException("Error reading responses", t));
      }
      
      close();
      
      if (LOG.isDebugEnabled())
        LOG.debug(getName() + ": stopped, remaining connections "
            + connections.size());
    }

    /** Initiates a rpc call by sending the rpc request to the remote server.
     * Note: this is not called from the Connection thread, but by other
     * threads.
     * @param call - the rpc request
     */
    public void sendRpcRequest(final Call call)
        throws InterruptedException, IOException {
      if (shouldCloseConnection.get()) {
        return;
      }

      // Serialize the call to be sent. This is done from the actual
      // caller thread, rather than the sendParamsExecutor thread,
      
      // so that if the serialization throws an error, it is reported
      // properly. This also parallelizes the serialization.
      //
      // Format of a call on the wire:
      // 0) Length of rest below (1 + 2)
      // 1) RpcRequestHeader  - is serialized Delimited hence contains length
      // 2) RpcRequest
      //
      // Items '1' and '2' are prepared here. 
      final DataOutputBuffer d = new DataOutputBuffer();
      RpcRequestHeaderProto header = ProtoUtil.makeRpcRequestHeader(
          call.rpcKind, OperationProto.RPC_FINAL_PACKET, call.id, call.retry,
          clientId);
      header.writeDelimitedTo(d);
      call.rpcRequest.write(d);

      synchronized (sendRpcRequestLock) {
        Future<?> senderFuture = sendParamsExecutor.submit(new Runnable() {
          @Override
          public void run() {
            try {
              synchronized (Connection.this.out) {
                if (shouldCloseConnection.get()) {
                  return;
                }
                
                if (LOG.isDebugEnabled())
                  LOG.debug(getName() + " sending #" + call.id);
         
                byte[] data = d.getData();
                int totalLength = d.getLength();
                out.writeInt(totalLength); // Total Length
                out.write(data, 0, totalLength);// RpcRequestHeader + RpcRequest
                out.flush();
              }
            } catch (IOException e) {
              // exception at this point would leave the connection in an
              // unrecoverable state (eg half a call left on the wire).
              // So, close the connection, killing any outstanding calls
              markClosed(e);
            } finally {
              //the buffer is just an in-memory buffer, but it is still polite to
              // close early
              IOUtils.closeStream(d);
            }
          }
        });
      
        try {
          senderFuture.get();
        } catch (ExecutionException e) {
          Throwable cause = e.getCause();
          
          // cause should only be a RuntimeException as the Runnable above
          // catches IOException
          if (cause instanceof RuntimeException) {
            throw (RuntimeException) cause;
          } else {
            throw new RuntimeException("unexpected checked exception", cause);
          }
        }
      }
    }

    /* Receive a response.
     * Because only one receiver, so no synchronization on in.
     */
    /**
     * 接收服务端返回的信息
     * */
    private void receiveRpcResponse() {
      if (shouldCloseConnection.get()) {
        return;
      }
      touch();
      
      try {
        int totalLen = in.readInt();
        RpcResponseHeaderProto header = 
            RpcResponseHeaderProto.parseDelimitedFrom(in);
        checkResponse(header);

        int headerLen = header.getSerializedSize();
        headerLen += CodedOutputStream.computeRawVarint32Size(headerLen);

        int callId = header.getCallId();
        if (LOG.isDebugEnabled())
          LOG.debug(getName() + " got value #" + callId);

        Call call = calls.get(callId);
        RpcStatusProto status = header.getStatus();
        if (status == RpcStatusProto.SUCCESS) {
          Writable value = ReflectionUtils.newInstance(valueClass, conf);
          value.readFields(in);                 // read value
          calls.remove(callId);
          call.setRpcResponse(value);
          
          // verify that length was correct
          // only for ProtobufEngine where len can be verified easily
          if (call.getRpcResponse() instanceof ProtobufRpcEngine.RpcWrapper) {
            ProtobufRpcEngine.RpcWrapper resWrapper = 
                (ProtobufRpcEngine.RpcWrapper) call.getRpcResponse();
            if (totalLen != headerLen + resWrapper.getLength()) { 
              throw new RpcClientException(
                  "RPC response length mismatch on rpc success");
            }
          }
        } else { // Rpc Request failed
          // Verify that length was correct
          if (totalLen != headerLen) {
            throw new RpcClientException(
                "RPC response length mismatch on rpc error");
          }
          
          final String exceptionClassName = header.hasExceptionClassName() ?
                header.getExceptionClassName() : 
                  "ServerDidNotSetExceptionClassName";
          final String errorMsg = header.hasErrorMsg() ? 
                header.getErrorMsg() : "ServerDidNotSetErrorMsg" ;
          final RpcErrorCodeProto erCode = 
                    (header.hasErrorDetail() ? header.getErrorDetail() : null);
          if (erCode == null) {
             LOG.warn("Detailed error code not set by server on rpc error");
          }
          RemoteException re = 
              ( (erCode == null) ? 
                  new RemoteException(exceptionClassName, errorMsg) :
              new RemoteException(exceptionClassName, errorMsg, erCode));
          if (status == RpcStatusProto.ERROR) {
            calls.remove(callId);
            call.setException(re);
          } else if (status == RpcStatusProto.FATAL) {
            // Close the connection
            markClosed(re);
          }
        }
      } catch (IOException e) {
        markClosed(e);
      }
    }
    
    private synchronized void markClosed(IOException e) {
      if (shouldCloseConnection.compareAndSet(false, true)) {
        closeException = e;
        notifyAll();
      }
    }
    
    /** Close the connection. */
    private synchronized void close() {
      if (!shouldCloseConnection.get()) {
        LOG.error("The connection is not in the closed state");
        return;
      }

      // release the resources
      // first thing to do;take the connection out of the connection list
      synchronized (connections) {
        if (connections.get(remoteId) == this) {
          connections.remove(remoteId);
        }
      }

      // close the streams and therefore the socket
      IOUtils.closeStream(out);
      IOUtils.closeStream(in);
      disposeSasl();

      // clean up all calls
      if (closeException == null) {
        if (!calls.isEmpty()) {
          LOG.warn(
              "A connection is closed for no cause and calls are not empty");

          // clean up calls anyway
          closeException = new IOException("Unexpected closed connection");
          cleanupCalls();
        }
      } else {
        // log the info
        if (LOG.isDebugEnabled()) {
          LOG.debug("closing ipc connection to " + server + ": " +
              closeException.getMessage(),closeException);
        }

        // cleanup calls
        cleanupCalls();
      }
      closeConnection();
      if (LOG.isDebugEnabled())
        LOG.debug(getName() + ": closed");
    }
    
    /* Cleanup all calls and mark them as done */
    private void cleanupCalls() {
      Iterator<Entry<Integer, Call>> itor = calls.entrySet().iterator() ;
      while (itor.hasNext()) {
        Call c = itor.next().getValue(); 
        itor.remove();
        c.setException(closeException); // local exception
      }
    }
  }

  /** Construct an IPC client whose values are of the given {@link Writable}
   * class. */
  public Client(Class<? extends Writable> valueClass, Configuration conf, 
      SocketFactory factory) {
    this.valueClass = valueClass;
    this.conf = conf;
    this.socketFactory = factory;
    this.connectionTimeout = conf.getInt(CommonConfigurationKeys.IPC_CLIENT_CONNECT_TIMEOUT_KEY,
        CommonConfigurationKeys.IPC_CLIENT_CONNECT_TIMEOUT_DEFAULT);
    this.fallbackAllowed = conf.getBoolean(CommonConfigurationKeys.IPC_CLIENT_FALLBACK_TO_SIMPLE_AUTH_ALLOWED_KEY,
        CommonConfigurationKeys.IPC_CLIENT_FALLBACK_TO_SIMPLE_AUTH_ALLOWED_DEFAULT);
    this.clientId = ClientId.getClientId();
    this.sendParamsExecutor = clientExcecutorFactory.refAndGetInstance();
  }

  /**
   * Construct an IPC client with the default SocketFactory
   * @param valueClass
   * @param conf
   */
  public Client(Class<? extends Writable> valueClass, Configuration conf) {
    this(valueClass, conf, NetUtils.getDefaultSocketFactory(conf));
  }
 
  /** Return the socket factory of this client
   *
   * @return this client's socket factory
   */
  SocketFactory getSocketFactory() {
    return socketFactory;
  }

  /** Stop all threads related to this client.  No further calls may be made
   * using this client. */
  public void stop() {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Stopping client");
    }

    if (!running.compareAndSet(true, false)) {
      return;
    }
    
    // wake up all connections
    synchronized (connections) {
      for (Connection conn : connections.values()) {
        conn.interrupt();
      }
    }
    
    // wait until all connections are closed
    while (!connections.isEmpty()) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
      }
    }
    
    clientExcecutorFactory.unrefAndCleanup();
  }

  /**
   * Same as {@link #call(RPC.RpcKind, Writable, ConnectionId)}
   *  for RPC_BUILTIN
   */
  public Writable call(Writable param, InetSocketAddress address)
      throws IOException {
    return call(RPC.RpcKind.RPC_BUILTIN, param, address);
    
  }
  /** Make a call, passing <code>param</code>, to the IPC server running at
   * <code>address</code>, returning the value.  Throws exceptions if there are
   * network problems or if the remote code threw an exception.
   * @deprecated Use {@link #call(RPC.RpcKind, Writable,
   *  ConnectionId)} instead 
   */
  @Deprecated
  public Writable call(RPC.RpcKind rpcKind, Writable param, InetSocketAddress address)
  throws IOException {
      return call(rpcKind, param, address, null);
  }
  
  /** Make a call, passing <code>param</code>, to the IPC server running at
   * <code>address</code> with the <code>ticket</code> credentials, returning 
   * the value.  
   * Throws exceptions if there are network problems or if the remote code 
   * threw an exception.
   * @deprecated Use {@link #call(RPC.RpcKind, Writable, 
   * ConnectionId)} instead 
   */
  @Deprecated
  public Writable call(RPC.RpcKind rpcKind, Writable param, InetSocketAddress addr, 
      UserGroupInformation ticket) throws IOException {
    ConnectionId remoteId = ConnectionId.getConnectionId(addr, null, ticket, 0,
        conf);
    return call(rpcKind, param, remoteId);
  }
  
  /** Make a call, passing <code>param</code>, to the IPC server running at
   * <code>address</code> which is servicing the <code>protocol</code> protocol, 
   * with the <code>ticket</code> credentials and <code>rpcTimeout</code> as 
   * timeout, returning the value.  
   * Throws exceptions if there are network problems or if the remote code 
   * threw an exception. 
   * @deprecated Use {@link #call(RPC.RpcKind, Writable,
   *  ConnectionId)} instead 
   */
  @Deprecated
  public Writable call(RPC.RpcKind rpcKind, Writable param, InetSocketAddress addr, 
                       Class<?> protocol, UserGroupInformation ticket,
                       int rpcTimeout) throws IOException {
    ConnectionId remoteId = ConnectionId.getConnectionId(addr, protocol,
        ticket, rpcTimeout, conf);
    return call(rpcKind, param, remoteId);
  }

  
  /**
   * Same as {@link #call(RPC.RpcKind, Writable, InetSocketAddress,
   * Class, UserGroupInformation, int, Configuration)}
   * except that rpcKind is writable.
   */
  public Writable call(Writable param, InetSocketAddress addr,
      Class<?> protocol, UserGroupInformation ticket,
      int rpcTimeout, Configuration conf) throws IOException {
    ConnectionId remoteId = ConnectionId.getConnectionId(addr, protocol,
        ticket, rpcTimeout, conf);
    return call(RPC.RpcKind.RPC_BUILTIN, param, remoteId);
  }
  
  /**
   * Same as {@link #call(Writable, InetSocketAddress,
   * Class, UserGroupInformation, int, Configuration)}
   * except that specifying serviceClass.
   */
  public Writable call(Writable param, InetSocketAddress addr,
      Class<?> protocol, UserGroupInformation ticket,
      int rpcTimeout, int serviceClass, Configuration conf)
      throws IOException {
    ConnectionId remoteId = ConnectionId.getConnectionId(addr, protocol,
        ticket, rpcTimeout, conf);
    return call(RPC.RpcKind.RPC_BUILTIN, param, remoteId, serviceClass);
  }

  /**
   * Make a call, passing <code>param</code>, to the IPC server running at
   * <code>address</code> which is servicing the <code>protocol</code> protocol,
   * with the <code>ticket</code> credentials, <code>rpcTimeout</code> as
   * timeout and <code>conf</code> as conf for this connection, returning the
   * value. Throws exceptions if there are network problems or if the remote
   * code threw an exception.
   */
  public Writable call(RPC.RpcKind rpcKind, Writable param, InetSocketAddress addr, 
                       Class<?> protocol, UserGroupInformation ticket,
                       int rpcTimeout, Configuration conf) throws IOException {
    ConnectionId remoteId = ConnectionId.getConnectionId(addr, protocol,
        ticket, rpcTimeout, conf);
    return call(rpcKind, param, remoteId);
  }
  
  /**
   * Same as {link {@link #call(RPC.RpcKind, Writable, ConnectionId)}
   * except the rpcKind is RPC_BUILTIN
   */
  public Writable call(Writable param, ConnectionId remoteId)
      throws IOException {
     return call(RPC.RpcKind.RPC_BUILTIN, param, remoteId);
  }
  
  /**
   * Make a call, passing <code>rpcRequest</code>, to the IPC server defined by
   * <code>remoteId</code>, returning the rpc respond.
   *
   * @param rpcKind
   * @param rpcRequest -  contains serialized method and method parameters
   * @param remoteId - the target rpc server
   * @returns the rpc response
   * Throws exceptions if there are network problems or if the remote code 
   * threw an exception.
   */
  public Writable call(RPC.RpcKind rpcKind, Writable rpcRequest,
      ConnectionId remoteId) throws IOException {
    return call(rpcKind, rpcRequest, remoteId, RPC.RPC_SERVICE_CLASS_DEFAULT);
  }

  /** 
   * Make a call, passing <code>rpcRequest</code>, to the IPC server defined by
   * <code>remoteId</code>, returning the rpc respond.
   *
   * @param rpcKind
   * @param rpcRequest -  contains serialized method and method parameters
   * @param remoteId - the target rpc server
   * @param fallbackToSimpleAuth - set to true or false during this method to
   *   indicate if a secure client falls back to simple auth
   * @returns the rpc response
   * Throws exceptions if there are network problems or if the remote code
   * threw an exception.
   */
  public Writable call(RPC.RpcKind rpcKind, Writable rpcRequest,
      ConnectionId remoteId, AtomicBoolean fallbackToSimpleAuth)
      throws IOException {
    return call(rpcKind, rpcRequest, remoteId, RPC.RPC_SERVICE_CLASS_DEFAULT,
      fallbackToSimpleAuth);
  }

  /**
   * Make a call, passing <code>rpcRequest</code>, to the IPC server defined by
   * <code>remoteId</code>, returning the rpc response.
   * 
   * @param rpcKind
   * @param rpcRequest -  contains serialized method and method parameters
   * @param remoteId - the target rpc server
   * @param serviceClass - service class for RPC
   * @returns the rpc response
   * Throws exceptions if there are network problems or if the remote code 
   * threw an exception.
   */
  public Writable call(RPC.RpcKind rpcKind, Writable rpcRequest,
      ConnectionId remoteId, int serviceClass) throws IOException {
    return call(rpcKind, rpcRequest, remoteId, serviceClass, null);
  }

  /**
   * Make a call, passing <code>rpcRequest</code>, to the IPC server defined by
   * <code>remoteId</code>, returning the rpc response.
   *
   * @param rpcKind
   * @param rpcRequest -  contains serialized method and method parameters
   * @param remoteId - the target rpc server
   * @param serviceClass - service class for RPC
   * @param fallbackToSimpleAuth - set to true or false during this method to
   *   indicate if a secure client falls back to simple auth
   * @returns the rpc response
   * Throws exceptions if there are network problems or if the remote code
   * threw an exception.
   */
  public Writable call(RPC.RpcKind rpcKind, Writable rpcRequest,
      ConnectionId remoteId, int serviceClass,
      AtomicBoolean fallbackToSimpleAuth) throws IOException {
    final Call call = createCall(rpcKind, rpcRequest);
    Connection connection = getConnection(remoteId, call, serviceClass,
      fallbackToSimpleAuth);
    try {
      connection.sendRpcRequest(call);                 // send the rpc request
    } catch (RejectedExecutionException e) {
      throw new IOException("connection has been closed", e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.warn("interrupted waiting to send rpc request to server", e);
      throw new IOException(e);
    }

    synchronized (call) {
      while (!call.done) {
        try {
          call.wait();                           // wait for the result
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          throw new InterruptedIOException("Call interrupted");
        }
      }

      if (call.error != null) {
        if (call.error instanceof RemoteException) {
          call.error.fillInStackTrace();
          throw call.error;
        } else { // local exception
          InetSocketAddress address = connection.getRemoteAddress();
          throw NetUtils.wrapException(address.getHostName(),
                  address.getPort(),
                  NetUtils.getHostname(),
                  0,
                  call.error);
        }
      } else {
        return call.getRpcResponse();
      }
    }
  }

  // for unit testing only
  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  Set<ConnectionId> getConnectionIds() {
    synchronized (connections) {
      return connections.keySet();
    }
  }
  
  /** Get a connection from the pool, or create a new one and add it to the
   * pool.  Connections to a given ConnectionId are reused. */
  /**
   * getConnection方法用来跟服务器建立Connection连接，第一次远程方法调用时建立，然后保存在缓存里重复使用
   * */
  private Connection getConnection(ConnectionId remoteId,
      Call call, int serviceClass, AtomicBoolean fallbackToSimpleAuth)
      throws IOException {
    if (!running.get()) {//running是client的一个属性，表示客户端现在是否向服务端进行请求，如果没有running（running是一个AtomicBollean原子布尔类的对象）就是返回false
      // the client is stopped
      throw new IOException("The client is stopped");
    }
    Connection connection;
    /* we could avoid this allocation for each RPC by having a  
     * connectionsId object and with set() method. We need to manage the
     * refs for keys in HashMap properly. For now its ok.
     */
    do {
      synchronized (connections) {
        //判断是否存在对应的连接,没有则新建
        connection = connections.get(remoteId);
        if (connection == null) {
          connection = new Connection(remoteId, serviceClass);
          connections.put(remoteId, connection);
        }
      }
    } while (!connection.addCall(call));//进行输入输出对象初始化
    
    //we don't invoke the method below inside "synchronized (connections)"
    //block above. The reason for that is if the server happens to be slow,
    //it will take longer to establish a connection and that will slow the
    //entire system down.
    connection.setupIOstreams(fallbackToSimpleAuth);//进行输入输出对象初始化
    return connection;
  }
  
  /**
   * This class holds the address and the user ticket. The client connections
   * to servers are uniquely identified by <remoteAddress, protocol, ticket>
   */
  @InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
  @InterfaceStability.Evolving
  public static class ConnectionId {
    InetSocketAddress address;
    UserGroupInformation ticket;
    final Class<?> protocol;
    private static final int PRIME = 16777619;
    private final int rpcTimeout;
    private final int maxIdleTime; //connections will be culled if it was idle for 
    //maxIdleTime msecs
    private final RetryPolicy connectionRetryPolicy;
    private final int maxRetriesOnSasl;
    // the max. no. of retries for socket connections on time out exceptions
    private final int maxRetriesOnSocketTimeouts;
    private final boolean tcpNoDelay; // if T then disable Nagle's Algorithm
    private final boolean doPing; //do we need to send ping message
    private final int pingInterval; // how often sends ping to the server in msecs
    private String saslQop; // here for testing
    private final Configuration conf; // used to get the expected kerberos principal name
    
    ConnectionId(InetSocketAddress address, Class<?> protocol, 
                 UserGroupInformation ticket, int rpcTimeout,
                 RetryPolicy connectionRetryPolicy, Configuration conf) {
      this.protocol = protocol;
      this.address = address;
      this.ticket = ticket;
      this.rpcTimeout = rpcTimeout;
      this.connectionRetryPolicy = connectionRetryPolicy;

      this.maxIdleTime = conf.getInt(
          CommonConfigurationKeysPublic.IPC_CLIENT_CONNECTION_MAXIDLETIME_KEY,
          CommonConfigurationKeysPublic.IPC_CLIENT_CONNECTION_MAXIDLETIME_DEFAULT);
      this.maxRetriesOnSasl = conf.getInt(
          CommonConfigurationKeys.IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SASL_KEY,
          CommonConfigurationKeys.IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SASL_DEFAULT);
      this.maxRetriesOnSocketTimeouts = conf.getInt(
          CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SOCKET_TIMEOUTS_KEY,
          CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SOCKET_TIMEOUTS_DEFAULT);
      this.tcpNoDelay = conf.getBoolean(
          CommonConfigurationKeysPublic.IPC_CLIENT_TCPNODELAY_KEY,
          CommonConfigurationKeysPublic.IPC_CLIENT_TCPNODELAY_DEFAULT);
      this.doPing = conf.getBoolean(
          CommonConfigurationKeys.IPC_CLIENT_PING_KEY,
          CommonConfigurationKeys.IPC_CLIENT_PING_DEFAULT);
      this.pingInterval = (doPing ? Client.getPingInterval(conf) : 0);
      this.conf = conf;
    }
    
    InetSocketAddress getAddress() {
      return address;
    }
    
    Class<?> getProtocol() {
      return protocol;
    }
    
    UserGroupInformation getTicket() {
      return ticket;
    }
    
    private int getRpcTimeout() {
      return rpcTimeout;
    }
    
    int getMaxIdleTime() {
      return maxIdleTime;
    }
    
    public int getMaxRetriesOnSasl() {
      return maxRetriesOnSasl;
    }

    /** max connection retries on socket time outs */
    public int getMaxRetriesOnSocketTimeouts() {
      return maxRetriesOnSocketTimeouts;
    }
    
    boolean getTcpNoDelay() {
      return tcpNoDelay;
    }
    
    boolean getDoPing() {
      return doPing;
    }
    
    int getPingInterval() {
      return pingInterval;
    }
    
    @VisibleForTesting
    String getSaslQop() {
      return saslQop;
    }
    
    static ConnectionId getConnectionId(InetSocketAddress addr,
        Class<?> protocol, UserGroupInformation ticket, int rpcTimeout,
        Configuration conf) throws IOException {
      return getConnectionId(addr, protocol, ticket, rpcTimeout, null, conf);
    }

    /**
     * Returns a ConnectionId object. 
     * @param addr Remote address for the connection.
     * @param protocol Protocol for RPC.
     * @param ticket UGI
     * @param rpcTimeout timeout
     * @param conf Configuration object
     * @return A ConnectionId instance
     * @throws IOException
     */
    static ConnectionId getConnectionId(InetSocketAddress addr,
        Class<?> protocol, UserGroupInformation ticket, int rpcTimeout,
        RetryPolicy connectionRetryPolicy, Configuration conf) throws IOException {

      if (connectionRetryPolicy == null) {
        final int max = conf.getInt(
            CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_KEY,
            CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_MAX_RETRIES_DEFAULT);
        final int retryInterval = conf.getInt(
            CommonConfigurationKeysPublic.IPC_CLIENT_CONNECT_RETRY_INTERVAL_KEY,
            CommonConfigurationKeysPublic
                .IPC_CLIENT_CONNECT_RETRY_INTERVAL_DEFAULT);

        connectionRetryPolicy = RetryPolicies.retryUpToMaximumCountWithFixedSleep(
            max, retryInterval, TimeUnit.MILLISECONDS);
      }

      return new ConnectionId(addr, protocol, ticket, rpcTimeout,
          connectionRetryPolicy, conf);
    }
    
    static boolean isEqual(Object a, Object b) {
      return a == null ? b == null : a.equals(b);
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == this) {
        return true;
      }
      if (obj instanceof ConnectionId) {
        ConnectionId that = (ConnectionId) obj;

        return isEqual(this.address, that.address)//同一个远端服务地址，即要连接同一个服务端
            && this.doPing == that.doPing
            && this.maxIdleTime == that.maxIdleTime
            && isEqual(this.connectionRetryPolicy, that.connectionRetryPolicy)
            && this.pingInterval == that.pingInterval
            && isEqual(this.protocol, that.protocol)//同一个远程协议，像datanode与namenode，client与 namenode等之间通信的时候都各自有自己的协议，如果不是同一个协议则使用不同的连接
                && this.rpcTimeout == that.rpcTimeout
            && this.tcpNoDelay == that.tcpNoDelay
            && isEqual(this.ticket, that.ticket);
      }
      return false;
    }
    
    @Override
    public int hashCode() {
      int result = connectionRetryPolicy.hashCode();
      result = PRIME * result + ((address == null) ? 0 : address.hashCode());
      result = PRIME * result + (doPing ? 1231 : 1237);
      result = PRIME * result + maxIdleTime;
      result = PRIME * result + pingInterval;
      result = PRIME * result + ((protocol == null) ? 0 : protocol.hashCode());
      result = PRIME * result + rpcTimeout;
      result = PRIME * result + (tcpNoDelay ? 1231 : 1237);
      result = PRIME * result + ((ticket == null) ? 0 : ticket.hashCode());
      return result;
    }
    
    @Override
    public String toString() {
      return address.toString();
    }
  }  

  /**
   * Returns the next valid sequential call ID by incrementing an atomic counter
   * and masking off the sign bit.  Valid call IDs are non-negative integers in
   * the range [ 0, 2^31 - 1 ].  Negative numbers are reserved for special
   * purposes.  The values can overflow back to 0 and be reused.  Note that prior
   * versions of the client did not mask off the sign bit, so a server may still
   * see a negative call ID if it receives connections from an old client.
   * 
   * @return next call ID
   */
  public static int nextCallId() {
    return callIdCounter.getAndIncrement() & 0x7FFFFFFF;
  }
}
