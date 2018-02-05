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

package org.apache.hadoop.yarn.server.nodemanager.containermanager;

import static org.apache.hadoop.service.Service.STATE.STARTED;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.io.DataInputByteBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.SaslRpcServer;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.service.ServiceStateChangeListener;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.ContainerManagementProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusesResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainersRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainersResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainersRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainersResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.LogAggregationContext;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.SerializedException;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationIdPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.LogAggregationContextPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ProtoUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.InvalidAuxServiceException;
import org.apache.hadoop.yarn.exceptions.InvalidContainerException;
import org.apache.hadoop.yarn.exceptions.NMNotYetReadyException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.ipc.RPCUtil;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationACLMapProto;
import org.apache.hadoop.yarn.proto.YarnServerNodemanagerRecoveryProtos.ContainerManagerApplicationProto;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.security.NMTokenIdentifier;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeContainerUpdate;
import org.apache.hadoop.yarn.server.nodemanager.CMgrCompletedAppsEvent;
import org.apache.hadoop.yarn.server.nodemanager.CMgrCompletedContainersEvent;
import org.apache.hadoop.yarn.server.nodemanager.CMgrUpdateContainersEvent;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.ContainerManagerEvent;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.DeletionService;
import org.apache.hadoop.yarn.server.nodemanager.LocalDirsHandlerService;
import org.apache.hadoop.yarn.server.nodemanager.NMAuditLogger;
import org.apache.hadoop.yarn.server.nodemanager.NMAuditLogger.AuditConstants;
import org.apache.hadoop.yarn.server.nodemanager.NodeManager;
import org.apache.hadoop.yarn.server.nodemanager.NodeStatusUpdater;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.Application;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationContainerInitEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationFinishEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationImpl;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationInitEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerImpl;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerKillEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerResourceUpdate;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher.ContainersLauncher;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher.ContainersLauncherEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ResourceLocalizationService;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.event.LocalizationEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.sharedcache.SharedCacheUploadEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.sharedcache.SharedCacheUploadService;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.logaggregation.LogAggregationService;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.loghandler.LogHandler;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.loghandler.NonAggregatingLogHandler;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.loghandler.event.LogHandlerEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor.ContainersMonitor;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor.ContainersMonitorEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor.ContainersMonitorImpl;
import org.apache.hadoop.yarn.server.nodemanager.metrics.NodeManagerMetrics;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService.RecoveredApplicationsState;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService.RecoveredContainerState;
import org.apache.hadoop.yarn.server.nodemanager.recovery.NMStateStoreService.RecoveredContainerStatus;
import org.apache.hadoop.yarn.server.nodemanager.security.authorize.NMPolicyProvider;
import org.apache.hadoop.yarn.server.security.ApplicationACLsManager;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;

public class ContainerManagerImpl extends CompositeService implements
    ServiceStateChangeListener, ContainerManagementProtocol,
    EventHandler<ContainerManagerEvent> {

  /**
   * Extra duration to wait for applications to be killed on shutdown.
   */
  private static final int SHUTDOWN_CLEANUP_SLOP_MS = 1000;

  private static final Log LOG = LogFactory.getLog(ContainerManagerImpl.class);

  final Context context;
  private final ContainersMonitor containersMonitor;
  private Server server;
  private final ResourceLocalizationService rsrcLocalizationSrvc;
  private final ContainersLauncher containersLauncher;
  private final AuxServices auxiliaryServices;
  private final NodeManagerMetrics metrics;

  private final NodeStatusUpdater nodeStatusUpdater;

  protected LocalDirsHandlerService dirsHandler;
  protected final AsyncDispatcher dispatcher;
  private final ApplicationACLsManager aclsManager;

  private final DeletionService deletionService;
  private AtomicBoolean blockNewContainerRequests = new AtomicBoolean(false);
  private boolean serviceStopped = false;
  private final ReadLock readLock;
  private final WriteLock writeLock;

  private long waitForContainersOnShutdownMillis;

  //ProcessorSharing
  private boolean processorSharingEnabled;
  private long processorSharingWindow;
  private int processorSharingFineGrainedInterval;
  private ProcessorSharingMonitor processorSharingMonitor;

  public ContainerManagerImpl(Context context, ContainerExecutor exec,
      DeletionService deletionContext, NodeStatusUpdater nodeStatusUpdater,
      NodeManagerMetrics metrics, ApplicationACLsManager aclsManager,
      LocalDirsHandlerService dirsHandler) {
    super(ContainerManagerImpl.class.getName());
    this.context = context;
    this.dirsHandler = dirsHandler;

    // ContainerManager level dispatcher.
    dispatcher = new AsyncDispatcher();
    this.deletionService = deletionContext;
    this.metrics = metrics;

    rsrcLocalizationSrvc =
        createResourceLocalizationService(exec, deletionContext, context);
    addService(rsrcLocalizationSrvc);

    containersLauncher = createContainersLauncher(context, exec);
    addService(containersLauncher);

    this.nodeStatusUpdater = nodeStatusUpdater;
    this.aclsManager = aclsManager;

    // Start configurable services
    auxiliaryServices = new AuxServices();
    auxiliaryServices.registerServiceListener(this);
    addService(auxiliaryServices);

    this.containersMonitor =
        new ContainersMonitorImpl(exec, dispatcher, this.context);
    addService(this.containersMonitor);

    dispatcher.register(ContainerEventType.class,
        new ContainerEventDispatcher());
    dispatcher.register(ApplicationEventType.class,
        new ApplicationEventDispatcher());
    dispatcher.register(LocalizationEventType.class, rsrcLocalizationSrvc);
    dispatcher.register(AuxServicesEventType.class, auxiliaryServices);
    dispatcher.register(ContainersMonitorEventType.class, containersMonitor);
    dispatcher.register(ContainersLauncherEventType.class, containersLauncher);
    
    addService(dispatcher);

    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    this.readLock = lock.readLock();
    this.writeLock = lock.writeLock();
  }

  @Override
  public void serviceInit(Configuration conf) throws Exception {
    LogHandler logHandler =
      createLogHandler(conf, this.context, this.deletionService);
    addIfService(logHandler);
    dispatcher.register(LogHandlerEventType.class, logHandler);
    
    // add the shared cache upload service (it will do nothing if the shared
    // cache is disabled)
    SharedCacheUploadService sharedCacheUploader =
        createSharedCacheUploaderService();
    addService(sharedCacheUploader);
    dispatcher.register(SharedCacheUploadEventType.class, sharedCacheUploader);

    waitForContainersOnShutdownMillis =
        conf.getLong(YarnConfiguration.NM_SLEEP_DELAY_BEFORE_SIGKILL_MS,
            YarnConfiguration.DEFAULT_NM_SLEEP_DELAY_BEFORE_SIGKILL_MS) +
        conf.getLong(YarnConfiguration.NM_PROCESS_KILL_WAIT_MS,
            YarnConfiguration.DEFAULT_NM_PROCESS_KILL_WAIT_MS) +
        SHUTDOWN_CLEANUP_SLOP_MS;
    //ProcessorSharing
    processorSharingEnabled = conf.getBoolean(YarnConfiguration.NM_PROCESSOR_SHARING_ENABLE,YarnConfiguration.DEFAULT_NM_PROCESSOR_SHARING_ENABLE);
    if (processorSharingEnabled) {
        processorSharingWindow = conf.getLong(YarnConfiguration.NM_PROCESSOR_SHARING_WINDOW_MS, YarnConfiguration.DEFAULT_NM_PROCESSOR_SHARING_WINDOW_MS);//default 5s
        processorSharingFineGrainedInterval = conf.getInt(YarnConfiguration.NM_PROCESSOR_SHARING_FINEGRAINED_INTERVAL_MS, YarnConfiguration.DEFAULT_NM_PROCESSOR_SHARING_FINEGRAINED_INTERVAL_MS);//default 500ms
        int minimumMemory = conf.getInt(YarnConfiguration.NM_PROCESSOR_SHARING_MINIMUM_MEMORY_MB, YarnConfiguration.DEFAULT_NM_PROCESSOR_SHARING_MINIMUM_MEMORY_MB);//default 128mb
        double minimumCpu = conf.getDouble(YarnConfiguration.NM_PROCESSOR_SHARING_MINIMUM_CPU, YarnConfiguration.DEFAULT_NM_PROCESSOR_SHARING_MINIMUM_CPU);
        // Concurrent containers should be calculated based on the total resources of a node and the size of the containers. IDEALLY DYNAMIC
        int maximumConcurrentContainers = conf.getInt(YarnConfiguration.NM_PROCESSOR_SHARING_MAXIMUM_CONCURRENT_CONTAINERS, YarnConfiguration.DEFAULT_NM_PROCESSOR_SHARING_MAXIMUM_CONCURRENT_CONTAINERS);
        this.processorSharingMonitor = new ProcessorSharingMonitor(context, this.processorSharingWindow, processorSharingFineGrainedInterval, minimumMemory, minimumCpu, maximumConcurrentContainers);
    }
    super.serviceInit(conf);
    recover();
  }

  @SuppressWarnings("unchecked")
  private void recover() throws IOException, URISyntaxException {
    NMStateStoreService stateStore = context.getNMStateStore();
    if (stateStore.canRecover()) {
      rsrcLocalizationSrvc.recoverLocalizedResources(
          stateStore.loadLocalizationState());

      RecoveredApplicationsState appsState = stateStore.loadApplicationsState();
      for (ContainerManagerApplicationProto proto :
           appsState.getApplications()) {
        recoverApplication(proto);
      }

      for (RecoveredContainerState rcs : stateStore.loadContainersState()) {
        recoverContainer(rcs);
      }

      String diagnostic = "Application marked finished during recovery";
      for (ApplicationId appId : appsState.getFinishedApplications()) {
        dispatcher.getEventHandler().handle(
            new ApplicationFinishEvent(appId, diagnostic));
      }
    }
  }

  private void recoverApplication(ContainerManagerApplicationProto p)
      throws IOException {
    ApplicationId appId = new ApplicationIdPBImpl(p.getId());
    Credentials creds = new Credentials();
    creds.readTokenStorageStream(
        new DataInputStream(p.getCredentials().newInput()));

    List<ApplicationACLMapProto> aclProtoList = p.getAclsList();
    Map<ApplicationAccessType, String> acls =
        new HashMap<ApplicationAccessType, String>(aclProtoList.size());
    for (ApplicationACLMapProto aclProto : aclProtoList) {
      acls.put(ProtoUtils.convertFromProtoFormat(aclProto.getAccessType()),
          aclProto.getAcl());
    }

    LogAggregationContext logAggregationContext = null;
    if (p.getLogAggregationContext() != null) {
      logAggregationContext =
          new LogAggregationContextPBImpl(p.getLogAggregationContext());
    }

    LOG.info("Recovering application " + appId);
    ApplicationImpl app = new ApplicationImpl(dispatcher, p.getUser(), appId,
        creds, context);
    context.getApplications().put(appId, app);
    app.handle(new ApplicationInitEvent(appId, acls, logAggregationContext));
  }

  @SuppressWarnings("unchecked")
  private void recoverContainer(RecoveredContainerState rcs)
      throws IOException {
    StartContainerRequest req = rcs.getStartRequest();
    ContainerLaunchContext launchContext = req.getContainerLaunchContext();
    ContainerTokenIdentifier token =
        BuilderUtils.newContainerTokenIdentifier(req.getContainerToken());
    ContainerId containerId = token.getContainerID();
    ApplicationId appId =
        containerId.getApplicationAttemptId().getApplicationId();

    LOG.info("Recovering " + containerId + " in state " + rcs.getStatus()
        + " with exit code " + rcs.getExitCode());
    
    Set<Integer> cores= this.context.getCoresManager().allocateCores(containerId, 
    		                          token.getResource().getVirtualCores());
   
    if (context.getApplications().containsKey(appId)) {
      Credentials credentials = parseCredentials(launchContext);
      Container container = new ContainerImpl(this.context,getConfig(), dispatcher,
          context.getNMStateStore(), req.getContainerLaunchContext(),
          credentials, metrics, token, rcs.getStatus(), rcs.getExitCode(),
          rcs.getDiagnostics(), rcs.getKilled(),cores);
      
      context.getContainers().put(containerId, container);
      dispatcher.getEventHandler().handle(
          new ApplicationContainerInitEvent(container));
    } else {
      if (rcs.getStatus() != RecoveredContainerStatus.COMPLETED) {
        LOG.warn(containerId + " has no corresponding application!");
      }
      LOG.info("Adding " + containerId + " to recently stopped containers");
      nodeStatusUpdater.addCompletedContainer(containerId);
    }
  }

  private void updateOldestYoungestAge(int oldestYoungestAge) {
      LOG.info("Adding oldestYoungestAge "+oldestYoungestAge);
	  nodeStatusUpdater.addOldestYoungestAge(oldestYoungestAge);
  }
  
  private void waitForRecoveredContainers() throws InterruptedException {
    final int sleepMsec = 100;
    int waitIterations = 100;
    List<ContainerId> newContainers = new ArrayList<ContainerId>();
    while (--waitIterations >= 0) {
      newContainers.clear();
      for (Container container : context.getContainers().values()) {
        if (container.getContainerState() == org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerState.NEW) {
          newContainers.add(container.getContainerId());
        }
      }
      if (newContainers.isEmpty()) {
        break;
      }
      LOG.info("Waiting for containers: " + newContainers);
      Thread.sleep(sleepMsec);
    }
    if (waitIterations < 0) {
      LOG.warn("Timeout waiting for recovered containers");
    }
  }

  protected LogHandler createLogHandler(Configuration conf, Context context,
      DeletionService deletionService) {
    if (conf.getBoolean(YarnConfiguration.LOG_AGGREGATION_ENABLED,
        YarnConfiguration.DEFAULT_LOG_AGGREGATION_ENABLED)) {
      return new LogAggregationService(this.dispatcher, context,
          deletionService, dirsHandler);
    } else {
      return new NonAggregatingLogHandler(this.dispatcher, deletionService,
                                          dirsHandler,
                                          context.getNMStateStore());
    }
  }

  public ContainersMonitor getContainersMonitor() {
    return this.containersMonitor;
  }

  protected ResourceLocalizationService createResourceLocalizationService(
      ContainerExecutor exec, DeletionService deletionContext, Context context) {
    return new ResourceLocalizationService(this.dispatcher, exec,
        deletionContext, dirsHandler, context);
  }

  protected SharedCacheUploadService createSharedCacheUploaderService() {
    return new SharedCacheUploadService();
  }

  protected ContainersLauncher createContainersLauncher(Context context,
      ContainerExecutor exec) {
    return new ContainersLauncher(context, this.dispatcher, exec, dirsHandler, this);
  }

  @Override
  protected void serviceStart() throws Exception {

    // Enqueue user dirs in deletion context

    Configuration conf = getConfig();
    final InetSocketAddress initialAddress = conf.getSocketAddr(
        YarnConfiguration.NM_BIND_HOST,
        YarnConfiguration.NM_ADDRESS,
        YarnConfiguration.DEFAULT_NM_ADDRESS,
        YarnConfiguration.DEFAULT_NM_PORT);
    boolean usingEphemeralPort = (initialAddress.getPort() == 0);
    if (context.getNMStateStore().canRecover() && usingEphemeralPort) {
      throw new IllegalArgumentException("Cannot support recovery with an "
          + "ephemeral server port. Check the setting of "
          + YarnConfiguration.NM_ADDRESS);
    }
    // If recovering then delay opening the RPC service until the recovery
    // of resources and containers have completed, otherwise requests from
    // clients during recovery can interfere with the recovery process.
    final boolean delayedRpcServerStart =
        context.getNMStateStore().canRecover();

    Configuration serverConf = new Configuration(conf);

    // always enforce it to be token-based.
    serverConf.set(
      CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION,
      SaslRpcServer.AuthMethod.TOKEN.toString());
    
    YarnRPC rpc = YarnRPC.create(conf);

    server =
        rpc.getServer(ContainerManagementProtocol.class, this, initialAddress, 
            serverConf, this.context.getNMTokenSecretManager(),
            conf.getInt(YarnConfiguration.NM_CONTAINER_MGR_THREAD_COUNT, 
                YarnConfiguration.DEFAULT_NM_CONTAINER_MGR_THREAD_COUNT));
    
    // Enable service authorization?
    if (conf.getBoolean(
        CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION, 
        false)) {
      refreshServiceAcls(conf, new NMPolicyProvider());
    }
    
    LOG.info("Blocking new container-requests as container manager rpc" +
    		" server is still starting.");
    this.setBlockNewContainerRequests(true);

    String bindHost = conf.get(YarnConfiguration.NM_BIND_HOST);
    String nmAddress = conf.getTrimmed(YarnConfiguration.NM_ADDRESS);
    String hostOverride = null;
    if (bindHost != null && !bindHost.isEmpty()
        && nmAddress != null && !nmAddress.isEmpty()) {
      //a bind-host case with an address, to support overriding the first
      //hostname found when querying for our hostname with the specified
      //address, combine the specified address with the actual port listened
      //on by the server
      hostOverride = nmAddress.split(":")[0];
    }

    // setup node ID
    InetSocketAddress connectAddress;
    if (delayedRpcServerStart) {
      connectAddress = NetUtils.getConnectAddress(initialAddress);
    } else {
      server.start();
      connectAddress = NetUtils.getConnectAddress(server);
    }
    NodeId nodeId = buildNodeId(connectAddress, hostOverride);
    ((NodeManager.NMContext)context).setNodeId(nodeId);
    this.context.getNMTokenSecretManager().setNodeId(nodeId);
    this.context.getContainerTokenSecretManager().setNodeId(nodeId);

    // start remaining services
    super.serviceStart();

    if (delayedRpcServerStart) {
      waitForRecoveredContainers();
      server.start();

      // check that the node ID is as previously advertised
      connectAddress = NetUtils.getConnectAddress(server);
      NodeId serverNode = buildNodeId(connectAddress, hostOverride);
      if (!serverNode.equals(nodeId)) {
        throw new IOException("Node mismatch after server started, expected '"
            + nodeId + "' but found '" + serverNode + "'");
      }
    }
    
    if(this.processorSharingEnabled)
        this.processorSharingMonitor.start();

    LOG.info("ContainerManager started at " + connectAddress);
    LOG.info("ContainerManager bound to " + initialAddress);
  }

  private NodeId buildNodeId(InetSocketAddress connectAddress,
      String hostOverride) {
    if (hostOverride != null) {
      connectAddress = NetUtils.getConnectAddress(
          new InetSocketAddress(hostOverride, connectAddress.getPort()));
    }
    return NodeId.newInstance(
        connectAddress.getAddress().getCanonicalHostName(),
        connectAddress.getPort());
  }

  void refreshServiceAcls(Configuration configuration, 
      PolicyProvider policyProvider) {
    this.server.refreshServiceAcl(configuration, policyProvider);
  }

  @Override
  public void serviceStop() throws Exception {
    setBlockNewContainerRequests(true);
    this.writeLock.lock();
    this.processorSharingMonitor.stopRunning();
    try {
      serviceStopped = true;
      if (context != null) {
        cleanUpApplicationsOnNMShutDown();
      }
    } finally {
      this.writeLock.unlock();
    }
    if (auxiliaryServices.getServiceState() == STARTED) {
      auxiliaryServices.unregisterServiceListener(this);
    }
    if (server != null) {
      server.stop();
    }
    super.serviceStop();
  }

  public void cleanUpApplicationsOnNMShutDown() {
    Map<ApplicationId, Application> applications =
        this.context.getApplications();
    if (applications.isEmpty()) {
      return;
    }
    LOG.info("Applications still running : " + applications.keySet());

    if (this.context.getNMStateStore().canRecover()
        && !this.context.getDecommissioned()) {
      // do not cleanup apps as they can be recovered on restart
      return;
    }

    List<ApplicationId> appIds =
        new ArrayList<ApplicationId>(applications.keySet());
    this.handle(
        new CMgrCompletedAppsEvent(appIds,
            CMgrCompletedAppsEvent.Reason.ON_SHUTDOWN));

    LOG.info("Waiting for Applications to be Finished");

    long waitStartTime = System.currentTimeMillis();
    while (!applications.isEmpty()
        && System.currentTimeMillis() - waitStartTime < waitForContainersOnShutdownMillis) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException ex) {
        LOG.warn(
          "Interrupted while sleeping on applications finish on shutdown", ex);
      }
    }

    // All applications Finished
    if (applications.isEmpty()) {
      LOG.info("All applications in FINISHED state");
    } else {
      LOG.info("Done waiting for Applications to be Finished. Still alive: " +
          applications.keySet());
    }
  }

  public void cleanupContainersOnNMResync() {
    Map<ContainerId, Container> containers = context.getContainers();
    if (containers.isEmpty()) {
      return;
    }
    LOG.info("Containers still running on "
        + CMgrCompletedContainersEvent.Reason.ON_NODEMANAGER_RESYNC + " : "
        + containers.keySet());

    List<ContainerId> containerIds =
      new ArrayList<ContainerId>(containers.keySet());

    LOG.info("Waiting for containers to be killed");

    this.handle(new CMgrCompletedContainersEvent(containerIds,
      CMgrCompletedContainersEvent.Reason.ON_NODEMANAGER_RESYNC));

    /*
     * We will wait till all the containers change their state to COMPLETE. We
     * will not remove the container statuses from nm context because these
     * are used while re-registering node manager with resource manager.
     */
    boolean allContainersCompleted = false;
    while (!containers.isEmpty() && !allContainersCompleted) {
      allContainersCompleted = true;
      for (Entry<ContainerId, Container> container : containers.entrySet()) {
        if (((ContainerImpl) container.getValue()).getCurrentState()
            != ContainerState.COMPLETE) {
          allContainersCompleted = false;
          try {
            Thread.sleep(1000);
          } catch (InterruptedException ex) {
            LOG.warn("Interrupted while sleeping on container kill on resync",
              ex);
          }
          break;
        }
      }
    }
    // All containers killed
    if (allContainersCompleted) {
      LOG.info("All containers in DONE state");
    } else {
      LOG.info("Done waiting for containers to be killed. Still alive: " +
        containers.keySet());
    }
  }

  // Get the remoteUGI corresponding to the api call.
  protected UserGroupInformation getRemoteUgi()
      throws YarnException {
    UserGroupInformation remoteUgi;
    try {
      remoteUgi = UserGroupInformation.getCurrentUser();
    } catch (IOException e) {
      String msg = "Cannot obtain the user-name. Got exception: "
          + StringUtils.stringifyException(e);
      LOG.warn(msg);
      throw RPCUtil.getRemoteException(msg);
    }
    return remoteUgi;
  }

  // Obtain the needed ContainerTokenIdentifier from the remote-UGI. RPC layer
  // currently sets only the required id, but iterate through anyways just to
  // be sure.
  @Private
  @VisibleForTesting
  protected NMTokenIdentifier selectNMTokenIdentifier(
      UserGroupInformation remoteUgi) {
    Set<TokenIdentifier> tokenIdentifiers = remoteUgi.getTokenIdentifiers();
    NMTokenIdentifier resultId = null;
    for (TokenIdentifier id : tokenIdentifiers) {
      if (id instanceof NMTokenIdentifier) {
        resultId = (NMTokenIdentifier) id;
        break;
      }
    }
    return resultId;
  }

  protected void authorizeUser(UserGroupInformation remoteUgi,
      NMTokenIdentifier nmTokenIdentifier) throws YarnException {
    if (!remoteUgi.getUserName().equals(
      nmTokenIdentifier.getApplicationAttemptId().toString())) {
      throw RPCUtil.getRemoteException("Expected applicationAttemptId: "
          + remoteUgi.getUserName() + "Found: "
          + nmTokenIdentifier.getApplicationAttemptId());
    }
  }

  /**
   * @param containerTokenIdentifier
   *          of the container to be started
   * @throws YarnException
   */
  @Private
  @VisibleForTesting
  protected void authorizeStartRequest(NMTokenIdentifier nmTokenIdentifier,
      ContainerTokenIdentifier containerTokenIdentifier) throws YarnException {

    ContainerId containerId = containerTokenIdentifier.getContainerID();
    String containerIDStr = containerId.toString();
    boolean unauthorized = false;
    StringBuilder messageBuilder =
        new StringBuilder("Unauthorized request to start container. ");
    if (!nmTokenIdentifier.getApplicationAttemptId().getApplicationId().
        equals(containerId.getApplicationAttemptId().getApplicationId())) {
      unauthorized = true;
      messageBuilder.append("\nNMToken for application attempt : ")
        .append(nmTokenIdentifier.getApplicationAttemptId())
        .append(" was used for starting container with container token")
        .append(" issued for application attempt : ")
        .append(containerId.getApplicationAttemptId());
    } else if (!this.context.getContainerTokenSecretManager()
        .isValidStartContainerRequest(containerTokenIdentifier)) {
      // Is the container being relaunched? Or RPC layer let startCall with
      // tokens generated off old-secret through?
      unauthorized = true;
      messageBuilder.append("\n Attempt to relaunch the same ")
        .append("container with id ").append(containerIDStr).append(".");
    } else if (containerTokenIdentifier.getExpiryTimeStamp() < System
      .currentTimeMillis()) {
      // Ensure the token is not expired.
      unauthorized = true;
      messageBuilder.append("\nThis token is expired. current time is ")
        .append(System.currentTimeMillis()).append(" found ")
        .append(containerTokenIdentifier.getExpiryTimeStamp());
      messageBuilder.append("\nNote: System times on machines may be out of sync.")
        .append(" Check system time and time zones.");
    }
    if (unauthorized) {
      String msg = messageBuilder.toString();
      LOG.error(msg);
      throw RPCUtil.getRemoteException(msg);
    }
  }

  /**
   * Start a list of containers on this NodeManager.
   */
  @Override
  public StartContainersResponse
      startContainers(StartContainersRequest requests) throws YarnException,
          IOException {
    if (blockNewContainerRequests.get()) {
      throw new NMNotYetReadyException(
        "Rejecting new containers as NodeManager has not"
            + " yet connected with ResourceManager");
    }
    UserGroupInformation remoteUgi = getRemoteUgi();
    NMTokenIdentifier nmTokenIdentifier = selectNMTokenIdentifier(remoteUgi);
    authorizeUser(remoteUgi,nmTokenIdentifier);
    List<ContainerId> succeededContainers = new ArrayList<ContainerId>();
    Map<ContainerId, SerializedException> failedContainers =
        new HashMap<ContainerId, SerializedException>();
    for (StartContainerRequest request : requests.getStartContainerRequests()) {
      ContainerId containerId = null;
      try {
        ContainerTokenIdentifier containerTokenIdentifier =
            BuilderUtils.newContainerTokenIdentifier(request.getContainerToken());
        verifyAndGetContainerTokenIdentifier(request.getContainerToken(),
          containerTokenIdentifier);
        containerId = containerTokenIdentifier.getContainerID();
    	//LOG.info("PAMELA startContainers starting container "+ containerId);
        startContainerInternal(nmTokenIdentifier, containerTokenIdentifier,
          request);
        LOG.info("PAMELA is this container attemptId "+ containerId+" is an AM container? "+containerId.getContainerId()+ "==1?");
        if(containerId.getContainerId() != 1 && this.processorSharingEnabled)
        	this.processorSharingMonitor.addContainer(containerId);
        succeededContainers.add(containerId);
      } catch (YarnException e) {
        failedContainers.put(containerId, SerializedException.newInstance(e));
      } catch (InvalidToken ie) {
        failedContainers.put(containerId, SerializedException.newInstance(ie));
        throw ie;
      } catch (IOException e) {
        throw RPCUtil.getRemoteException(e);
      }
    }

    return StartContainersResponse.newInstance(getAuxServiceMetaData(),
      succeededContainers, failedContainers);
  }

  private ContainerManagerApplicationProto buildAppProto(ApplicationId appId,
      String user, Credentials credentials,
      Map<ApplicationAccessType, String> appAcls,
      LogAggregationContext logAggregationContext) {

    ContainerManagerApplicationProto.Builder builder =
        ContainerManagerApplicationProto.newBuilder();
    builder.setId(((ApplicationIdPBImpl) appId).getProto());
    builder.setUser(user);

    if (logAggregationContext != null) {
      builder.setLogAggregationContext((
          (LogAggregationContextPBImpl)logAggregationContext).getProto());
    }

    builder.clearCredentials();
    if (credentials != null) {
      DataOutputBuffer dob = new DataOutputBuffer();
      try {
        credentials.writeTokenStorageToStream(dob);
        builder.setCredentials(ByteString.copyFrom(dob.getData()));
      } catch (IOException e) {
        // should not occur
        LOG.error("Cannot serialize credentials", e);
      }
    }

    builder.clearAcls();
    if (appAcls != null) {
      for (Map.Entry<ApplicationAccessType, String> acl : appAcls.entrySet()) {
        ApplicationACLMapProto p = ApplicationACLMapProto.newBuilder()
            .setAccessType(ProtoUtils.convertToProtoFormat(acl.getKey()))
            .setAcl(acl.getValue())
            .build();
        builder.addAcls(p);
      }
    }

    return builder.build();
  }

  @SuppressWarnings("unchecked")
  private void startContainerInternal(NMTokenIdentifier nmTokenIdentifier,
      ContainerTokenIdentifier containerTokenIdentifier,
      StartContainerRequest request) throws YarnException, IOException {

    /*
     * 1) It should save the NMToken into NMTokenSecretManager. This is done
     * here instead of RPC layer because at the time of opening/authenticating
     * the connection it doesn't know what all RPC calls user will make on it.
     * Also new NMToken is issued only at startContainer (once it gets renewed).
     * 
     * 2) It should validate containerToken. Need to check below things. a) It
     * is signed by correct master key (part of retrieve password). b) It
     * belongs to correct Node Manager (part of retrieve password). c) It has
     * correct RMIdentifier. d) It is not expired.
     */
	  
    authorizeStartRequest(nmTokenIdentifier, containerTokenIdentifier);
 
    if (containerTokenIdentifier.getRMIdentifier() != nodeStatusUpdater
        .getRMIdentifier()) {
        // Is the container coming from unknown RM
        StringBuilder sb = new StringBuilder("\nContainer ");
        sb.append(containerTokenIdentifier.getContainerID().toString())
          .append(" rejected as it is allocated by a previous RM");
        throw new InvalidContainerException(sb.toString());
    }
    // update NMToken
    updateNMTokenIdentifier(nmTokenIdentifier);

    ContainerId containerId = containerTokenIdentifier.getContainerID();
    String containerIdStr = containerId.toString();
    String user = containerTokenIdentifier.getApplicationSubmitter();

    LOG.info("Start request for " + containerIdStr + " by user " + user);
 
    ContainerLaunchContext launchContext = request.getContainerLaunchContext();

    Map<String, ByteBuffer> serviceData = getAuxServiceMetaData();
    if (launchContext.getServiceData()!=null && 
        !launchContext.getServiceData().isEmpty()) {
      for (Map.Entry<String, ByteBuffer> meta : launchContext.getServiceData()
          .entrySet()) {
        if (null == serviceData.get(meta.getKey())) {
          throw new InvalidAuxServiceException("The auxService:" + meta.getKey()
              + " does not exist");
        }
      }
    }

    Credentials credentials = parseCredentials(launchContext);
    
    
    Set<Integer> cores = this.context.getCoresManager().allocateCores(containerId,
    		                            containerTokenIdentifier.getResource().getVirtualCores());
    
   
    Container container =
        new ContainerImpl(this.context,getConfig(), this.dispatcher,
            context.getNMStateStore(), launchContext,
          credentials, metrics, containerTokenIdentifier,cores);
    LOG.info("allocate cpuset "+cores+" for containers "+container.getContainerId());
    
    ApplicationId applicationID =
        containerId.getApplicationAttemptId().getApplicationId();
    if (context.getContainers().putIfAbsent(containerId, container) != null) {
      NMAuditLogger.logFailure(user, AuditConstants.START_CONTAINER,
        "ContainerManagerImpl", "Container already running on this node!",
        applicationID, containerId);
      throw RPCUtil.getRemoteException("Container " + containerIdStr
          + " already is running on this node!!");
    }
    LOG.info("PAMELA startContainerInternal added container "+containerId+" to context, new number of containers "+context.getContainers().size());
    this.readLock.lock();
    try {
      if (!serviceStopped) {
        // Create the application
        Application application =
            new ApplicationImpl(dispatcher, user, applicationID, credentials, context);
        if (null == context.getApplications().putIfAbsent(applicationID,
          application)) {
          LOG.info("Creating a new application reference for app " + applicationID);
          LogAggregationContext logAggregationContext =
              containerTokenIdentifier.getLogAggregationContext();
          Map<ApplicationAccessType, String> appAcls =
              container.getLaunchContext().getApplicationACLs();
          context.getNMStateStore().storeApplication(applicationID,
              buildAppProto(applicationID, user, credentials, appAcls,
                logAggregationContext));
          dispatcher.getEventHandler().handle(
            new ApplicationInitEvent(applicationID, appAcls,
              logAggregationContext));
        }

        this.context.getNMStateStore().storeContainer(containerId, request);
        dispatcher.getEventHandler().handle(
          new ApplicationContainerInitEvent(container));

        this.context.getContainerTokenSecretManager().startContainerSuccessful(
          containerTokenIdentifier);
        NMAuditLogger.logSuccess(user, AuditConstants.START_CONTAINER,
          "ContainerManageImpl", applicationID, containerId);
        // TODO launchedContainer misplaced -> doesn't necessarily mean a container
        // launch. A finished Application will not launch containers.
        metrics.launchedContainer();
        metrics.allocateContainer(containerTokenIdentifier.getResource());
      } else {
        throw new YarnException(
            "Container start failed as the NodeManager is " +
            "in the process of shutting down");
      }
    } finally {
      this.readLock.unlock();
    }
  }

  protected ContainerTokenIdentifier verifyAndGetContainerTokenIdentifier(
      org.apache.hadoop.yarn.api.records.Token token,
      ContainerTokenIdentifier containerTokenIdentifier) throws YarnException,
      InvalidToken {
    byte[] password =
        context.getContainerTokenSecretManager().retrievePassword(
          containerTokenIdentifier);
    byte[] tokenPass = token.getPassword().array();
    if (password == null || tokenPass == null
        || !Arrays.equals(password, tokenPass)) {
      throw new InvalidToken(
        "Invalid container token used for starting container on : "
            + context.getNodeId().toString());
    }
    return containerTokenIdentifier;
  }

  @Private
  @VisibleForTesting
  protected void updateNMTokenIdentifier(NMTokenIdentifier nmTokenIdentifier)
      throws InvalidToken {
    context.getNMTokenSecretManager().appAttemptStartContainer(
      nmTokenIdentifier);
  }

  private Credentials parseCredentials(ContainerLaunchContext launchContext)
      throws IOException {
    Credentials credentials = new Credentials();
    // //////////// Parse credentials
    ByteBuffer tokens = launchContext.getTokens();

    if (tokens != null) {
      DataInputByteBuffer buf = new DataInputByteBuffer();
      tokens.rewind();
      buf.reset(tokens);
      credentials.readTokenStorageStream(buf);
      if (LOG.isDebugEnabled()) {
        for (Token<? extends TokenIdentifier> tk : credentials.getAllTokens()) {
          LOG.debug(tk.getService() + " = " + tk.toString());
        }
      }
    }
    // //////////// End of parsing credentials
    return credentials;
  }

  /**
   * Stop a list of containers running on this NodeManager.
   */
  @Override
  public StopContainersResponse stopContainers(StopContainersRequest requests)
      throws YarnException, IOException {

    List<ContainerId> succeededRequests = new ArrayList<ContainerId>();
    Map<ContainerId, SerializedException> failedRequests =
        new HashMap<ContainerId, SerializedException>();
    UserGroupInformation remoteUgi = getRemoteUgi();
    NMTokenIdentifier identifier = selectNMTokenIdentifier(remoteUgi);
    for (ContainerId id : requests.getContainerIds()) {
      try {
        stopContainerInternal(identifier, id);
        succeededRequests.add(id);
      } catch (YarnException e) {
        failedRequests.put(id, SerializedException.newInstance(e));
      }
    }
    return StopContainersResponse
      .newInstance(succeededRequests, failedRequests);
  }

  @SuppressWarnings("unchecked")
  private void stopContainerInternal(NMTokenIdentifier nmTokenIdentifier,
      ContainerId containerID) throws YarnException, IOException {
    String containerIDStr = containerID.toString();
    Container container = this.context.getContainers().get(containerID);
    LOG.info("Stopping container with container Id: " + containerIDStr);
    authorizeGetAndStopContainerRequest(containerID, container, true,
      nmTokenIdentifier);

    if (container == null) {
      if (!nodeStatusUpdater.isContainerRecentlyStopped(containerID)) {
        throw RPCUtil.getRemoteException("Container " + containerIDStr
          + " is not handled by this NodeManager");
      }
    } else {
      context.getNMStateStore().storeContainerKilled(containerID);
      dispatcher.getEventHandler().handle(
        new ContainerKillEvent(containerID,
            ContainerExitStatus.KILLED_BY_APPMASTER,
            "Container killed by the ApplicationMaster."));

      NMAuditLogger.logSuccess(container.getUser(),    
        AuditConstants.STOP_CONTAINER, "ContainerManageImpl", containerID
          .getApplicationAttemptId().getApplicationId(), containerID);

      // TODO: Move this code to appropriate place once kill_container is
      // implemented.
      nodeStatusUpdater.sendOutofBandHeartBeat();
    }
  }

  /**
   * Get a list of container statuses running on this NodeManager
   */
  @Override
  public GetContainerStatusesResponse getContainerStatuses(
      GetContainerStatusesRequest request) throws YarnException, IOException {

    List<ContainerStatus> succeededRequests = new ArrayList<ContainerStatus>();
    Map<ContainerId, SerializedException> failedRequests =
        new HashMap<ContainerId, SerializedException>();
    UserGroupInformation remoteUgi = getRemoteUgi();
    NMTokenIdentifier identifier = selectNMTokenIdentifier(remoteUgi);
    for (ContainerId id : request.getContainerIds()) {
      try {
        ContainerStatus status = getContainerStatusInternal(id, identifier);
        succeededRequests.add(status);
      } catch (YarnException e) {
        failedRequests.put(id, SerializedException.newInstance(e));
      }
    }
    return GetContainerStatusesResponse.newInstance(succeededRequests,
      failedRequests);
  }

  private ContainerStatus getContainerStatusInternal(ContainerId containerID,
      NMTokenIdentifier nmTokenIdentifier) throws YarnException {
    String containerIDStr = containerID.toString();
    Container container = this.context.getContainers().get(containerID);

    LOG.info("Getting container-status for " + containerIDStr);
    authorizeGetAndStopContainerRequest(containerID, container, false,
      nmTokenIdentifier);

    if (container == null) {
      if (nodeStatusUpdater.isContainerRecentlyStopped(containerID)) {
        throw RPCUtil.getRemoteException("Container " + containerIDStr
          + " was recently stopped on node manager.");
      } else {
        throw RPCUtil.getRemoteException("Container " + containerIDStr
          + " is not handled by this NodeManager");
      }
    }
    ContainerStatus containerStatus = container.cloneAndGetContainerStatus();
    LOG.info("Returning " + containerStatus);
    return containerStatus;
  }

  @Private
  @VisibleForTesting
  protected void authorizeGetAndStopContainerRequest(ContainerId containerId,
      Container container, boolean stopRequest, NMTokenIdentifier identifier)
      throws YarnException {
    /*
     * For get/stop container status; we need to verify that 1) User (NMToken)
     * application attempt only has started container. 2) Requested containerId
     * belongs to the same application attempt (NMToken) which was used. (Note:-
     * This will prevent user in knowing another application's containers).
     */
    ApplicationId nmTokenAppId =
        identifier.getApplicationAttemptId().getApplicationId();
    
    if ((!nmTokenAppId.equals(containerId.getApplicationAttemptId().getApplicationId()))
        || (container != null && !nmTokenAppId.equals(container
            .getContainerId().getApplicationAttemptId().getApplicationId()))) {
      if (stopRequest) {
        LOG.warn(identifier.getApplicationAttemptId()
            + " attempted to stop non-application container : "
            + container.getContainerId());
        NMAuditLogger.logFailure("UnknownUser", AuditConstants.STOP_CONTAINER,
          "ContainerManagerImpl", "Trying to stop unknown container!",
          nmTokenAppId, container.getContainerId());
      } else {
        LOG.warn(identifier.getApplicationAttemptId()
            + " attempted to get status for non-application container : "
            + container.getContainerId());
      }
    }
  }

  // ProcessorSharing
  private class ProcessorSharingContainer {
	  Container container;
	  long age;
	  long lastStarted_time_ms; 
	  
	  public ProcessorSharingContainer(ProcessorSharingContainer another) {
		    this.container = another.container; // you can access 
		    this.age = another.age;
		    this.lastStarted_time_ms = another.lastStarted_time_ms;
		  }
	  
	  public ProcessorSharingContainer(Container container) {
		  this.container = container;
		  this.age = 0;
		  this.lastStarted_time_ms = System.currentTimeMillis();
	  }
	  
	  public void resume() {
		  this.lastStarted_time_ms = System.currentTimeMillis();
	  }
	  
	  public void suspend() {
		  long progressNow = System.currentTimeMillis() - lastStarted_time_ms;
		  this.age += progressNow;
	  }
	  
	  public void updateAge() {
		  long timeNow = System.currentTimeMillis();
		  long progressNow = timeNow - lastStarted_time_ms;
		  this.age += progressNow;	
		  this.lastStarted_time_ms = timeNow;
	  }
  }
  
  private static Comparator<ProcessorSharingContainer> youngestContainersAgeComparator = new Comparator<ProcessorSharingContainer>(){	
		@Override
		public int compare(ProcessorSharingContainer container1, ProcessorSharingContainer container2) {
            return Math.toIntExact(container1.age - container2.age);
        }
  };
  
  private static Comparator<ProcessorSharingContainer> oldestContainersAgeComparator = new Comparator<ProcessorSharingContainer>(){	
		@Override
		public int compare(ProcessorSharingContainer container1, ProcessorSharingContainer container2) {
            return Math.toIntExact(container2.age - container1.age);
        }
  };
  
  private class ProcessorSharingMonitor extends Thread {
	  //Queue<ContainerId> processorSharingContainersList   = new LinkedList<ContainerId>();
	  //Container currentlyExecutingContainer;
	  java.util.Queue<ProcessorSharingContainer> currentlyExecutingContainers   = new PriorityQueue<>(16,oldestContainersAgeComparator);
	  java.util.Queue<ProcessorSharingContainer> suspendedContainers   = new PriorityQueue<>(16,youngestContainersAgeComparator);
	  Queue<ProcessorSharingContainer> containersToSuspendList   = new LinkedList<ProcessorSharingContainer>();
	  long processorSharingInterval;
      int fineGrainedMonitorInterval;
      Context context;
      boolean running;
      double minimumCpu;
      int minimumMemory;
      int maximumConcurrentContainers;
      int pendingSuspendUpdateRequestId;
      ProcessorSharingContainer pendingSuspendContainer;
      int pendingResumeUpdateRequestId;
      ProcessorSharingContainer pendingResumeContainer;
      boolean needToResume;
      AtomicInteger lastRequestID;
      
	  public ProcessorSharingMonitor(Context context, long processorSharingInterval, int fineGrainedMonitorInterval, int minimumMemory, double minimumCpu, int maximumConcurrentContainers) {
          LOG.info("PAMELA ProcessorSharingMonitor created");
	      this.processorSharingInterval = processorSharingInterval;
          this.context = context;
          this.running = true;
          this.fineGrainedMonitorInterval = fineGrainedMonitorInterval;
          this.minimumCpu = minimumCpu; //1;//0.001;
          this.minimumMemory = minimumMemory;
          this.maximumConcurrentContainers = maximumConcurrentContainers;
          this.pendingSuspendUpdateRequestId = -1;
          this.pendingSuspendContainer = null;
          this.pendingResumeUpdateRequestId = -1;
          this.pendingResumeContainer = null;
          this.needToResume = false;
          this.lastRequestID = new AtomicInteger(-1);
	  }
	  
	 /* @Override
	  public void run() {
		 LOG.info("PAMELA ProcessorSharingMonitor started running!");
	     while(running){
			 // NOTE: only to make it work, sleep for shorter periods of time then verify if the container is done
			 long leftProcessorSharingWindow = delay;
			 while(leftProcessorSharingWindow > 0 && running) {
				 try {			    
					synchronized(processorSharingContainersList){
					  LOG.info("PAMELA ProcessorSharingMonitor BEFORE leftProcessorSharingWindow "+leftProcessorSharingWindow+" currentlyExecutingContainer "+currentlyExecutingContainer + " number of containers here "+ processorSharingContainersList.size());	
					  // Previous Container has been suspended or Queue was empty now there's a container there
 					  if(currentlyExecutingContainer == null && processorSharingContainersList.size() > 0) {
 						 if (! context.getContainers().containsKey(processorSharingContainersList.peek())) {
 							LOG.info("PAMELA context container "+processorSharingContainersList.peek()+" does not exist anymore!!!! Getting the next one.");
 							processorSharingContainersList.poll();
 						 }

 						 Container chosenContainer = context.getContainers().get(processorSharingContainersList.poll());
						 for(ContainerId contId : processorSharingContainersList) {
						    LOG.info("PAMELA ProcessorSharingMonitor processorSharingContainersList after poll containerId "+contId);
						 }
						
						if(chosenContainer.getContainerState() == org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerState.RUNNING){
							currentlyExecutingContainer = chosenContainer;
							resumeContainer();
						    leftProcessorSharingWindow = delay;
						}
					  }
					
					  // currentlyExecutingContainer is DONE. Dont wait until PS window is done.
					  if (currentlyExecutingContainer != null && currentlyExecutingContainer.getContainerState() == org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerState.DONE){
						LOG.info("PAMELA currentlyExecutingContainer "+currentlyExecutingContainer.getContainerId()+" DONE. leftProcessorSharingWindow "+leftProcessorSharingWindow);
						currentlyExecutingContainer = null;
						leftProcessorSharingWindow = 0; //to force resuming if applicable
 					  }
					}
					  LOG.info("PAMELA ProcessorSharingMonitor AFTER leftProcessorSharingWindow "+leftProcessorSharingWindow+" currentlyExecutingContainer "+currentlyExecutingContainer);
					  if(leftProcessorSharingWindow > 0)
				        Thread.sleep(fineGrainedMonitorInterval);

				 } catch (InterruptedException e) {
				     e.printStackTrace();
				 }
				 leftProcessorSharingWindow -= fineGrainedMonitorInterval;
			 }

			// PROCESSOR SHARING STOPPING SWITCHING CONTAINERS 
			if(currentlyExecutingContainer != null)
			synchronized(processorSharingContainersList){
			  LOG.info("PAMELA ProcessorSharingMonitor finished PS window. currentlyExecutingContainer "+currentlyExecutingContainer.getContainerId());
			  // If only one container in node, dont suspend it just let it run
			  if (processorSharingContainersList.size() > 0) {
			  if (currentlyExecutingContainer.getContainerState() == org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerState.RUNNING) 
			      suspendContainer(currentlyExecutingContainer.getContainerId());
			  }
			}
	     }
	  }*/
	  
	  /*@Override
	  public void run() {
		 LOG.info("PAMELA ProcessorSharingMonitor started running!");
	     while(running){
			 // NOTE: only to make it work, sleep for shorter periods of time then verify if the container is done
			 long leftProcessorSharingWindow = delay;
			 while(leftProcessorSharingWindow > 0 && running) {
				 try {			    
					synchronized(currentlyExecutingContainers){
					  Iterator<ProcessorSharingContainer> currentlyExecutingContainersIterator = currentlyExecutingContainers.iterator();
                      while (currentlyExecutingContainersIterator.hasNext()) {
                    	  ProcessorSharingContainer psContainer = currentlyExecutingContainersIterator.next();
                    	  Container currentlyExecutingContainer = context.getContainers().get(psContainer.container.getContainerId());
					      if (currentlyExecutingContainer != null
					    		  && currentlyExecutingContainer.getContainerState() == org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerState.DONE){
						      currentlyExecutingContainers.remove(psContainer);
						      LOG.info("PAMELA ProcessorSharingMonitor cleaning up "+currentlyExecutingContainer.getContainerId()+" DONE. "+currentlyExecutingContainers.size()+" containers left running");						      
						      // TODO resume a container from the queue
 					      }
					      psContainer.container = currentlyExecutingContainer; //update container, it might have other states. Not sure if needed.
					      psContainer.updateAge();
					  }
                    }
			        Thread.sleep(fineGrainedMonitorInterval);
				 } catch (InterruptedException e) {
				     e.printStackTrace();
				 }
				 leftProcessorSharingWindow -= fineGrainedMonitorInterval;
			 }

	 		 LOG.info("PAMELA ProcessorSharingMonitor finished PS window. Checking if there are containersToSuspend "+ leftProcessorSharingWindow);
             synchronized(containersToSuspendList){
			     // Initializing containers that now need to be suspended
			     Iterator<ProcessorSharingContainer> containersToSuspendIterator = containersToSuspendList.iterator();
			     while(containersToSuspendIterator.hasNext()) {
				     ProcessorSharingContainer psContainerToSuspend = containersToSuspendIterator.next();
					 Container containerToSuspend = context.getContainers().get(psContainerToSuspend.container.getContainerId());
					 if(containerToSuspend != null 
							&& containerToSuspend.getContainerState() == org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerState.RUNNING ) {
	   		             LOG.info("PAMELA ProcessorSharingMonitor suspend container "+containerToSuspend.getContainerId()+" age "+psContainerToSuspend.age+" state "+containerToSuspend.getContainerState());
	   	        		 int updateRequestId = suspendContainer(containerToSuspend);
		   		         int suspended = containerToSuspend.getUpdateRequestResult(updateRequestId);
	   		             if(suspended == 0) {
		   					psContainerToSuspend.suspend();
	   		                suspendedContainers.add(psContainerToSuspend);
	   	        		    containersToSuspendList.remove(psContainerToSuspend);
	   		             } else {
	   		            	LOG.info("PAMELA ProcessorSharingMonitor NOT SUSPENDED container "+containerToSuspend.getContainerId()+" state "+containerToSuspend.getContainerState()+" started? "+ (suspended!=-1));
	   		            	continue;
	   		             }
					 } else {
						LOG.info("PAMELA ProcessorSharingMonitor NOT SUSPENDING container "+containerToSuspend.getContainerId()+" state "+containerToSuspend.getContainerState());
						continue;
					 }
			     }
	         } // synchronized(containersToSuspendList)

  			LOG.info("PAMELA ProcessorSharingMonitor Checking if there is something to suspend/resume leftProcessorSharingWindow"+ leftProcessorSharingWindow);
			// Pausing and resuming containers as needed 
			synchronized(currentlyExecutingContainers){		
				synchronized(suspendedContainers){	
				   Iterator<ProcessorSharingContainer> suspendedContainersIterator = suspendedContainers.iterator();			
				   while(suspendedContainersIterator.hasNext()) {
					  ProcessorSharingContainer psSuspendedContainer = suspendedContainersIterator.next();
					  Container chosenSuspendedContainer = context.getContainers().get(psSuspendedContainer.container.getContainerId());
			          // Check it is actually running (not initializing)
			          if (chosenSuspendedContainer != null && chosenSuspendedContainer.getContainerState() == org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerState.RUNNING) {
			        	if(currentlyExecutingContainers.size()>0) {
			        	  ProcessorSharingContainer oldestCurrentlyExecutingContainer = currentlyExecutingContainers.peek();
			        	  LOG.info("PAMELA ProcessorSharingMonitor oldestCurrentlyExecutingContainer is null? "+oldestCurrentlyExecutingContainer.container==null);
			        	  Container oldestExecutingContainer = context.getContainers().get(oldestCurrentlyExecutingContainer.container.getContainerId());		        	  
			        	  if(oldestExecutingContainer != null && oldestExecutingContainer.getContainerState() == org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerState.RUNNING) {
			        	   oldestCurrentlyExecutingContainer.container = oldestExecutingContainer; // Just in case, dont know if we need this
			        	   if((psSuspendedContainer.age - oldestCurrentlyExecutingContainer.age)<delay)//(oldestCurrentlyExecutingContainer.age > psSuspendedContainer.age)
			        	   {
				              LOG.info("PAMELA ProcessorSharingMonitor run() going to suspend container "+oldestExecutingContainer.getContainerId()+" age "+oldestCurrentlyExecutingContainer.age);
				              int updateRequestId = suspendContainer(oldestExecutingContainer);
				   		      int suspended = oldestExecutingContainer.getUpdateRequestResult(updateRequestId);
				              if (suspended == 0) {
			        		     suspendedContainers.add(oldestCurrentlyExecutingContainer);
				        		 currentlyExecutingContainers.remove(oldestCurrentlyExecutingContainer);
				        		 oldestCurrentlyExecutingContainer.suspend();
				              } else {
			   		            	LOG.info("PAMELA ProcessorSharingMonitor NOT SUSPENDED container "+oldestExecutingContainer.getContainerId()+" state "+oldestExecutingContainer.getContainerState()+" started? "+ (suspended!=-1));
			   		            	//TODO put a flag for the other container to resume when ready, flag should include requestId pendingToResumeRequestId
			   		            	continue; //while(suspendedContainersIterator.hasNext()) 
			   		          }
			        	   } else {
			        		   LOG.info("PAMELA ProcessorSharingMonitor NOT SUSPENDING: oldest concurrently executing container age "+ oldestCurrentlyExecutingContainer.age +" youngest suspended container age "+ psSuspendedContainer.age);
			        		   break;//while(suspendedContainersIterator.hasNext()) 
			        	   }
			        	  } else {
			        		  LOG.info("PAMELA ProcessorSharingMonitor oldestExecutingContainer null? "+ oldestExecutingContainer ==null+" or state not running");
			        	  }
			        	 }  else {
			        		 LOG.info("PAMELA ProcessorSharingMonitor NOTHING TO SUSPEND? currentlyExecutingContainers == 0, but suspended containers >0 Container probably finished");			        		 
			        	 }
			        	
			        	if (currentlyExecutingContainers.size() < maximumConcurrentContainers) {
			        		int resumed = resumeContainer(chosenSuspendedContainer, chosenSuspendedContainer.getResource());
			        		if (resumed == 0) {
					             // Resume chosen suspended container
					        	 suspendedContainers.remove(psSuspendedContainer);
					        	 psSuspendedContainer.resume();
					        	 currentlyExecutingContainers.add(psSuspendedContainer);
			        		}else {
		   		            	LOG.info("PAMELA ProcessorSharingMonitor NOT RESUMED YET container "+chosenSuspendedContainer.getContainerId()+" state "+chosenSuspendedContainer.getContainerState()+" started? "+ (resumed!=-1));
		   		          }
			        	} else {
			        		LOG.info("PAMELA ProcessorSharingMonitor NOT RESUMING container "+chosenSuspendedContainer.getContainerId()+ " because currentlyExecutingContainers.size() "+ currentlyExecutingContainers.size()+ " and maximumConcurrentContainers "+ maximumConcurrentContainers);
			        	}
			        	} else {
			        		LOG.info("PAMELA ProcessorSharingMonitor suspended container "+psSuspendedContainer.container.getContainerId()+" NULL or NOT RUNNING? going to remove from suspendedContainers? "+(chosenSuspendedContainer == null|| chosenSuspendedContainer.getContainerState() == org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerState.DONE));
			        		if (chosenSuspendedContainer == null || chosenSuspendedContainer.getContainerState() == org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerState.DONE) {
			        			LOG.info("PAMELA ProcessorSharingMonitor suspended container "+psSuspendedContainer.container.getContainerId()+" DONE. Cleaing up.");
 			        		    suspendedContainers.remove(psSuspendedContainer);
			        		}
			        	}
			    } //while(suspendedContainersIterator.hasNext())
			   
                synchronized(suspendedContainers) {
				   //TODO update this!!
				   if (suspendedContainers.size() > 0) {
	        	     int youngestSuspendedContainersAge = (int) suspendedContainers.peek().age;
	        	     updateOldestYoungestAge(youngestSuspendedContainersAge);
				     LOG.info("PAMELA ProcessorSharingMonitor DEBUG suspended youngest age "+youngestSuspendedContainersAge);
				   }
					 //DEBUG
				   Iterator<ProcessorSharingContainer> debugIterator = suspendedContainers.iterator();			
				   while(debugIterator.hasNext()) {
					   ProcessorSharingContainer debugContainer = debugIterator.next();
					   LOG.info("PAMELA ProcessorSharingMonitor DEBUG suspended "+debugContainer.container.getContainerId()+" age "+debugContainer.age);
				   }
                }
                synchronized (currentlyExecutingContainers) {
                   Iterator<ProcessorSharingContainer> debugIterator = currentlyExecutingContainers.iterator();			
				   while(debugIterator.hasNext()) {
					   ProcessorSharingContainer debugContainer = debugIterator.next();
					   LOG.info("PAMELA ProcessorSharingMonitor DEBUG currently executing "+debugContainer.container.getContainerId()+" age "+debugContainer.age);
				   }
   				}

			    }//synchronized(suspendedContainers)
			  }//synchronized(currentlyExecutingContainers)
		}//while(running)
	  }//public run()
 */

	  private void printContainers(Queue<ProcessorSharingContainer> containers, String typeContainers) {
		  synchronized (containers) {				
			Iterator<ProcessorSharingContainer> iterator = containers.iterator();
			while(iterator.hasNext()) {
				ProcessorSharingContainer iContainer = iterator.next();
				LOG.info("PAMELA ProcessorSharingMonitor DEBUG "+typeContainers+" "+iContainer.container.getContainerId()+" "+ iContainer.container.getContainerState()+" age "+ iContainer.age);
			}
			}
	  }
	  
 	 private int additionalSleepForLaunching;
	  @Override
	  public void run() {
		 LOG.info("PAMELA ProcessorSharingMonitor started running!");
		 long timeLeftProcessorSharingInterval = this.processorSharingInterval;
		 int delay = -1;
		 boolean resumeExtra = false;
	     while(running){
	    	 // Sleep first
	    	 try {
		        Thread.sleep(fineGrainedMonitorInterval);
		    	//LOG.info("PAMELA ProcessorSharingMonitor DEBUG additionalSleepForLaunching "+ additionalSleepForLaunching); 
		        Thread.sleep(additionalSleepForLaunching);
			 } catch (InterruptedException e) {
			     e.printStackTrace();
			 }
	    	 
	    	 resumeExtra = cleanupFinishingContainers(resumeExtra);
	    	 
	    	 timeLeftProcessorSharingInterval -= fineGrainedMonitorInterval;
	    	 if (delay >=0)
	    		 delay += fineGrainedMonitorInterval;
	    	 
	    	 LOG.info("PAMELA ProcessorSharingMonitor DEBUG timeLeftPSInterval "+ timeLeftProcessorSharingInterval 
	    			 + " pendingSuspend "+ (pendingSuspendContainer != null? pendingSuspendContainer.container.getContainerId(): " none")
	    			 + " suspendRequestId "+ pendingSuspendUpdateRequestId
	    			 + " pendingResume "+(pendingResumeContainer != null? pendingResumeContainer.container.getContainerId(): " none")
	    			 + " resumeRequestId "+pendingResumeUpdateRequestId);

	    	 printContainers(currentlyExecutingContainers,"BEFORE currentlyExecuting");
	    	 printContainers(suspendedContainers,"BEFORE suspended");
	    	 printContainers(containersToSuspendList,"BEFORE containerToSuspend");
	    	 
	    	 //To check what happens when adding a new container
	    	 synchronized(containersToSuspendList) {
	    		 if(containersToSuspendList.size()>0 && pendingSuspendUpdateRequestId == -1) { //TODO case where we have more than one
		    		 LOG.info("PAMELA ProcessorSharingMonitor suspend after adding, additionalSleepForLaunching "+additionalSleepForLaunching);
		    	     // Try to suspend from toSuspendList
	    			 ProcessorSharingContainer pendingSuspend = containersToSuspendList.poll();
	    			 assert(!containerExiting(pendingSuspend.container.getContainerState()));
					 if(pendingSuspend.container.getWasLaunched() && !containerExiting(pendingSuspend.container.getContainerState())) {
		    			 pendingSuspendUpdateRequestId = suspendContainer(pendingSuspend.container);
		    			 additionalSleepForLaunching = 0;
		    			 pendingSuspendContainer = pendingSuspend;
		    			 needToResume = false;
					 } else {
					    LOG.info("PAMELA ProcessorSharingMonitor "+ pendingSuspend.container.getContainerId()+" not yet launched, state "+pendingSuspend.container.getContainerState());
					    additionalSleepForLaunching = 1000;
					    containersToSuspendList.add(pendingSuspend);
					 }
		    	     continue; // Skip the rest and go to sleep
		    	 }
	    	 }
	    	 //TODO ages!
	    	 // Check if pendingSuspend finished
	    	 if (pendingSuspendUpdateRequestId != -1) {
	    		 LOG.info("PAMELA ProcessorSharingMonitor checking pending suspend "+pendingSuspendUpdateRequestId);
	    		 assert(pendingResumeUpdateRequestId == -1);
	    		 // TODO maybe do the same as pending resume in case container is null 
	    		 int statusPendingSuspend = pendingSuspendContainer.container.getUpdateRequestResult(pendingSuspendUpdateRequestId);
	    		 if (statusPendingSuspend > -1) {
	    			 if (statusPendingSuspend == 0) { //SUCCEEDED SUSPENDING
						// Cleanup data structures
		    			LOG.info("PAMELA ProcessorSharingMonitor FINISHED suspending "+pendingSuspendContainer.container.getContainerId()+" suspendRequestId "+pendingSuspendUpdateRequestId+" needToResume "+needToResume);
		    			ProcessorSharingContainer finishedSuspendingContainer = new ProcessorSharingContainer(pendingSuspendContainer);
						pendingSuspendContainer.suspend();
						pendingSuspendUpdateRequestId = -1;
						pendingSuspendContainer = null;
	    				if(needToResume) {
	    					ProcessorSharingContainer resumeContainer = suspendedContainers.poll();
    					   //RESUME suspendedContainer
			        	   pendingResumeUpdateRequestId = resumeContainer(resumeContainer.container, resumeContainer.container.getResource());
			        	   pendingResumeContainer = resumeContainer;
	    				 } else {
	    					 timeLeftProcessorSharingInterval = processorSharingInterval;
	    				 }
						suspendedContainers.add(finishedSuspendingContainer);

	    			 } else { 
	    				if (!needToResume) { // it was a container suspended after adding another
	    					LOG.info("PAMELA ProcessorSharingMonitor "+ pendingSuspendContainer.container.getContainerId()+" SUSPEND FAILED going to retry");
	    					synchronized (containersToSuspendList) {
							   containersToSuspendList.add(pendingSuspendContainer);
							   additionalSleepForLaunching = 1000;
							   pendingSuspendContainer = null;
							   pendingSuspendUpdateRequestId = -1;
							}
	    				} else {// failed!!!
	    					LOG.info("PAMELA ProcessorSharingMonitor "+ pendingSuspendContainer.container.getContainerId()+" SUSPEND FAILED ignore it and hope nothing crashes");
						   pendingSuspendContainer = null;
						   pendingSuspendUpdateRequestId = -1;	    					
	    				}	    				
	    			 }//if (statusPendingSuspend == 0) 
	    		 }
	    		continue;
	    	 }
	    	 
	    	 // Check if pendingResume finished
	    	 if (pendingResumeUpdateRequestId != -1) {
	    		 LOG.info("PAMELA ProcessorSharingMonitor checking pending resume "+pendingResumeUpdateRequestId);
	    		 assert(pendingSuspendUpdateRequestId == -1);
	    		 assert(pendingResumeContainer.container != null);
	    		 int statusPendingResume = pendingResumeContainer.container.getUpdateRequestResult(pendingResumeUpdateRequestId);
	    		 if (statusPendingResume > -1) {
	    			 if (statusPendingResume == 0) {
		    			 // Cleanup data structures	    	    		  
  		    			 LOG.info("PAMELA ProcessorSharingMonitor FINISHED resuming " + pendingResumeContainer.container.getContainerId() +" resumeRequestId "+pendingResumeUpdateRequestId+" total PS delay "+delay);
		    			 pendingResumeContainer.resume();
		    			 currentlyExecutingContainers.add(pendingResumeContainer);
    					 pendingResumeContainer = null;
			        	 pendingResumeUpdateRequestId = -1;

		    			 // RESET time counters
		    			 timeLeftProcessorSharingInterval = processorSharingInterval;
		    			 delay = -1;
	    			 } //else failed!!!
	    		 } 
	    		 continue;
	    	 }
	    	 
	    	 // PS window finished
	    	 if (timeLeftProcessorSharingInterval == 0) {
	    	   if(suspendedContainers.size() > 0) {
	    		 assert(containersToSuspendList.size() == 0);
	    		 // SUSPEND executing container
			     if(currentlyExecutingContainers.size() > 0) {
			    	 ProcessorSharingContainer suspendContainer = currentlyExecutingContainers.poll();
				     assert(!containerExiting(suspendContainer.container.getContainerState()));
			         LOG.info("PAMELA ProcessorSharingMonitor STARTING PS "+suspendContainer.container.getContainerId()+" age "+suspendContainer.age+" state "+suspendContainer.container.getContainerState());
	        		 pendingSuspendUpdateRequestId = suspendContainer(suspendContainer.container);
	        		 pendingSuspendContainer = suspendContainer;
	        		 needToResume = true;
			     } else { // else then resume 
			    	 //TODO check for multiple containers at once 
 				   ProcessorSharingContainer resumeContainer = suspendedContainers.poll();
			       LOG.info("PAMELA ProcessorSharingMonitor FINISHED PS nothing to suspend, resuming container "+resumeContainer.container.getContainerId());
				   //RESUME suspendedContainer
	        	   pendingResumeUpdateRequestId = resumeContainer(resumeContainer.container, resumeContainer.container.getResource());
	        	   pendingResumeContainer = resumeContainer;
				   delay = 0;
		    	   needToResume = false;
			     } 
	    	   } else {
	    		   timeLeftProcessorSharingInterval = this.processorSharingInterval;
	    	   }
	    	 }//if (timeLeftProcessorSharingInterval == 0)

	    	 if(resumeExtra && pendingResumeUpdateRequestId == -1 && suspendedContainers.size() > 0) { //apparently we need to resume another container?
	    		 LOG.info("PAMELA ProcessorSharingMonitor RESUME EXTRA apparently because of cleaning");
				ProcessorSharingContainer resumeContainer = suspendedContainers.poll();
				assert(!containerExiting(resumeContainer.container.getContainerState()));
				//RESUME suspendedContainer
	        	pendingResumeUpdateRequestId = resumeContainer(resumeContainer.container, resumeContainer.container.getResource());
	        	pendingResumeContainer = resumeContainer;
	        	delay = 0;
	        	resumeExtra = false;
	    	 } else if (resumeExtra && suspendedContainers.size() > 0){ //there's some other pending resume
	    		 LOG.info("PAMELA ProcessorSharingMonitor shouldnt come here.");
	    	 } else
	    		 resumeExtra = false; //suspendedContainers empty

	    	 printContainers(currentlyExecutingContainers,"AFTER currentlyExecuting");
	    	 printContainers(suspendedContainers,"AFTER suspended");
	    	 printContainers(containersToSuspendList,"AFTER containerToSuspend");
	     }
	  }
	  
	private boolean cleanupFinishingContainers(boolean resumeExtra) {
		boolean resume = false;
		Iterator<ProcessorSharingContainer> iterator = null;
		synchronized (currentlyExecutingContainers) {				
			iterator = currentlyExecutingContainers.iterator();
			while(iterator.hasNext()) {
				ProcessorSharingContainer iContainer = iterator.next();
				iContainer.updateAge();
				if(iContainer.container == null || containerExiting(iContainer.container.getContainerState())) {
				   LOG.info("PAMELA ProcessorSharingMonitor CLEANUP currentlyExecutingContainer "+iContainer.container.getContainerId()+" "+ iContainer.container.getContainerState()+" age "+ iContainer.age);
				   currentlyExecutingContainers.remove(iContainer);
				   resume = true;
				}
			}
		}
		iterator = suspendedContainers.iterator();
		while(iterator.hasNext()) {
			ProcessorSharingContainer iContainer = iterator.next();
			if(iContainer.container == null || containerExiting(iContainer.container.getContainerState())) {
			   LOG.info("PAMELA ProcessorSharingMonitor CLEANUP suspendedContainers "+iContainer.container.getContainerId()+" "+ iContainer.container.getContainerState()+" age "+ iContainer.age);
			   suspendedContainers.remove(iContainer);
			}
		}

		synchronized (containersToSuspendList) {				
			iterator = containersToSuspendList.iterator();
			while(iterator.hasNext()) {
				ProcessorSharingContainer iContainer = iterator.next();
				if(iContainer.container == null || containerExiting(iContainer.container.getContainerState())) {
				   LOG.info("PAMELA ProcessorSharingMonitor CLEANUP containersToSuspendList "+iContainer.container.getContainerId()+" "+ iContainer.container.getContainerState()+" age "+ iContainer.age);
				   containersToSuspendList.remove(iContainer);
				   additionalSleepForLaunching = 0;
				}
			}
		}
		
		//pending suspend
		if(pendingSuspendUpdateRequestId != -1) {
			if(pendingSuspendContainer.container == null || containerExiting(pendingSuspendContainer.container.getContainerState())) {
				LOG.info("PAMELA ProcessorSharingMonitor CLEANUP suspendedContainers "+pendingSuspendContainer.container.getContainerId());
				pendingSuspendUpdateRequestId = -1;
				pendingSuspendContainer = null;
				resume = true;
			}
		}
		
		//pending resume
		if(pendingResumeUpdateRequestId != -1) {
			if(pendingResumeContainer.container == null || containerExiting(pendingResumeContainer.container.getContainerState())) {
				LOG.info("PAMELA ProcessorSharingMonitor CLEANUP resumeContainers "+pendingResumeContainer.container.getContainerId());
				pendingResumeUpdateRequestId = -1;
				pendingResumeContainer = null;
				resume = true;
			}
		}
		return resume || resumeExtra;
	}

	private boolean containerExiting(
			org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerState containerState) {
		// if state not killing or done or exited with success or exited with failure?
		boolean exiting =(containerState == org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerState.KILLING
				|| containerState == org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerState.DONE
				|| containerState == org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerState.EXITED_WITH_SUCCESS
				|| containerState == org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerState.EXITED_WITH_FAILURE);
		//LOG.info("PAMELA ProcessorSharingMonitor "+ containerState+" containerExiting? "+exiting); 
		return exiting;
	}

	private int resumeContainer(Container containerToResume, Resource resources) {
	        //XXX RESUME
		    int updateRequestId = lastRequestID.incrementAndGet();
	        LOG.info("PAMELA ProcessorSharingMonitor RESUMING "+containerToResume.getContainerId() +" updateRequestId "+ updateRequestId +" STATE "+containerToResume.getContainerState() +" with resources "+resources);	        
		    NodeContainerUpdate nodeContainerUpdate = NodeContainerUpdate.newInstance(containerToResume.getContainerId(),	resources.getMemory(), resources.getVirtualCores(), false, true, updateRequestId);
		    containerToResume.handle(new ContainerResourceUpdate(containerToResume.getContainerId(),nodeContainerUpdate));
		    return updateRequestId;
	  }
	  
	  private int suspendContainer(Container containerToSuspend) {
	      //XXX SUSPEND 
		  int updateRequestId = lastRequestID.incrementAndGet();
		  LOG.info("PAMELA ProcessorSharingMonitor SUSPENDING "+containerToSuspend.getContainerId()  +" updateRequestId "+ updateRequestId +" STATE "+containerToSuspend.getContainerState()+" to "+ minimumMemory + "m memory and "+minimumCpu+" of cpu");
	      NodeContainerUpdate nodeContainerUpdate = NodeContainerUpdate.newInstance(containerToSuspend.getContainerId(), minimumMemory, minimumCpu, true, false, updateRequestId); // -1 means no change to memory -- for now
	      containerToSuspend.handle(new ContainerResourceUpdate(containerToSuspend.getContainerId(),nodeContainerUpdate));
	      return updateRequestId;
	  }
	  
	/*  public void addContainer(ContainerId containerId){
		  synchronized(currentlyExecutingContainers){		  
	    	 if(currentlyExecutingContainers.size() >= maximumConcurrentContainers) {
	    		 // suspend OLDEST currently executing container
	    		 //oldest should be running, otherwise means that it is initializing or finishing, in that case dont touch it.
	    		 ProcessorSharingContainer oldestCurrentlyExecutingContainer = currentlyExecutingContainers.peek();
	    		 if(oldestCurrentlyExecutingContainer.container.getContainerState() == org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerState.RUNNING
	    				 && context.getContainers().containsKey(oldestCurrentlyExecutingContainer.container.getContainerId())) {
	                 LOG.info("PAMELA ProcessorSharingMonitor going to suspend container "+oldestCurrentlyExecutingContainer.container.getContainerId()+" age "+oldestCurrentlyExecutingContainer.age);
	                 int suspended = suspendContainer(oldestCurrentlyExecutingContainer.container);
	                 if(suspended == 0) {
	      	   		   synchronized(suspendedContainers){
		    		     // Put to queue
		    		     oldestCurrentlyExecutingContainer.suspend();
		    		     suspendedContainers.add(oldestCurrentlyExecutingContainer);
		    			 currentlyExecutingContainers.remove(oldestCurrentlyExecutingContainer);
		    		     int youngestSuspendedContainersAge = (int) suspendedContainers.peek().age;
		    		     updateOldestYoungestAge(youngestSuspendedContainersAge);
						 LOG.info("PAMELA ProcessorSharingMonitor DEBUG suspended youngest age "+youngestSuspendedContainersAge);
	      	   		   }//synchronized(suspendedContainers)
	                 } else {
	                	 LOG.info("PAMELA ProcessorSharingMonitor COULDNT SUSPEND container "+oldestCurrentlyExecutingContainer.container.getContainerId()+" suspended "+suspended);
	                 }
	    		 } else { //oldestCurrentlyExecutingContainer is running
	    			 if(!context.getContainers().containsKey(oldestCurrentlyExecutingContainer.container.getContainerId())) {
	    				 LOG.info("PAMELA ProcessorSharingMonitor for some reason container "+oldestCurrentlyExecutingContainer.container.getContainerId()+" NOT in context");
		    			 currentlyExecutingContainers.remove(oldestCurrentlyExecutingContainer);
	    			 }else {
	    			   synchronized(containersToSuspendList){
	    				 LOG.info("PAMELA ProcessorSharingMonitor container "+oldestCurrentlyExecutingContainer.container.getContainerId()+" NOT RUNNING, state "+oldestCurrentlyExecutingContainer.container.getContainerState()+ " adding it to containersToSuspendList");
	    				 containersToSuspendList.add(oldestCurrentlyExecutingContainer);
	    				 currentlyExecutingContainers.remove(oldestCurrentlyExecutingContainer);
	    				}
	    			 }
		   			 //Try to suspend the one after
		   			 //TODO if it will run no mather what.... suspend any container in a loop
		   			 // put in a list of pendingsuspensionrequests
		   		 }

	    	 }	//if(currentlyExecutingContainers.size() >= maximumConcurrentContainers)     	 

	    	 //add currentlyExecutingContainer
   		     Container newlyAddedContainer = this.context.getContainers().get(containerId);
   		     currentlyExecutingContainers.add(new ProcessorSharingContainer(newlyAddedContainer));
             LOG.info("PAMELA ProcessorSharingMonitor adding container "+newlyAddedContainer.getContainerId()+" new size "+ currentlyExecutingContainers.size());
	     }//synchronized(currentlyExecutingContainers)
	  }*/
	  public void addContainer(ContainerId containerId){
		  synchronized(currentlyExecutingContainers){		  
	    	 if(currentlyExecutingContainers.size() >= maximumConcurrentContainers) {
	    	   ProcessorSharingContainer oldestCurrentlyExecutingContainer = currentlyExecutingContainers.poll();
			   LOG.info("PAMELA ProcessorSharingMonitor "+oldestCurrentlyExecutingContainer.container.getContainerId()+ " added to containersToSuspendList");
			   synchronized(containersToSuspendList){
				   if(!containersToSuspendList.contains(oldestCurrentlyExecutingContainer)) {
				       additionalSleepForLaunching = 3000;
				       containersToSuspendList.add(oldestCurrentlyExecutingContainer);
				   }
			   }
	    	 }	//if(currentlyExecutingContainers.size() >= maximumConcurrentContainers)     	 

	    	 //add currentlyExecutingContainer
   		     Container newlyAddedContainer = this.context.getContainers().get(containerId);
   		     currentlyExecutingContainers.add(new ProcessorSharingContainer(newlyAddedContainer));
             LOG.info("PAMELA ProcessorSharingMonitor new container "+newlyAddedContainer.getContainerId()+" currentlyExecutingContainers "+ currentlyExecutingContainers.size());
	     }//synchronized(currentlyExecutingContainers)
	  }
          
	   public void stopRunning() {
		   this.running = false;
	   }
  }

  class ContainerEventDispatcher implements EventHandler<ContainerEvent> {
    @Override
    public void handle(ContainerEvent event) {
      Map<ContainerId,Container> containers =
        ContainerManagerImpl.this.context.getContainers();
      //LOG.info("PAMELA ContainerEventDispatcher handling event "+event.getType()+" total number of containers in this node? "+containers.size());
      Container c = containers.get(event.getContainerID());
      if (c != null) {
        c.handle(event);
      } else {
        LOG.warn("Event " + event + " sent to absent container " +
            event.getContainerID());
      }
    }
  }

  class ApplicationEventDispatcher implements EventHandler<ApplicationEvent> {

    @Override
    public void handle(ApplicationEvent event) {
      Application app =
          ContainerManagerImpl.this.context.getApplications().get(
              event.getApplicationID());
      if (app != null) {
        app.handle(event);
      } else {
        LOG.warn("Event " + event + " sent to absent application "
            + event.getApplicationID());
      }
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public void handle(ContainerManagerEvent event) {
    switch (event.getType()) {
    case FINISH_APPS:
      CMgrCompletedAppsEvent appsFinishedEvent =
          (CMgrCompletedAppsEvent) event;
      for (ApplicationId appID : appsFinishedEvent.getAppsToCleanup()) {
        String diagnostic = "";
        if (appsFinishedEvent.getReason() == CMgrCompletedAppsEvent.Reason.ON_SHUTDOWN) {
          diagnostic = "Application killed on shutdown";
        } else if (appsFinishedEvent.getReason() == CMgrCompletedAppsEvent.Reason.BY_RESOURCEMANAGER) {
          diagnostic = "Application killed by ResourceManager";
        }
        try {
          this.context.getNMStateStore().storeFinishedApplication(appID);
        } catch (IOException e) {
          LOG.error("Unable to update application state in store", e);
        }
        this.dispatcher.getEventHandler().handle(
            new ApplicationFinishEvent(appID,
                diagnostic));
      }
      break;
    case FINISH_CONTAINERS:
      CMgrCompletedContainersEvent containersFinishedEvent =
          (CMgrCompletedContainersEvent) event;
      for (ContainerId container : containersFinishedEvent
          .getContainersToCleanup()) {
          this.dispatcher.getEventHandler().handle(
              new ContainerKillEvent(container,
                  ContainerExitStatus.KILLED_BY_RESOURCEMANAGER,
                  "Container Killed by ResourceManager"));
      }
      break;
    case UPDATE_CONTAINERS:
    	LOG.info("get containerUpdateEvents");
    	CMgrUpdateContainersEvent containerUpdateEvents =
    			(CMgrUpdateContainersEvent) event;
    	for(NodeContainerUpdate containerUpdate : containerUpdateEvents.getNodeContainerUpdate()){            
        	LOG.info("PAMELA UPDATE_CONTAINERS eventhandler is "+this.dispatcher.getEventHandler().toString()+" dispatcher is "+this.dispatcher.getName());
    		this.dispatcher.getEventHandler().handle(
    			new ContainerResourceUpdate(containerUpdate.getContainerId(),containerUpdate));
    	}
    	break;
    default:
        throw new YarnRuntimeException(
            "Got an unknown ContainerManagerEvent type: " + event.getType());
    }
  }

  public void setBlockNewContainerRequests(boolean blockNewContainerRequests) {
    this.blockNewContainerRequests.set(blockNewContainerRequests);
  }

  @Private
  @VisibleForTesting
  public boolean getBlockNewContainerRequestsStatus() {
    return this.blockNewContainerRequests.get();
  }
  
  @Override
  public void stateChanged(Service service) {
    // TODO Auto-generated method stub
  }
  
  public Context getContext() {
    return this.context;
  }

  public Map<String, ByteBuffer> getAuxServiceMetaData() {
    return this.auxiliaryServices.getMetaData();
  }
}
