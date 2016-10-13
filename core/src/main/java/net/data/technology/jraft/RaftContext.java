/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  The ASF licenses 
 * this file to you under the Apache License, Version 2.0 (the
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

package net.data.technology.jraft;

import java.util.concurrent.ScheduledThreadPoolExecutor;

public class RaftContext {

    private ServerStateManager serverStateManager;
    private RpcListener rpcListener;
    private LoggerFactory loggerFactory;
    private RpcClientFactory rpcClientFactory;
    private StateMachine stateMachine;
    private RaftParameters raftParameters;
    private ScheduledThreadPoolExecutor scheduledExecutor;

    public RaftContext(ServerStateManager stateManager, StateMachine stateMachine, RaftParameters raftParameters, RpcListener rpcListener, LoggerFactory logFactory, RpcClientFactory rpcClientFactory){
        this(stateManager, stateMachine, raftParameters, rpcListener, logFactory, rpcClientFactory, null);
    }

    public RaftContext(ServerStateManager stateManager, StateMachine stateMachine, RaftParameters raftParameters, RpcListener rpcListener, LoggerFactory logFactory, RpcClientFactory rpcClientFactory, ScheduledThreadPoolExecutor scheduledExecutor){
        this.serverStateManager = stateManager;
        this.stateMachine = stateMachine;
        this.raftParameters = raftParameters;
        this.rpcClientFactory = rpcClientFactory;
        this.rpcListener = rpcListener;
        this.loggerFactory = logFactory;
        this.scheduledExecutor = scheduledExecutor;
        if(this.scheduledExecutor == null){
            this.scheduledExecutor = new ScheduledThreadPoolExecutor(Runtime.getRuntime().availableProcessors());
        }

        if(this.raftParameters == null){
            this.raftParameters = new RaftParameters()
                    .withElectionTimeoutUpper(300)
                    .withElectionTimeoutLower(150)
                    .withHeartbeatInterval(75)
                    .withRpcFailureBackoff(25)
                    .withMaximumAppendingSize(100)
                    .withLogSyncBatchSize(1000)
                    .withLogSyncStoppingGap(100)
                    .withSnapshotEnabled(0)
                    .withSyncSnapshotBlockSize(0);
        }
    }

    public ServerStateManager getServerStateManager() {
        return serverStateManager;
    }

    public RpcListener getRpcListener() {
        return rpcListener;
    }

    public LoggerFactory getLoggerFactory() {
        return loggerFactory;
    }

    public RpcClientFactory getRpcClientFactory() {
        return rpcClientFactory;
    }

    public StateMachine getStateMachine() {
        return stateMachine;
    }

    public RaftParameters getRaftParameters() {
        return raftParameters;
    }

    public ScheduledThreadPoolExecutor getScheduledExecutor(){
        return this.scheduledExecutor;
    }
}
