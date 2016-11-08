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

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * Peer server in the same cluster for local server
 * this represents a peer for local server, it could be a leader, however, if local server is not a leader, though it has a list of peer servers, they are not used
 * @author Data Technology LLC
 *
 */
public class PeerServer {

    private ClusterServer clusterConfig;
    private RpcClient rpcClient;
    private int currentHeartbeatInterval;
    private int heartbeatInterval;
    private int rpcBackoffInterval;
    private int maxHeartbeatInterval;
    private AtomicInteger busyFlag;
    private AtomicInteger pendingCommitFlag;
    private Callable<Void> heartbeatTimeoutHandler;
    private ScheduledFuture<?> heartbeatTask;
    private long nextLogIndex;
    private long matchedIndex;
    private boolean heartbeatEnabled;
    private SnapshotSyncContext snapshotSyncContext;
    private Executor executor;

    public PeerServer(ClusterServer server, RaftContext context, final Consumer<PeerServer> heartbeatConsumer){
        this.clusterConfig = server;
        this.rpcClient = context.getRpcClientFactory().createRpcClient(server.getEndpoint());
        this.busyFlag = new AtomicInteger(0);
        this.pendingCommitFlag = new AtomicInteger(0);
        this.heartbeatInterval = this.currentHeartbeatInterval = context.getRaftParameters().getHeartbeatInterval();
        this.maxHeartbeatInterval = context.getRaftParameters().getMaxHeartbeatInterval();
        this.rpcBackoffInterval = context.getRaftParameters().getRpcFailureBackoff();
        this.heartbeatTask = null;
        this.snapshotSyncContext = null;
        this.nextLogIndex = 1;
        this.matchedIndex = 0;
        this.heartbeatEnabled = false;
        this.executor = context.getScheduledExecutor();
        PeerServer self = this;
        this.heartbeatTimeoutHandler = new Callable<Void>(){

            @Override
            public Void call() throws Exception {
                heartbeatConsumer.accept(self);
                return null;
            }};
    }

    public int getId(){
        return this.clusterConfig.getId();
    }

    public ClusterServer getClusterConfig(){
        return this.clusterConfig;
    }

    public Callable<Void> getHeartbeartHandler(){
        return this.heartbeatTimeoutHandler;
    }

    public synchronized int getCurrentHeartbeatInterval(){
        return this.currentHeartbeatInterval;
    }

    public void setHeartbeatTask(ScheduledFuture<?> heartbeatTask){
        this.heartbeatTask = heartbeatTask;
    }

    public ScheduledFuture<?> getHeartbeatTask(){
        return this.heartbeatTask;
    }

    public boolean makeBusy(){
        return this.busyFlag.compareAndSet(0, 1);
    }

    public void setFree(){
        this.busyFlag.set(0);
    }

    public boolean isHeartbeatEnabled(){
        return this.heartbeatEnabled;
    }

    public void enableHeartbeat(boolean enable){
        this.heartbeatEnabled = enable;

        if(!enable){
            this.heartbeatTask = null;
        }
    }

    public long getNextLogIndex() {
        return nextLogIndex;
    }

    public void setNextLogIndex(long nextLogIndex) {
        this.nextLogIndex = nextLogIndex;
    }

    public long getMatchedIndex(){
        return this.matchedIndex;
    }

    public void setMatchedIndex(long matchedIndex){
        this.matchedIndex = matchedIndex;
    }

    public void setPendingCommit(){
        this.pendingCommitFlag.set(1);
    }

    public boolean clearPendingCommit(){
        return this.pendingCommitFlag.compareAndSet(1, 0);
    }

    public void setSnapshotInSync(Snapshot snapshot){
        if(snapshot == null){
            this.snapshotSyncContext = null;
        }else{
            this.snapshotSyncContext = new SnapshotSyncContext(snapshot);
        }
    }

    public SnapshotSyncContext getSnapshotSyncContext(){
        return this.snapshotSyncContext;
    }

    public CompletableFuture<RaftResponseMessage> SendRequest(RaftRequestMessage request){
        boolean isAppendRequest = request.getMessageType() == RaftMessageType.AppendEntriesRequest || request.getMessageType() == RaftMessageType.InstallSnapshotRequest;
        return this.rpcClient.send(request)
                .thenComposeAsync((RaftResponseMessage response) -> {
                    if(isAppendRequest){
                        this.setFree();
                    }

                    this.resumeHeartbeatingSpeed();
                    return CompletableFuture.completedFuture(response);
                }, this.executor)
                .exceptionally((Throwable error) -> {
                    if(isAppendRequest){
                        this.setFree();
                    }

                    this.slowDownHeartbeating();
                    throw new RpcException(error, request);
                });
    }

    public synchronized void slowDownHeartbeating(){
        this.currentHeartbeatInterval = Math.min(this.maxHeartbeatInterval, this.currentHeartbeatInterval + this.rpcBackoffInterval);
    }

    public synchronized void resumeHeartbeatingSpeed(){
        if(this.currentHeartbeatInterval > this.heartbeatInterval){
            this.currentHeartbeatInterval = this.heartbeatInterval;
        }
    }
}
