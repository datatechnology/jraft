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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class RaftServer implements RaftMessageHandler {

    private static final int DEFAULT_SNAPSHOT_SYNC_BLOCK_SIZE = 4 * 1024;
    private static final Comparator<Long> indexComparator = new Comparator<Long>(){

        @Override
        public int compare(Long arg0, Long arg1) {
            return (int)(arg1.longValue() - arg0.longValue());
        }};
    private RaftContext context;
    private ScheduledFuture<?> scheduledElection;
    private Map<Integer, PeerServer> peers = new HashMap<Integer, PeerServer>();
    private Set<Integer> votedServers = new HashSet<>();
    private ServerRole role;
    private ServerState state;
    private int leader;
    private int id;
    private int votesGranted;
    private boolean electionCompleted;
    private SequentialLogStore logStore;
    private StateMachine stateMachine;
    private Logger logger;
    private Random random;
    private Callable<Void> electionTimeoutTask;
    private ClusterConfiguration config;
    private long quickCommitIndex;
    private CommittingThread commitingThread;

    // fields for extended messages
    private PeerServer serverToJoin = null;
    private boolean configChanging = false;
    private boolean catchingUp = false;
    private int steppingDown = 0;
    private AtomicInteger snapshotInProgress;
    // end fields for extended messages

    public RaftServer(RaftContext context){
        this.id = context.getServerStateManager().getServerId();
        this.state = context.getServerStateManager().readState();
        this.logStore = context.getServerStateManager().loadLogStore();
        this.config = context.getServerStateManager().loadClusterConfiguration();
        this.stateMachine = context.getStateMachine();
        this.votesGranted = 0;
        this.leader = -1;
        this.electionCompleted = false;
        this.snapshotInProgress = new AtomicInteger(0);
        this.context = context;
        this.logger = context.getLoggerFactory().getLogger(this.getClass());
        this.random = new Random(Calendar.getInstance().getTimeInMillis());
        this.electionTimeoutTask = new Callable<Void>(){

            @Override
            public Void call() throws Exception {
                handleElectionTimeout();
                return null;
            }};


        if(this.state == null){
            this.state = new ServerState();
            this.state.setTerm(0);
            this.state.setVotedFor(-1);
            this.state.setCommitIndex(0);
        }

        /**
         * I found this implementation is also a victim of bug https://groups.google.com/forum/#!topic/raft-dev/t4xj6dJTP6E
         * As the implementation is based on Diego's thesis
         * Fix:
         * We should never load configurations that is not committed, 
         *   this prevents an old server from replicating an obsoleted config to other servers
         * The prove is as below:
         * Assume S0 is the last committed server set for the old server A
         * |- EXITS Log l which has been committed but l !BELONGS TO A.logs =>  Vote(A) < Majority(S0)
         * In other words, we need to prove that A cannot be elected to leader if any logs/configs has been committed.
         * Case #1, There is no configuration change since S0, then it's obvious that Vote(A) < Majority(S0), see the core Algorithm
         * Case #2, There are one or more configuration changes since S0, then at the time of first configuration change was committed, 
         *      there are at least Majority(S0 - 1) servers committed the configuration change
         *      Majority(S0 - 1) + Majority(S0) > S0 => Vote(A) < Majority(S0)
         * -|
         */

        //try to see if there is an uncommitted configuration change, since we cannot allow two configuration changes at a time
        for(long i = Math.max(this.state.getCommitIndex() + 1, this.logStore.getStartIndex()); i < this.logStore.getFirstAvailableIndex(); ++i){
            LogEntry logEntry = this.logStore.getLogEntryAt(i);
            if(logEntry.getValueType() == LogValueType.Configuration){
                this.logger.info("detect a configuration change that is not committed yet at index %d", i);
                this.configChanging = true;
                break;
            }
        }

        for(ClusterServer server : this.config.getServers()){
            if(server.getId() != this.id){
                this.peers.put(server.getId(), new PeerServer(server, context, peerServer -> this.handleHeartbeatTimeout(peerServer)));
            }
        }

        this.quickCommitIndex = this.state.getCommitIndex();
        this.commitingThread = new CommittingThread(this);
        this.role = ServerRole.Follower;
        new Thread(this.commitingThread).start();
        this.restartElectionTimer();
        this.logger.info("Server %d started", this.id);
    }

    public RaftMessageSender createMessageSender(){
        return new RaftMessageSenderImpl(this);
    }

    @Override
    public RaftResponseMessage processRequest(RaftRequestMessage request) {
        this.logger.debug(
                "Receive a %s message from %d with LastLogIndex=%d, LastLogTerm=%d, EntriesLength=%d, CommitIndex=%d and Term=%d",
                request.getMessageType().toString(),
                request.getSource(),
                request.getLastLogIndex(),
                request.getLastLogTerm(),
                request.getLogEntries() == null ? 0 : request.getLogEntries().length,
                request.getCommitIndex(),
                request.getTerm());

        RaftResponseMessage response = null;
        if(request.getMessageType() == RaftMessageType.AppendEntriesRequest){
            response = this.handleAppendEntriesRequest(request);
        }else if(request.getMessageType() == RaftMessageType.RequestVoteRequest){
            response = this.handleVoteRequest(request);
        }else if(request.getMessageType() == RaftMessageType.ClientRequest){
            response = this.handleClientRequest(request);
        }else{
            // extended requests
            response = this.handleExtendedMessages(request);
        }

        if(response != null){
            this.logger.debug(
                    "Response back a %s message to %d with Accepted=%s, Term=%d, NextIndex=%d",
                    response.getMessageType().toString(),
                    response.getDestination(),
                    String.valueOf(response.isAccepted()),
                    response.getTerm(),
                    response.getNextIndex());
        }

        return response;
    }

    private synchronized RaftResponseMessage handleAppendEntriesRequest(RaftRequestMessage request){
        // we allow the server to be continue after term updated to save a round message
        this.updateTerm(request.getTerm());

        // Reset stepping down value to prevent this server goes down when leader crashes after sending a LeaveClusterRequest
        if(this.steppingDown > 0){
            this.steppingDown = 2;
        }
        
        if(request.getTerm() == this.state.getTerm()){
            if(this.role == ServerRole.Candidate){
                this.becomeFollower();
            }else if(this.role == ServerRole.Leader){
                this.logger.error("Receive AppendEntriesRequest from another leader(%d) with same term, there must be a bug, server exits", request.getSource());
                this.stateMachine.exit(-1);
            }else{
                this.restartElectionTimer();
            }
        }

        RaftResponseMessage response = new RaftResponseMessage();
        response.setMessageType(RaftMessageType.AppendEntriesResponse);
        response.setTerm(this.state.getTerm());
        response.setSource(this.id);
        response.setDestination(request.getSource());

        // After a snapshot the request.getLastLogIndex() may less than logStore.getStartingIndex() but equals to logStore.getStartingIndex() -1
        // In this case, log is Okay if request.getLastLogIndex() == lastSnapshot.getLastLogIndex() && request.getLastLogTerm() == lastSnapshot.getLastTerm()
        boolean logOkay = request.getLastLogIndex() == 0 ||
                (request.getLastLogIndex() < this.logStore.getFirstAvailableIndex() &&
                        request.getLastLogTerm() == this.termForLastLog(request.getLastLogIndex()));
        if(request.getTerm() < this.state.getTerm() || !logOkay){
            response.setAccepted(false);
            response.setNextIndex(this.logStore.getFirstAvailableIndex());
            return response;
        }

        // The role is Follower and log is okay now
        if(request.getLogEntries() != null && request.getLogEntries().length > 0){
            // write the logs to the store, first of all, check for overlap, and skip them
            LogEntry[] logEntries = request.getLogEntries();
            long index = request.getLastLogIndex() + 1;
            int logIndex = 0;
            while(index < this.logStore.getFirstAvailableIndex() &&
                    logIndex < logEntries.length &&
                    logEntries[logIndex].getTerm() == this.logStore.getLogEntryAt(index).getTerm()){
                logIndex ++;
                index ++;
            }

            // dealing with overwrites
            while(index < this.logStore.getFirstAvailableIndex() && logIndex < logEntries.length){
                LogEntry oldEntry = this.logStore.getLogEntryAt(index);
                if(oldEntry.getValueType() == LogValueType.Application){
                    this.stateMachine.rollback(index, oldEntry.getValue());
                }else if(oldEntry.getValueType() == LogValueType.Configuration){
                    this.logger.info("revert a previous config change to config at %d", this.config.getLogIndex());
                    this.configChanging = false;
                }

                LogEntry logEntry = logEntries[logIndex];
                this.logStore.writeAt(index, logEntry);
                if (logEntry.getValueType() == LogValueType.Application) {
                    this.stateMachine.preCommit(index, logEntry.getValue());
                } else if (logEntry.getValueType() == LogValueType.Configuration) {
                    this.logger.info("received a configuration change at index %d from leader", index);
                    this.configChanging = true;
                }

                index += 1;
                logIndex += 1;
            }

            // append the new log entries
            while(logIndex < logEntries.length){
                LogEntry logEntry = logEntries[logIndex ++];
                long indexForEntry = this.logStore.append(logEntry);
                if(logEntry.getValueType() == LogValueType.Configuration){
                    this.logger.info("received a configuration change at index %d from leader", indexForEntry);
                    this.configChanging = true;
                } else if(logEntry.getValueType() == LogValueType.Application) {
                    this.stateMachine.preCommit(indexForEntry, logEntry.getValue());
                }
            }
        }

        this.leader = request.getSource();
        this.commit(request.getCommitIndex());
        response.setAccepted(true);
        response.setNextIndex(request.getLastLogIndex() + (request.getLogEntries() == null ? 0 : request.getLogEntries().length) + 1);
        return response;
    }

    private synchronized RaftResponseMessage handleVoteRequest(RaftRequestMessage request){
        // we allow the server to be continue after term updated to save a round message
        this.updateTerm(request.getTerm());

        // Reset stepping down value to prevent this server goes down when leader crashes after sending a LeaveClusterRequest
        if(this.steppingDown > 0){
            this.steppingDown = 2;
        }
        
        RaftResponseMessage response = new RaftResponseMessage();
        response.setMessageType(RaftMessageType.RequestVoteResponse);
        response.setSource(this.id);
        response.setDestination(request.getSource());
        response.setTerm(this.state.getTerm());

        boolean logOkay = request.getLastLogTerm() > this.logStore.getLastLogEntry().getTerm() ||
                (request.getLastLogTerm() == this.logStore.getLastLogEntry().getTerm() &&
                 this.logStore.getFirstAvailableIndex() - 1 <= request.getLastLogIndex());
        boolean grant = request.getTerm() == this.state.getTerm() && logOkay && (this.state.getVotedFor() == request.getSource() || this.state.getVotedFor() == -1);
        response.setAccepted(grant);
        if(grant){
            this.state.setVotedFor(request.getSource());
            this.context.getServerStateManager().persistState(this.state);
        }

        return response;
    }

    private RaftResponseMessage handleClientRequest(RaftRequestMessage request){
        RaftResponseMessage response = new RaftResponseMessage();
        response.setMessageType(RaftMessageType.AppendEntriesResponse);
        response.setSource(this.id);
        response.setDestination(this.leader);
        response.setTerm(this.state.getTerm());

        long term;
        synchronized(this){
            if(this.role != ServerRole.Leader){
                response.setAccepted(false);
                return response;
            }
            
            term = this.state.getTerm();
        }

        LogEntry[] logEntries = request.getLogEntries();
        if(logEntries != null && logEntries.length > 0){
            for(int i = 0; i < logEntries.length; ++i){
                
                this.stateMachine.preCommit(this.logStore.append(new LogEntry(term, logEntries[i].getValue())), logEntries[i].getValue());
            }
        }

        // Urgent commit, so that the commit will not depend on heartbeat
        this.requestAppendEntries();
        response.setAccepted(true);
        response.setNextIndex(this.logStore.getFirstAvailableIndex());
        return response;
    }

    private synchronized void handleElectionTimeout(){
        if(this.steppingDown > 0){
            if(--this.steppingDown == 0){
                this.logger.info("no hearing further news from leader, remove this server from config and step down");
                ClusterServer server = this.config.getServer(this.id);
                if(server != null){
                    this.config.getServers().remove(server);
                    this.context.getServerStateManager().saveClusterConfiguration(this.config);
                }
                
                this.stateMachine.exit(0);
                return;
            }

            this.logger.info("stepping down (cycles left: %d), skip this election timeout event", this.steppingDown);
            this.restartElectionTimer();
            return;
        }

        if(this.catchingUp){
            // this is a new server for the cluster, will not send out vote request until the config that includes this server is committed
            this.logger.info("election timeout while joining the cluster, ignore it.");
            this.restartElectionTimer();
            return;
        }

        if(this.role == ServerRole.Leader){
            this.logger.error("A leader should never encounter election timeout, illegal application state, stop the application");
            this.stateMachine.exit(-1);
            return;
        }

        this.logger.debug("Election timeout, change to Candidate");
        this.state.increaseTerm();
        this.state.setVotedFor(-1);
        this.role = ServerRole.Candidate;
        this.votedServers.clear();
        this.votesGranted = 0;
        this.electionCompleted = false;
        this.context.getServerStateManager().persistState(this.state);
        this.requestVote();

        // restart the election timer if this is not yet a leader
        if(this.role != ServerRole.Leader){
            this.restartElectionTimer();
        }
    }

    private void requestVote(){
        // vote for self
        this.logger.info("requestVote started with term %d", this.state.getTerm());
        this.state.setVotedFor(this.id);
        this.context.getServerStateManager().persistState(this.state);
        this.votesGranted += 1;
        this.votedServers.add(this.id);

        // this is the only server?
        if(this.votesGranted > (this.peers.size() + 1) / 2){
            this.electionCompleted = true;
            this.becomeLeader();
            return;
        }

        for(PeerServer peer : this.peers.values()){
            RaftRequestMessage request = new RaftRequestMessage();
            request.setMessageType(RaftMessageType.RequestVoteRequest);
            request.setDestination(peer.getId());
            request.setSource(this.id);
            request.setLastLogIndex(this.logStore.getFirstAvailableIndex() - 1);
            request.setLastLogTerm(this.termForLastLog(this.logStore.getFirstAvailableIndex() - 1));
            request.setTerm(this.state.getTerm());
            this.logger.debug("send %s to server %d with term %d", RaftMessageType.RequestVoteRequest.toString(), peer.getId(), this.state.getTerm());
            peer.SendRequest(request).whenCompleteAsync((RaftResponseMessage response, Throwable error) -> {
                handlePeerResponse(response, error);
            }, this.context.getScheduledExecutor());
        }
    }

    private void requestAppendEntries(){
        if(this.peers.size() == 0){
            this.commit(this.logStore.getFirstAvailableIndex() - 1);
            return;
        }

        for(PeerServer peer : this.peers.values()){
            this.requestAppendEntries(peer);
        }
    }

    private boolean requestAppendEntries(PeerServer peer){
        if(peer.makeBusy()){
            peer.SendRequest(this.createAppendEntriesRequest(peer))
                .whenCompleteAsync((RaftResponseMessage response, Throwable error) -> {
                    try{
                        handlePeerResponse(response, error);
                    }catch(Throwable err){
                        this.logger.error("Uncaught exception %s", err.toString());
                    }
                }, this.context.getScheduledExecutor());
            return true;
        }

        this.logger.debug("Server %d is busy, skip the request", peer.getId());
        return false;
    }

    private synchronized void handlePeerResponse(RaftResponseMessage response, Throwable error){
        if(error != null){
            this.logger.info("peer response error: %s", error.getMessage());
            return;
        }

        this.logger.debug(
                "Receive a %s message from peer %d with Result=%s, Term=%d, NextIndex=%d",
                response.getMessageType().toString(),
                response.getSource(),
                String.valueOf(response.isAccepted()),
                response.getTerm(),
                response.getNextIndex());
        // If term is updated no need to proceed
        if(this.updateTerm(response.getTerm())){
            return;
        }

        // Ignore the response that with lower term for safety
        if(response.getTerm() < this.state.getTerm()){
            this.logger.info("Received a peer response from %d that with lower term value %d v.s. %d", response.getSource(), response.getTerm(), this.state.getTerm());
            return;
        }

        if(response.getMessageType() == RaftMessageType.RequestVoteResponse){
            this.handleVotingResponse(response);
        }else if(response.getMessageType() == RaftMessageType.AppendEntriesResponse){
            this.handleAppendEntriesResponse(response);
        }else if(response.getMessageType() == RaftMessageType.InstallSnapshotResponse){
            this.handleInstallSnapshotResponse(response);
        }else{
            this.logger.error("Received an unexpected message %s for response, system exits.", response.getMessageType().toString());
            this.stateMachine.exit(-1);
        }
    }

    private void handleAppendEntriesResponse(RaftResponseMessage response){
        PeerServer peer = this.peers.get(response.getSource());
        if(peer == null){
            this.logger.info("the response is from an unkonw peer %d", response.getSource());
            return;
        }

        // If there are pending logs to be synced or commit index need to be advanced, continue to send appendEntries to this peer
        boolean needToCatchup = true;
        if(response.isAccepted()){
            synchronized(peer){
                peer.setNextLogIndex(response.getNextIndex());
                peer.setMatchedIndex(response.getNextIndex() - 1);
            }

            // try to commit with this response
            ArrayList<Long> matchedIndexes = new ArrayList<Long>(this.peers.size() + 1);
            matchedIndexes.add(this.logStore.getFirstAvailableIndex() - 1);
            for(PeerServer p : this.peers.values()){
                matchedIndexes.add(p.getMatchedIndex());
            }

            matchedIndexes.sort(indexComparator);
            this.commit(matchedIndexes.get((this.peers.size() + 1) / 2));
            needToCatchup = peer.clearPendingCommit() || response.getNextIndex() < this.logStore.getFirstAvailableIndex();
        }else{
            synchronized(peer){
                // Improvement: if peer's real log length is less than was assumed, reset to that length directly
                if(response.getNextIndex() > 0 && peer.getNextLogIndex() > response.getNextIndex()){
                    peer.setNextLogIndex(response.getNextIndex());
                }else{
                    peer.setNextLogIndex(peer.getNextLogIndex() - 1);
                }
            }
        }

        // This may not be a leader anymore, such as the response was sent out long time ago
        // and the role was updated by UpdateTerm call
        // Try to match up the logs for this peer
        if(this.role == ServerRole.Leader && needToCatchup){
            this.requestAppendEntries(peer);
        }
    }

    private void handleInstallSnapshotResponse(RaftResponseMessage response){
        PeerServer peer = this.peers.get(response.getSource());
        if(peer == null){
            this.logger.info("the response is from an unkonw peer %d", response.getSource());
            return;
        }

        // If there are pending logs to be synced or commit index need to be advanced, continue to send appendEntries to this peer
        boolean needToCatchup = true;
        if(response.isAccepted()){
            synchronized(peer){
                SnapshotSyncContext context = peer.getSnapshotSyncContext();
                if(context == null){
                    this.logger.info("no snapshot sync context for this peer, drop the response");
                    needToCatchup = false;
                }else{
                    if(response.getNextIndex() >= context.getSnapshot().getSize()){
                        this.logger.debug("snapshot sync is done");
                        peer.setNextLogIndex(context.getSnapshot().getLastLogIndex() + 1);
                        peer.setMatchedIndex(context.getSnapshot().getLastLogIndex());
                        peer.setSnapshotInSync(null);
                        needToCatchup = peer.clearPendingCommit() || response.getNextIndex() < this.logStore.getFirstAvailableIndex();
                    }else{
                        this.logger.debug("continue to sync snapshot at offset %d", response.getNextIndex());
                        context.setOffset(response.getNextIndex());
                    }
                }
            }

        }else{
            this.logger.info("peer declines to install the snapshot, will retry");
        }

        // This may not be a leader anymore, such as the response was sent out long time ago
        // and the role was updated by UpdateTerm call
        // Try to match up the logs for this peer
        if(this.role == ServerRole.Leader && needToCatchup){
            this.requestAppendEntries(peer);
        }
    }

    private void handleVotingResponse(RaftResponseMessage response){
        if(this.votedServers.contains(response.getSource())) {
            this.logger.info("Duplicate vote from %d form term %d", response.getSource(), this.state.getTerm());
            return;
        }

        this.votedServers.add(response.getSource());
        if(this.electionCompleted){
            this.logger.info("Election completed, will ignore the voting result from this server");
            return;
        }

        if(response.isAccepted()){
            this.votesGranted += 1;
        }

        if(this.votedServers.size() >= this.peers.size() + 1){
            this.electionCompleted = true;
        }

        // got a majority set of granted votes
        if(this.votesGranted > (this.peers.size() + 1) / 2){
            this.logger.info("Server is elected as leader for term %d", this.state.getTerm());
            this.electionCompleted = true;
            this.becomeLeader();
        }
    }

    private synchronized void handleHeartbeatTimeout(PeerServer peer){
        this.logger.debug("Heartbeat timeout for %d", peer.getId());
        if(this.role == ServerRole.Leader){
            this.requestAppendEntries(peer);

            synchronized(peer){
                if(peer.isHeartbeatEnabled()){
                    // Schedule another heartbeat if heartbeat is still enabled
                    peer.setHeartbeatTask(this.context.getScheduledExecutor().schedule(peer.getHeartbeartHandler(), peer.getCurrentHeartbeatInterval(), TimeUnit.MILLISECONDS));
                }else{
                    this.logger.debug("heartbeat is disabled for peer %d", peer.getId());
                }
            }
        }else{
            this.logger.info("Receive a heartbeat event for %d while no longer as a leader", peer.getId());
        }
    }

    private void restartElectionTimer(){
        // don't start the election timer while this server is still catching up the logs
        if(this.catchingUp){
            return;
        }

        if(this.scheduledElection != null){
            this.scheduledElection.cancel(false);
        }

        RaftParameters parameters = this.context.getRaftParameters();
        int electionTimeout = parameters.getElectionTimeoutLowerBound() + this.random.nextInt(parameters.getElectionTimeoutUpperBound() - parameters.getElectionTimeoutLowerBound() + 1);
        this.scheduledElection = this.context.getScheduledExecutor().schedule(this.electionTimeoutTask, electionTimeout, TimeUnit.MILLISECONDS);
    }

    private void stopElectionTimer(){
        if(this.scheduledElection == null){
            this.logger.warning("Election Timer is never started but is requested to stop, protential a bug");
            return;
        }

        this.scheduledElection.cancel(false);
        this.scheduledElection = null;
    }

    private void becomeLeader(){
        this.stopElectionTimer();
        this.role = ServerRole.Leader;
        this.leader = this.id;
        this.serverToJoin = null;
        for(PeerServer server : this.peers.values()){
            server.setNextLogIndex(this.logStore.getFirstAvailableIndex());
            server.setSnapshotInSync(null);
            server.setFree();
            this.enableHeartbeatForPeer(server);
        }

        // if current config is not committed, try to commit it
        if(this.config.getLogIndex() == 0){
            this.config.setLogIndex(this.logStore.getFirstAvailableIndex());
            this.logStore.append(new LogEntry(this.state.getTerm(), this.config.toBytes(), LogValueType.Configuration));
            this.logger.info("add initial configuration to log store");
            this.configChanging = true;
        }

        this.requestAppendEntries();
    }

    private void enableHeartbeatForPeer(PeerServer peer){
        peer.enableHeartbeat(true);
        peer.resumeHeartbeatingSpeed();
        peer.setHeartbeatTask(this.context.getScheduledExecutor().schedule(peer.getHeartbeartHandler(), peer.getCurrentHeartbeatInterval(), TimeUnit.MILLISECONDS));
    }

    private void becomeFollower(){
        // stop heartbeat for all peers
        for(PeerServer server : this.peers.values()){
            if(server.getHeartbeatTask() != null){
                server.getHeartbeatTask().cancel(false);
            }

            server.enableHeartbeat(false);
        }

        this.serverToJoin = null;
        this.role = ServerRole.Follower;
        this.restartElectionTimer();
    }

    private boolean updateTerm(long term){
        if(term > this.state.getTerm()){
            this.state.setTerm(term);
            this.state.setVotedFor(-1);
            this.electionCompleted = false;
            this.votesGranted = 0;
            this.votedServers.clear();
            this.context.getServerStateManager().persistState(this.state);
            this.becomeFollower();
            return true;
        }

        return false;
    }

    private void commit(long targetIndex){
        if(targetIndex > this.quickCommitIndex){
            this.quickCommitIndex = targetIndex;

            // if this is a leader notify peers to commit as well
            // for peers that are free, send the request, otherwise, set pending commit flag for that peer
            if(this.role == ServerRole.Leader){
                for(PeerServer peer : this.peers.values()){
                    if(!this.requestAppendEntries(peer)){
                        peer.setPendingCommit();
                    }
                }
            }
        }

        if(this.logStore.getFirstAvailableIndex() - 1 > this.state.getCommitIndex() && this.quickCommitIndex > this.state.getCommitIndex()){
            this.commitingThread.moreToCommit();
        }
    }

    private void snapshotAndCompact(long indexCommitted){
        boolean snapshotInAction = false;
        try{
            // see if we need to do snapshots
            if(this.context.getRaftParameters().getSnapshotDistance() > 0
                && ((indexCommitted - this.logStore.getStartIndex()) > this.context.getRaftParameters().getSnapshotDistance())
                && this.snapshotInProgress.compareAndSet(0, 1)){
                snapshotInAction = true;
                Snapshot currentSnapshot = this.stateMachine.getLastSnapshot();
                if(currentSnapshot != null && indexCommitted - currentSnapshot.getLastLogIndex() < this.context.getRaftParameters().getSnapshotDistance()){
                    this.logger.info("a very recent snapshot is available at index %d, will skip this one", currentSnapshot.getLastLogIndex());
                    this.snapshotInProgress.set(0);
                    snapshotInAction = false;
                }else{
                    this.logger.info("creating a snapshot for index %d", indexCommitted);

                    // get the latest configuration info
                    ClusterConfiguration config = this.config;
                    while(config.getLogIndex() > indexCommitted && config.getLastLogIndex() >= this.logStore.getStartIndex()){
                        config = ClusterConfiguration.fromBytes(this.logStore.getLogEntryAt(config.getLastLogIndex()).getValue());
                    }

                    if(config.getLogIndex() > indexCommitted && config.getLastLogIndex() > 0 && config.getLastLogIndex() < this.logStore.getStartIndex()){
                        Snapshot lastSnapshot = this.stateMachine.getLastSnapshot();
                        if(lastSnapshot == null){
                            this.logger.error("No snapshot could be found while no configuration cannot be found in current committed logs, this is a system error, exiting");
                            this.stateMachine.exit(-1);
                            return;
                        }

                        config = lastSnapshot.getLastConfig();
                    }else if(config.getLogIndex() > indexCommitted && config.getLastLogIndex() == 0){
                        this.logger.error("BUG!!! stop the system, there must be a configuration at index one");
                        this.stateMachine.exit(-1);
                    }

                    long indexToCompact = indexCommitted - 1;
                    long logTermToCompact = this.logStore.getLogEntryAt(indexToCompact).getTerm();
                    Snapshot snapshot = new Snapshot(indexToCompact, logTermToCompact, config);
                    this.stateMachine.createSnapshot(snapshot).whenCompleteAsync((Boolean result, Throwable error) -> {
                        try{
                            if(error != null){
                                this.logger.error("failed to create a snapshot due to %s", error.getMessage());
                                return;
                            }

                            if(!result.booleanValue()){
                                this.logger.info("the state machine rejects to create the snapshot");
                                return;
                            }

                            synchronized(this){
                                this.logger.debug("snapshot created, compact the log store");
                                try{
                                    this.logStore.compact(snapshot.getLastLogIndex());
                                }catch(Throwable ex){
                                    this.logger.error("failed to compact the log store, no worries, the system still in a good shape", ex);
                                }
                            }
                        }finally{
                            this.snapshotInProgress.set(0);
                        }
                    }, this.context.getScheduledExecutor());
                    snapshotInAction = false;
                }
            }
        }catch(Throwable error){
            this.logger.error("failed to compact logs at index %d, due to errors %s", indexCommitted, error.toString());
            if(snapshotInAction){
                this.snapshotInProgress.compareAndSet(1, 0);
            }
        }
    }

    private RaftRequestMessage createAppendEntriesRequest(PeerServer peer){
        long currentNextIndex = 0;
        long commitIndex = 0;
        long lastLogIndex = 0;
        long term = 0;
        long startingIndex = 1;

        synchronized(this){
            startingIndex = this.logStore.getStartIndex();
            currentNextIndex = this.logStore.getFirstAvailableIndex();
            commitIndex = this.quickCommitIndex;
            term = this.state.getTerm();
        }

        synchronized(peer){
            if(peer.getNextLogIndex() == 0){
                peer.setNextLogIndex(currentNextIndex);
            }

            lastLogIndex = peer.getNextLogIndex() - 1;
        }

        if(lastLogIndex >= currentNextIndex){
            this.logger.error("Peer's lastLogIndex is too large %d v.s. %d, server exits", lastLogIndex, currentNextIndex);
            this.stateMachine.exit(-1);
        }

        // for syncing the snapshots, if the lastLogIndex == lastSnapshot.getLastLogIndex, we could get the term from the snapshot
        if(lastLogIndex > 0 && lastLogIndex < startingIndex - 1){
            return this.createSyncSnapshotRequest(peer, lastLogIndex, term, commitIndex);
        }

        long lastLogTerm = this.termForLastLog(lastLogIndex);
        long endIndex = Math.min(currentNextIndex, lastLogIndex + 1 + context.getRaftParameters().getMaximumAppendingSize());
        LogEntry[] logEntries = (lastLogIndex + 1) >= endIndex ? null : this.logStore.getLogEntries(lastLogIndex + 1, endIndex);
        this.logger.debug(
                "An AppendEntries Request for %d with LastLogIndex=%d, LastLogTerm=%d, EntriesLength=%d, CommitIndex=%d and Term=%d",
                peer.getId(),
                lastLogIndex,
                lastLogTerm,
                logEntries == null ? 0 : logEntries.length,
                commitIndex,
                term);
        RaftRequestMessage requestMessage = new RaftRequestMessage();
        requestMessage.setMessageType(RaftMessageType.AppendEntriesRequest);
        requestMessage.setSource(this.id);
        requestMessage.setDestination(peer.getId());
        requestMessage.setLastLogIndex(lastLogIndex);
        requestMessage.setLastLogTerm(lastLogTerm);
        requestMessage.setLogEntries(logEntries);
        requestMessage.setCommitIndex(commitIndex);
        requestMessage.setTerm(term);
        return requestMessage;
    }

    private void reconfigure(ClusterConfiguration newConfig){
        this.logger.debug(
                "system is reconfigured to have %d servers, last config index: %d, this config index: %d",
                newConfig.getServers().size(),
                newConfig.getLastLogIndex(),
                newConfig.getLogIndex());
        List<Integer> serversRemoved = new LinkedList<Integer>();
        List<ClusterServer> serversAdded = new LinkedList<ClusterServer>();
        for(ClusterServer s : newConfig.getServers()){
            if(!this.peers.containsKey(s.getId()) && s.getId() != this.id){
                serversAdded.add(s);
            }
        }

        for(Integer id : this.peers.keySet()){
            if(newConfig.getServer(id.intValue()) == null){
                serversRemoved.add(id);
            }
        }
        
        if(newConfig.getServer(this.id) == null){
            serversRemoved.add(this.id);
        }

        for(ClusterServer server : serversAdded){
            if(server.getId() != this.id){
                PeerServer peer = new PeerServer(server, context, peerServer -> this.handleHeartbeatTimeout(peerServer));
                peer.setNextLogIndex(this.logStore.getFirstAvailableIndex());
                this.peers.put(server.getId(), peer);
                this.logger.info("server %d is added to cluster", peer.getId());
                if(this.role == ServerRole.Leader){
                    this.logger.info("enable heartbeating for server %d", peer.getId());
                    this.enableHeartbeatForPeer(peer);
                    if(this.serverToJoin != null && this.serverToJoin.getId() == peer.getId()){
                        peer.setNextLogIndex(this.serverToJoin.getNextLogIndex());
                        this.serverToJoin = null;
                    }
                }
            }
        }

        for(Integer id : serversRemoved){
            if(id == this.id && !this.catchingUp){
                // this server is removed from cluster
                this.context.getServerStateManager().saveClusterConfiguration(newConfig);
                this.logger.info("server has been removed from cluster, step down");
                this.stateMachine.exit(0);
                return;
            }
            
            PeerServer peer = this.peers.get(id);
            if(peer == null){
                this.logger.info("peer %d cannot be found in current peer list", id);
            } else{
                if(peer.getHeartbeatTask() != null){
                    peer.getHeartbeatTask().cancel(false);
                }

                peer.enableHeartbeat(false);
                this.peers.remove(id);
                this.logger.info("server %d is removed from cluster", id.intValue());
            }
        }

        this.config = newConfig;
    }

    private synchronized RaftResponseMessage handleExtendedMessages(RaftRequestMessage request){
        if(request.getMessageType() == RaftMessageType.AddServerRequest){
            return this.handleAddServerRequest(request);
        }else if(request.getMessageType() == RaftMessageType.RemoveServerRequest){
            return this.handleRemoveServerRequest(request);
        }else if(request.getMessageType() == RaftMessageType.SyncLogRequest){
            return this.handleLogSyncRequest(request);
        }else if(request.getMessageType() == RaftMessageType.JoinClusterRequest){
            return this.handleJoinClusterRequest(request);
        }else if(request.getMessageType() == RaftMessageType.LeaveClusterRequest){
            return this.handleLeaveClusterRequest(request);
        }else if(request.getMessageType() == RaftMessageType.InstallSnapshotRequest){
            return this.handleInstallSnapshotRequest(request);
        }else{
            this.logger.error("receive an unknown request %s, for safety, step down.", request.getMessageType().toString());
            this.stateMachine.exit(-1);
        }

        return null;
    }

    private RaftResponseMessage handleInstallSnapshotRequest(RaftRequestMessage request){
        // we allow the server to be continue after term updated to save a round message
        this.updateTerm(request.getTerm());

        // Reset stepping down value to prevent this server goes down when leader crashes after sending a LeaveClusterRequest
        if(this.steppingDown > 0){
            this.steppingDown = 2;
        }
        
        if(request.getTerm() == this.state.getTerm() && !this.catchingUp){
            if(this.role == ServerRole.Candidate){
                this.becomeFollower();
            }else if(this.role == ServerRole.Leader){
                this.logger.error("Receive InstallSnapshotRequest from another leader(%d) with same term, there must be a bug, server exits", request.getSource());
                this.stateMachine.exit(-1);
            }else{
                this.restartElectionTimer();
            }
        }

        RaftResponseMessage response = new RaftResponseMessage();
        response.setMessageType(RaftMessageType.InstallSnapshotResponse);
        response.setTerm(this.state.getTerm());
        response.setSource(this.id);
        response.setDestination(request.getSource());
        if(!this.catchingUp && request.getTerm() < this.state.getTerm()){
            this.logger.info("received an install snapshot request which has lower term than this server, decline the request");
            response.setAccepted(false);
            response.setNextIndex(0);
            return response;
        }

        LogEntry logEntries[] = request.getLogEntries();
        if(logEntries == null || logEntries.length != 1 || logEntries[0].getValueType() != LogValueType.SnapshotSyncRequest){
            this.logger.warning("Receive an invalid InstallSnapshotRequest due to bad log entries or bad log entry value");
            response.setNextIndex(0);
            response.setAccepted(false);
            return response;
        }

        SnapshotSyncRequest snapshotSyncRequest = SnapshotSyncRequest.fromBytes(logEntries[0].getValue());

        // We don't want to apply a snapshot that is older than we have, this may not happen, but just in case
        if(snapshotSyncRequest.getSnapshot().getLastLogIndex() <= this.quickCommitIndex){
            this.logger.error("Received a snapshot which is older than this server (%d)", this.id);
            response.setNextIndex(0);
            response.setAccepted(false);
            return response;
        }

        response.setAccepted(this.handleSnapshotSyncRequest(snapshotSyncRequest));
        response.setNextIndex(snapshotSyncRequest.getOffset() + snapshotSyncRequest.getData().length); // the next index will be ignored if "accept" is false
        return response;
    }

    private boolean handleSnapshotSyncRequest(SnapshotSyncRequest snapshotSyncRequest){
        try{
            this.stateMachine.saveSnapshotData(snapshotSyncRequest.getSnapshot(), snapshotSyncRequest.getOffset(), snapshotSyncRequest.getData());
            if(snapshotSyncRequest.isDone()){
                // Only follower will run this piece of code, but let's check it again
                if(this.role != ServerRole.Follower){
                    this.logger.error("bad server role for applying a snapshot, exit for debugging");
                    this.stateMachine.exit(-1);
                }

                this.logger.debug("sucessfully receive a snapshot from leader");
                if(this.logStore.compact(snapshotSyncRequest.getSnapshot().getLastLogIndex())){
                    // The state machine will not be able to commit anything before the snapshot is applied, so make this synchronously
                    // with election timer stopped as usually applying a snapshot may take a very long time
                    this.stopElectionTimer();
                    try{
                        this.logger.info("successfully compact the log store, will now ask the statemachine to apply the snapshot");
                        if(!this.stateMachine.applySnapshot(snapshotSyncRequest.getSnapshot())){
                            this.logger.error("failed to apply the snapshot after log compacted, to ensure the safety, will shutdown the system");
                            this.stateMachine.exit(-1);
                            return false; //should never be reached
                        }

                        this.reconfigure(snapshotSyncRequest.getSnapshot().getLastConfig());
                        this.context.getServerStateManager().saveClusterConfiguration(this.config);
                        this.state.setCommitIndex(snapshotSyncRequest.getSnapshot().getLastLogIndex());
                        this.quickCommitIndex = snapshotSyncRequest.getSnapshot().getLastLogIndex();
                        this.context.getServerStateManager().persistState(this.state);
                        this.logger.info("snapshot is successfully applied");
                    }finally{
                        this.restartElectionTimer();
                    }
                }else{
                    this.logger.error("failed to compact the log store after a snapshot is received, will ask the leader to retry");
                    return false;
                }
            }
        }catch(Throwable error){
            this.logger.error("I/O error %s while saving the snapshot or applying the snapshot, no reason to continue", error.getMessage());
            this.stateMachine.exit(-1);
            return false;
        }

        return true;
    }

    private synchronized void handleExtendedResponse(RaftResponseMessage response, Throwable error){
        if(error != null){
            this.handleExtendedResponseError(error);
            return;
        }

        this.logger.debug(
                "Receive an extended %s message from peer %d with Result=%s, Term=%d, NextIndex=%d",
                response.getMessageType().toString(),
                response.getSource(),
                String.valueOf(response.isAccepted()),
                response.getTerm(),
                response.getNextIndex());
        if(response.getMessageType() == RaftMessageType.SyncLogResponse){
            if(this.serverToJoin != null){
                // we are reusing heartbeat interval value to indicate when to stop retry
                this.serverToJoin.resumeHeartbeatingSpeed();
                this.serverToJoin.setNextLogIndex(response.getNextIndex());
                this.serverToJoin.setMatchedIndex(response.getNextIndex() - 1);
                this.syncLogsToNewComingServer(response.getNextIndex());
            }
        }else if(response.getMessageType() == RaftMessageType.JoinClusterResponse){
            if(this.serverToJoin != null){
                if(response.isAccepted()){
                    this.logger.debug("new server confirms it will join, start syncing logs to it");
                    this.syncLogsToNewComingServer(1);
                }else{
                    this.logger.debug("new server cannot accept the invitation, give up");
                }
            }else{
                this.logger.debug("no server to join, drop the message");
            }
        }else if(response.getMessageType() == RaftMessageType.LeaveClusterResponse){
            if(!response.isAccepted()){
                this.logger.info("peer doesn't accept to stepping down, stop proceeding");
                return;
            }

            this.logger.debug("peer accepted to stepping down, removing this server from cluster");
            this.removeServerFromCluster(response.getSource());
        }else if(response.getMessageType() == RaftMessageType.InstallSnapshotResponse){
            if(this.serverToJoin == null){
                this.logger.info("no server to join, the response must be very old.");
                return;
            }

            if(!response.isAccepted()){
                this.logger.info("peer doesn't accept the snapshot installation request");
                return;
            }

            SnapshotSyncContext context = this.serverToJoin.getSnapshotSyncContext();
            if(context == null){
                this.logger.error("Bug! SnapshotSyncContext must not be null");
                this.stateMachine.exit(-1);
                return;
            }

            if(response.getNextIndex() >= context.getSnapshot().getSize()){
                // snapshot is done
                this.logger.debug("snapshot has been copied and applied to new server, continue to sync logs after snapshot");
                this.serverToJoin.setSnapshotInSync(null);
                this.serverToJoin.setNextLogIndex(context.getSnapshot().getLastLogIndex() + 1);
                this.serverToJoin.setMatchedIndex(context.getSnapshot().getLastLogIndex());
            }else{
                context.setOffset(response.getNextIndex());
                this.logger.debug("continue to send snapshot to new server at offset %d", response.getNextIndex());
            }

            this.syncLogsToNewComingServer(this.serverToJoin.getNextLogIndex());
        }else{
            // No more response message types need to be handled
            this.logger.error("received an unexpected response message type %s, for safety, stepping down", response.getMessageType());
            this.stateMachine.exit(-1);
        }
    }

    private void handleExtendedResponseError(Throwable error){
        this.logger.info("receive an error response from peer server, %s", error.toString());
        RpcException rpcError = null;
        if(error instanceof RpcException){
            rpcError = (RpcException)error;
        }else if(error instanceof CompletionException && ((CompletionException)error).getCause() instanceof RpcException){
            rpcError = (RpcException)((CompletionException)error).getCause();
        }

        if(rpcError != null){
            this.logger.debug("it's a rpc error, see if we need to retry");
            final RaftRequestMessage request = rpcError.getRequest();
            if(request.getMessageType() == RaftMessageType.SyncLogRequest || request.getMessageType() == RaftMessageType.JoinClusterRequest || request.getMessageType() == RaftMessageType.LeaveClusterRequest){
                final PeerServer server = (request.getMessageType() == RaftMessageType.LeaveClusterRequest) ? this.peers.get(request.getDestination()) : this.serverToJoin;
                if(server != null){
                    if(server.getCurrentHeartbeatInterval() >= this.context.getRaftParameters().getMaxHeartbeatInterval()){
                        if(request.getMessageType() == RaftMessageType.LeaveClusterRequest){
                            this.logger.info("rpc failed again for the removing server (%d), will remove this server directly", server.getId());
                            
                            /**
                             * In case of there are only two servers in the cluster, it safe to remove the server directly from peers
                             * as at most one config change could happen at a time
                             *  prove:
                             *      assume there could be two config changes at a time
                             *      this means there must be a leader after previous leader offline, which is impossible 
                             *      (no leader could be elected after one server goes offline in case of only two servers in a cluster)
                             * so the bug https://groups.google.com/forum/#!topic/raft-dev/t4xj6dJTP6E 
                             * does not apply to cluster which only has two members
                             */
                            if(this.peers.size() == 1){
                                PeerServer peer = this.peers.get(server.getId());
                                if(peer == null){
                                    this.logger.info("peer %d cannot be found in current peer list", id);
                                } else{
                                    if(peer.getHeartbeatTask() != null){
                                        peer.getHeartbeatTask().cancel(false);
                                    }

                                    peer.enableHeartbeat(false);
                                    this.peers.remove(server.getId());
                                    this.logger.info("server %d is removed from cluster", server.getId());
                                }
                            }
                            
                            this.removeServerFromCluster(server.getId());
                        }else{
                            this.logger.info("rpc failed again for the new coming server (%d), will stop retry for this server", server.getId());
                            this.configChanging = false;
                            this.serverToJoin = null;
                        }
                    }else{
                        // reuse the heartbeat interval value to indicate when to stop retrying, as rpc backoff is the same
                        this.logger.debug("retry the request");
                        server.slowDownHeartbeating();
                        final RaftServer self = this;
                        this.context.getScheduledExecutor().schedule(new Callable<Void>(){

                            @Override
                            public Void call() throws Exception {
                                self.logger.debug("retrying the request %s", request.getMessageType().toString());
                                server.SendRequest(request).whenCompleteAsync((RaftResponseMessage furtherResponse, Throwable furtherError) -> {
                                    self.handleExtendedResponse(furtherResponse, furtherError);
                                }, self.context.getScheduledExecutor());
                                return null;
                            }}, server.getCurrentHeartbeatInterval(), TimeUnit.MILLISECONDS);
                    }
                }
            }
        }
    }

    private RaftResponseMessage handleRemoveServerRequest(RaftRequestMessage request){
        LogEntry[] logEntries = request.getLogEntries();
        RaftResponseMessage response = new RaftResponseMessage();
        response.setSource(this.id);
        response.setDestination(this.leader);
        response.setTerm(this.state.getTerm());
        response.setMessageType(RaftMessageType.RemoveServerResponse);
        response.setNextIndex(this.logStore.getFirstAvailableIndex());
        response.setAccepted(false);
        if(logEntries.length != 1 || logEntries[0].getValue() == null || logEntries[0].getValue().length != Integer.BYTES){
            this.logger.info("bad remove server request as we are expecting one log entry with value type of Integer");
            return response;
        }

        if(this.role != ServerRole.Leader){
            this.logger.info("this is not a leader, cannot handle RemoveServerRequest");
            return response;
        }

        if(this.configChanging){
            // the previous config has not committed yet
            this.logger.info("previous config has not committed yet");
            return response;
        }

        int serverId = ByteBuffer.wrap(logEntries[0].getValue()).getInt();
        if(serverId == this.id){
            this.logger.info("cannot request to remove leader");
            return response;
        }

        PeerServer peer = this.peers.get(serverId);
        if(peer == null){
            this.logger.info("server %d does not exist", serverId);
            return response;
        }

        RaftRequestMessage leaveClusterRequest = new RaftRequestMessage();
        leaveClusterRequest.setCommitIndex(this.quickCommitIndex);
        leaveClusterRequest.setDestination(peer.getId());
        leaveClusterRequest.setLastLogIndex(this.logStore.getFirstAvailableIndex() - 1);
        leaveClusterRequest.setLastLogTerm(0);
        leaveClusterRequest.setTerm(this.state.getTerm());
        leaveClusterRequest.setMessageType(RaftMessageType.LeaveClusterRequest);
        leaveClusterRequest.setSource(this.id);
        peer.SendRequest(leaveClusterRequest).whenCompleteAsync((RaftResponseMessage peerResponse, Throwable error) -> {
            this.handleExtendedResponse(peerResponse, error);
        }, this.context.getScheduledExecutor());
        response.setAccepted(true);
        return response;
    }

    private RaftResponseMessage handleAddServerRequest(RaftRequestMessage request){
        LogEntry[] logEntries = request.getLogEntries();
        RaftResponseMessage response = new RaftResponseMessage();
        response.setSource(this.id);
        response.setDestination(this.leader);
        response.setTerm(this.state.getTerm());
        response.setMessageType(RaftMessageType.AddServerResponse);
        response.setNextIndex(this.logStore.getFirstAvailableIndex());
        response.setAccepted(false);
        if(logEntries.length != 1 || logEntries[0].getValueType() != LogValueType.ClusterServer){
            this.logger.info("bad add server request as we are expecting one log entry with value type of ClusterServer");
            return response;
        }

        if(this.role != ServerRole.Leader){
            this.logger.info("this is not a leader, cannot handle AddServerRequest");
            return response;
        }

        ClusterServer server = new ClusterServer(ByteBuffer.wrap(logEntries[0].getValue()));
        if(this.peers.containsKey(server.getId()) || this.id == server.getId()){
            this.logger.warning("the server to be added has a duplicated id with existing server %d", server.getId());
            return response;
        }

        if(this.configChanging){
            // the previous config has not committed yet
            this.logger.info("previous config has not committed yet");
            return response;
        }

        this.serverToJoin = new PeerServer(server, this.context, peerServer -> {
            this.handleHeartbeatTimeout(peerServer);
        });

        this.inviteServerToJoinCluster();
        response.setNextIndex(this.logStore.getFirstAvailableIndex());
        response.setAccepted(true);
        return response;
    }

    private RaftResponseMessage handleLogSyncRequest(RaftRequestMessage request){
        LogEntry[] logEntries = request.getLogEntries();
        RaftResponseMessage response = new RaftResponseMessage();
        response.setSource(this.id);
        response.setDestination(request.getSource());
        response.setTerm(this.state.getTerm());
        response.setMessageType(RaftMessageType.SyncLogResponse);
        response.setNextIndex(this.logStore.getFirstAvailableIndex());
        response.setAccepted(false);
        if(logEntries == null ||
                logEntries.length != 1 ||
                logEntries[0].getValueType() != LogValueType.LogPack ||
                logEntries[0].getValue() == null ||
                logEntries[0].getValue().length == 0){
            this.logger.info("receive an invalid LogSyncRequest as the log entry value doesn't meet the requirements");
            return response;
        }

        if(!this.catchingUp){
            this.logger.debug("This server is ready for cluster, ignore the request");
            return response;
        }

        this.logStore.applyLogPack(request.getLastLogIndex() + 1, logEntries[0].getValue());
        this.commit(this.logStore.getFirstAvailableIndex() -1);
        response.setNextIndex(this.logStore.getFirstAvailableIndex());
        response.setAccepted(true);
        return response;
    }

    private void syncLogsToNewComingServer(long startIndex){
        // only sync committed logs
        int gap = (int)(this.quickCommitIndex - startIndex);
        if(gap < this.context.getRaftParameters().getLogSyncStopGap()){

            this.logger.info("LogSync is done for server %d with log gap %d, now put the server into cluster", this.serverToJoin.getId(), gap);
            ClusterConfiguration newConfig = new ClusterConfiguration();
            newConfig.setLastLogIndex(this.config.getLogIndex());
            newConfig.setLogIndex(this.logStore.getFirstAvailableIndex());
            newConfig.getServers().addAll(this.config.getServers());
            newConfig.getServers().add(this.serverToJoin.getClusterConfig());
            LogEntry configEntry = new LogEntry(this.state.getTerm(), newConfig.toBytes(), LogValueType.Configuration);
            this.logStore.append(configEntry);
            this.configChanging = true;
            this.requestAppendEntries();
            return;
        }

        RaftRequestMessage request = null;
        if(startIndex > 0 && startIndex < this.logStore.getStartIndex()){
            request = this.createSyncSnapshotRequest(this.serverToJoin, startIndex, this.state.getTerm(), this.quickCommitIndex);

        }else{
            int sizeToSync = Math.min(gap, this.context.getRaftParameters().getLogSyncBatchSize());
            byte[] logPack = this.logStore.packLog(startIndex, sizeToSync);
            request = new RaftRequestMessage();
            request.setCommitIndex(this.quickCommitIndex);
            request.setDestination(this.serverToJoin.getId());
            request.setSource(this.id);
            request.setTerm(this.state.getTerm());
            request.setMessageType(RaftMessageType.SyncLogRequest);
            request.setLastLogIndex(startIndex - 1);
            request.setLogEntries(new LogEntry[] { new LogEntry(this.state.getTerm(), logPack, LogValueType.LogPack) });
        }

        this.serverToJoin.SendRequest(request).whenCompleteAsync((RaftResponseMessage response, Throwable error) -> {
            this.handleExtendedResponse(response, error);
        }, this.context.getScheduledExecutor());
    }

    private void inviteServerToJoinCluster(){
        RaftRequestMessage request = new RaftRequestMessage();
        request.setCommitIndex(this.quickCommitIndex);
        request.setDestination(this.serverToJoin.getId());
        request.setSource(this.id);
        request.setTerm(this.state.getTerm());
        request.setMessageType(RaftMessageType.JoinClusterRequest);
        request.setLastLogIndex(this.logStore.getFirstAvailableIndex() - 1);
        request.setLogEntries(new LogEntry[] { new LogEntry(this.state.getTerm(), this.config.toBytes(), LogValueType.Configuration) });
        this.serverToJoin.SendRequest(request).whenCompleteAsync((RaftResponseMessage response, Throwable error) -> {
            this.handleExtendedResponse(response, error);
        }, this.context.getScheduledExecutor());
    }

    private RaftResponseMessage handleJoinClusterRequest(RaftRequestMessage request){
        LogEntry[] logEntries = request.getLogEntries();
        RaftResponseMessage response = new RaftResponseMessage();
        response.setSource(this.id);
        response.setDestination(request.getSource());
        response.setTerm(this.state.getTerm());
        response.setMessageType(RaftMessageType.JoinClusterResponse);
        response.setNextIndex(this.logStore.getFirstAvailableIndex());
        response.setAccepted(false);
        if(logEntries == null ||
                logEntries.length != 1 ||
                logEntries[0].getValueType() != LogValueType.Configuration ||
                logEntries[0].getValue() == null ||
                logEntries[0].getValue().length == 0){
            this.logger.info("receive an invalid JoinClusterRequest as the log entry value doesn't meet the requirements");
            return response;
        }

        if(this.catchingUp){
            this.logger.info("this server is already in log syncing mode");
            return response;
        }

        this.catchingUp = true;
        this.role = ServerRole.Follower;
        this.leader = request.getSource();
        this.state.setTerm(request.getTerm());
        this.state.setCommitIndex(0);
        this.quickCommitIndex = 0;
        this.state.setVotedFor(-1);
        this.context.getServerStateManager().persistState(this.state);
        this.stopElectionTimer();
        ClusterConfiguration newConfig = ClusterConfiguration.fromBytes(logEntries[0].getValue());
        this.reconfigure(newConfig);
        response.setTerm(this.state.getTerm());
        response.setAccepted(true);
        return response;
    }

    private RaftResponseMessage handleLeaveClusterRequest(RaftRequestMessage request){
        RaftResponseMessage response = new RaftResponseMessage();
        response.setSource(this.id);
        response.setDestination(request.getSource());
        response.setTerm(this.state.getTerm());
        response.setMessageType(RaftMessageType.LeaveClusterResponse);
        response.setNextIndex(this.logStore.getFirstAvailableIndex());
        if(!this.configChanging){
            this.steppingDown = 2;
            response.setAccepted(true);
        }else{
            response.setAccepted(false);
        }
        
        return response;
    }

    private void removeServerFromCluster(int serverId){
        ClusterConfiguration newConfig = new ClusterConfiguration();
        newConfig.setLastLogIndex(this.config.getLogIndex());
        newConfig.setLogIndex(this.logStore.getFirstAvailableIndex());
        for(ClusterServer server: this.config.getServers()){
            if(server.getId() != serverId){
                newConfig.getServers().add(server);
            }
        }

        this.logger.info("removed a server from configuration and save the configuration to log store at %d", newConfig.getLogIndex());
        this.configChanging = true;
        this.logStore.append(new LogEntry(this.state.getTerm(), newConfig.toBytes(), LogValueType.Configuration));
        this.requestAppendEntries();
    }

    private int getSnapshotSyncBlockSize(){
        int blockSize = this.context.getRaftParameters().getSnapshotBlockSize();
        return blockSize == 0 ? DEFAULT_SNAPSHOT_SYNC_BLOCK_SIZE : blockSize;
    }

    private RaftRequestMessage createSyncSnapshotRequest(PeerServer peer, long lastLogIndex, long term, long commitIndex){
        synchronized(peer){
            SnapshotSyncContext context = peer.getSnapshotSyncContext();
            Snapshot snapshot = context == null ? null : context.getSnapshot();
            Snapshot lastSnapshot = this.stateMachine.getLastSnapshot();
            if(snapshot == null || (lastSnapshot != null && lastSnapshot.getLastLogIndex() > snapshot.getLastLogIndex())){
                snapshot = lastSnapshot;

                if(snapshot == null || lastLogIndex > snapshot.getLastLogIndex()){
                    this.logger.error("system is running into fatal errors, failed to find a snapshot for peer %d(snapshot null: %s, snapshot doesn't contais lastLogIndex: %s)", peer.getId(), String.valueOf(snapshot == null), String.valueOf(lastLogIndex > snapshot.getLastLogIndex()));
                    this.stateMachine.exit(-1);
                    return null;
                }

                if(snapshot.getSize() < 1L){
                    this.logger.error("invalid snapshot, this usually means a bug from state machine implementation, stop the system to prevent further errors");
                    this.stateMachine.exit(-1);
                    return null;
                }

                this.logger.info("trying to sync snapshot with last index %d to peer %d", snapshot.getLastLogIndex(), peer.getId());
                peer.setSnapshotInSync(snapshot);
            }

            long offset = peer.getSnapshotSyncContext().getOffset();
            long sizeLeft = snapshot.getSize() - offset;
            int blockSize = this.getSnapshotSyncBlockSize();
            byte[] data = new byte[sizeLeft > blockSize ? blockSize : (int)sizeLeft];
            try{
                int sizeRead = this.stateMachine.readSnapshotData(snapshot, offset, data);
                if(sizeRead < data.length){
                    this.logger.error("only %d bytes could be read from snapshot while %d bytes are expected, should be something wrong" , sizeRead, data.length);
                    this.stateMachine.exit(-1);
                    return null;
                }
            }catch(Throwable error){
                // if there is i/o error, no reason to continue
                this.logger.error("failed to read snapshot data due to io error %s", error.toString());
                this.stateMachine.exit(-1);
                return null;
            }
            SnapshotSyncRequest syncRequest = new SnapshotSyncRequest(snapshot, offset, data, (offset + data.length) >= snapshot.getSize());
            RaftRequestMessage requestMessage = new RaftRequestMessage();
            requestMessage.setMessageType(RaftMessageType.InstallSnapshotRequest);
            requestMessage.setSource(this.id);
            requestMessage.setDestination(peer.getId());
            requestMessage.setLastLogIndex(snapshot.getLastLogIndex());
            requestMessage.setLastLogTerm(snapshot.getLastLogTerm());
            requestMessage.setLogEntries(new LogEntry[] { new LogEntry(term, syncRequest.toBytes(), LogValueType.SnapshotSyncRequest) });
            requestMessage.setCommitIndex(commitIndex);
            requestMessage.setTerm(term);
            return requestMessage;
        }
    }

    private long termForLastLog(long logIndex){
        if(logIndex == 0){
            return 0;
        }

        if(logIndex >= this.logStore.getStartIndex()){
            return this.logStore.getLogEntryAt(logIndex).getTerm();
        }

        Snapshot lastSnapshot = this.stateMachine.getLastSnapshot();
        if(lastSnapshot == null || logIndex != lastSnapshot.getLastLogIndex()){
            throw new IllegalArgumentException("logIndex is beyond the range that no term could be retrieved");
        }

        return lastSnapshot.getLastLogTerm();
    }

    static class RaftMessageSenderImpl implements RaftMessageSender {

        private RaftServer server;
        private Map<Integer, RpcClient> rpcClients;

        RaftMessageSenderImpl(RaftServer server){
            this.server = server;
            this.rpcClients = new ConcurrentHashMap<Integer, RpcClient>();
        }

        @Override
        public CompletableFuture<Boolean> addServer(ClusterServer server) {
            LogEntry[] logEntries = new LogEntry[1];
            logEntries[0] = new LogEntry(0, server.toBytes(), LogValueType.ClusterServer);
            RaftRequestMessage request = new RaftRequestMessage();
            request.setMessageType(RaftMessageType.AddServerRequest);
            request.setLogEntries(logEntries);
            return this.sendMessageToLeader(request);
        }

        @Override
        public CompletableFuture<Boolean> removeServer(int serverId) {
            if(serverId < 0){
                return CompletableFuture.completedFuture(false);
            }

            ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
            buffer.putInt(serverId);
            LogEntry[] logEntries = new LogEntry[1];
            logEntries[0] = new LogEntry(0, buffer.array(), LogValueType.ClusterServer);
            RaftRequestMessage request = new RaftRequestMessage();
            request.setMessageType(RaftMessageType.RemoveServerRequest);
            request.setLogEntries(logEntries);
            return this.sendMessageToLeader(request);
        }

        @Override
        public CompletableFuture<Boolean> appendEntries(byte[][] values) {
            if(values == null || values.length == 0){
                return CompletableFuture.completedFuture(false);
            }

            LogEntry[] logEntries = new LogEntry[values.length];
            for(int i = 0; i < values.length; ++i){
                logEntries[i] = new LogEntry(0, values[i]);
            }

            RaftRequestMessage request = new RaftRequestMessage();
            request.setMessageType(RaftMessageType.ClientRequest);
            request.setLogEntries(logEntries);
            return this.sendMessageToLeader(request);
        }

        private CompletableFuture<Boolean> sendMessageToLeader(RaftRequestMessage request){
            int leaderId = this.server.leader;
            ClusterConfiguration config = this.server.config;
            if(leaderId == -1){
                return CompletableFuture.completedFuture(false);
            }

            if(leaderId == this.server.id){
                return CompletableFuture.completedFuture(this.server.processRequest(request).isAccepted());
            }

            CompletableFuture<Boolean> result = new CompletableFuture<Boolean>();
            RpcClient rpcClient = this.rpcClients.get(leaderId);
            if(rpcClient == null){
                ClusterServer leader = config.getServer(leaderId);
                if(leader == null){
                    result.complete(false);
                    return result;
                }

                rpcClient = this.server.context.getRpcClientFactory().createRpcClient(leader.getEndpoint());
                this.rpcClients.put(leaderId, rpcClient);
            }

            rpcClient.send(request).whenCompleteAsync((RaftResponseMessage response, Throwable err) -> {
                if(err != null){
                    this.server.logger.info("Received an rpc error %s while sending a request to server (%d)", err.getMessage(), leaderId);
                    result.complete(false);
                }else{
                    result.complete(response.isAccepted());
                }
            }, this.server.context.getScheduledExecutor());

            return result;
        }
    }

    static class CommittingThread implements Runnable{

        private RaftServer server;
        private Object conditionalLock;

        CommittingThread(RaftServer server){
            this.server = server;
            this.conditionalLock = new Object();
        }

        void moreToCommit(){
            synchronized(this.conditionalLock){
                this.conditionalLock.notify();
            }
        }

        @Override
        public void run() {
            while(true){
                try{
                    long currentCommitIndex = server.state.getCommitIndex();
                    while(server.quickCommitIndex <= currentCommitIndex
                            || currentCommitIndex >= server.logStore.getFirstAvailableIndex() - 1){
                        synchronized(this.conditionalLock){
                            this.conditionalLock.wait();
                        }

                        currentCommitIndex = server.state.getCommitIndex();
                    }

                    while(currentCommitIndex < server.quickCommitIndex && currentCommitIndex < server.logStore.getFirstAvailableIndex() - 1){
                        currentCommitIndex += 1;
                        LogEntry logEntry = server.logStore.getLogEntryAt(currentCommitIndex);
                        if(logEntry.getValueType() == LogValueType.Application){
                            server.stateMachine.commit(currentCommitIndex, logEntry.getValue());
                        }else if(logEntry.getValueType() == LogValueType.Configuration){
                            synchronized(server){
                                ClusterConfiguration newConfig = ClusterConfiguration.fromBytes(logEntry.getValue());
                                server.logger.info("configuration at index %d is committed", newConfig.getLogIndex());
                                server.context.getServerStateManager().saveClusterConfiguration(newConfig);
                                server.configChanging = false;
                                if(server.config.getLogIndex() < newConfig.getLogIndex()){
                                    server.reconfigure(newConfig);
                                }
                                
                                if(server.catchingUp && newConfig.getServer(server.id) != null){
                                    server.logger.info("this server is committed as one of cluster members");
                                    server.catchingUp = false;
                                }
                            }
                        }

                        server.state.setCommitIndex(currentCommitIndex);
                        server.snapshotAndCompact(currentCommitIndex);
                    }

                    server.context.getServerStateManager().persistState(server.state);
                }catch(Throwable error){
                    server.logger.error("error %s encountered for committing thread, which should not happen, according to this, state machine may not have further progress, stop the system", error, error.getMessage());
                    server.stateMachine.exit(-1);
                }
            }
        }

    }
}
