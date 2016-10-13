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

public class RaftParameters {

    private int electionTimeoutUpperBound;
    private int electionTimeoutLowerBound;
    private int heartbeatInterval;
    private int rpcFailureBackoff;
    private int logSyncBatchSize;
    private int logSyncStopGap;
    private int snapshotDistance;
    private int snapshotBlockSize;
    private int maxAppendingSize;

    /**
     * The tcp block size for syncing the snapshots
     * @param size
     * @return self
     */
    public RaftParameters withSyncSnapshotBlockSize(int size){
        this.snapshotBlockSize = size;
        return this;
    }

    /**
     * Enable log compact and snapshot with the commit distance
     * @param distance, log distance to compact between two snapshots
     * @return self
     */
    public RaftParameters withSnapshotEnabled(int distance){
        this.snapshotDistance = distance;
        return this;
    }

    /**
     * For new member that just joined the cluster, we will use log sync to ask it to catch up,
     * and this parameter is to tell when to stop using log sync but appendEntries for the new server
     * when leaderCommitIndex - indexCaughtUp < logSyncStopGap, then appendEntries will be used
     * @param logSyncStopGap
     * @return self
     */
    public RaftParameters withLogSyncStoppingGap(int logSyncStopGap){
        this.logSyncStopGap = logSyncStopGap;
        return this;
    }

    /**
     * For new member that just joined the cluster, we will use log sync to ask it to catch up,
     * and this parameter is to specify how many log entries to pack for each sync request
     * @param logSyncBatchSize
     * @return self
     */
    public RaftParameters withLogSyncBatchSize(int logSyncBatchSize){
        this.logSyncBatchSize = logSyncBatchSize;
        return this;
    }

    /**
     * The maximum log entries could be attached to an appendEntries call
     * @param maxAppendingSize
     * @return self
     */
    public RaftParameters withMaximumAppendingSize(int maxAppendingSize){
        this.maxAppendingSize = maxAppendingSize;
        return this;
    }

    /**
     * Election timeout upper bound in milliseconds
     * @param electionTimeoutUpper
     * @return self
     */
    public RaftParameters withElectionTimeoutUpper(int electionTimeoutUpper){
        this.electionTimeoutUpperBound = electionTimeoutUpper;
        return this;
    }

    /**
     * Election timeout lower bound in milliseconds
     * @param electionTimeoutLower
     * @return self
     */
    public RaftParameters withElectionTimeoutLower(int electionTimeoutLower){
        this.electionTimeoutLowerBound = electionTimeoutLower;
        return this;
    }

    /**
     * heartbeat interval in milliseconds
     * @param heartbeatInterval
     * @return self
     */
    public RaftParameters withHeartbeatInterval(int heartbeatInterval){
        this.heartbeatInterval = heartbeatInterval;
        return this;
    }

    /**
     * Rpc failure backoff in milliseconds
     * @param rpcFailureBackoff
     * @return self
     */
    public RaftParameters withRpcFailureBackoff(int rpcFailureBackoff){
        this.rpcFailureBackoff = rpcFailureBackoff;
        return this;
    }

    /**
     * Upper value for election timeout
     * @return
     */
    public int getElectionTimeoutUpperBound() {
        return electionTimeoutUpperBound;
    }

    /**
     * Lower value for election timeout
     * @return
     */
    public int getElectionTimeoutLowerBound() {
        return electionTimeoutLowerBound;
    }

    /**
     * Heartbeat interval for each peer
     * @return
     */
    public int getHeartbeatInterval() {
        return heartbeatInterval;
    }

    /**
     * Rpc backoff for peers that failed to be connected
     * @return
     */
    public int getRpcFailureBackoff() {
        return rpcFailureBackoff;
    }

    /**
     * The maximum heartbeat interval, any value beyond this may lead to election timeout for a peer before receiving a heartbeat
     * @return
     */
    public int getMaxHeartbeatInterval(){
        return Math.max(this.heartbeatInterval, this.electionTimeoutLowerBound - this.heartbeatInterval / 2);
    }

    /**
     * The batch size for each ReplicateLogRequest message
     * @return
     */
    public int getLogSyncBatchSize() {
        return logSyncBatchSize;
    }

    /**
     * the max gap allowed for log sync, if the gap between the client and leader is less than this value,
     * the ReplicateLogRequest will be stopped
     * @return
     */
    public int getLogSyncStopGap() {
        return logSyncStopGap;
    }

    /**
     * The commit distances for snapshots, zero means don't take any snapshots
     * @return
     */
    public int getSnapshotDistance(){
        return this.snapshotDistance;
    }

    /**
     * The block size to sync while syncing snapshots to peers
     * @return
     */
    public int getSnapshotBlockSize() {
        return snapshotBlockSize;
    }

    /**
     * The maximum log entries in an appendEntries request
     * @return
     */
    public int getMaximumAppendingSize(){
        return this.maxAppendingSize;
    }
}
