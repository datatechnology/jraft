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

import java.util.concurrent.CompletableFuture;

public interface StateMachine {

	/**
	 * Starts the state machine, called by RaftConsensus, RaftConsensus will pass an instance of
	 * RaftMessageSender for the state machine to send logs to cluster, so that all state machines
	 * in the same cluster could be in synced
	 * @param raftMessageSender rpc message sender
	 */
	public void start(RaftMessageSender raftMessageSender);
	
    /**
     * Commit the log data at the {@code logIndex}
     * @param logIndex the log index in the logStore
     * @param data application data to commit
     */
    public void commit(long logIndex, byte[] data);

    /**
     * Rollback a preCommit item at index {@code logIndex}
     * @param logIndex log index to be rolled back
     * @param data application data to rollback
     */
    public void rollback(long logIndex, byte[] data);

    /**
     * PreCommit a log entry at log index {@code logIndex}
     * @param logIndex the log index to commit
     * @param data application data for pre-commit
     */
    public void preCommit(long logIndex, byte[] data);

    /**
     * Save data for the snapshot
     * @param snapshot the snapshot information
     * @param offset offset of the data in the whole snapshot
     * @param data part of snapshot data
     */
    public void saveSnapshotData(Snapshot snapshot, long offset, byte[] data);

    /**
     * Apply a snapshot to current state machine
     * @param snapshot the snapshot to be applied
     * @return true if successfully applied, otherwise false
     */
    public boolean applySnapshot(Snapshot snapshot);

    /**
     * Read snapshot data at the specified offset to buffer and return bytes read
     * @param snapshot the snapshot info
     * @param offset the offset of the snapshot data
     * @param buffer the buffer to be filled
     * @return bytes read
     */
    public int readSnapshotData(Snapshot snapshot, long offset, byte[] buffer);

    /**
     * Read the last snapshot information
     * @return last snapshot information in the state machine or null if none
     */
    public Snapshot getLastSnapshot();

    /**
     * Create a snapshot data based on the snapshot information asynchronously
     * set the future to true if snapshot is successfully created, otherwise, 
     * set it to false
     * @param snapshot the snapshot info
     * @return true if snapshot is created successfully, otherwise false
     */
    public CompletableFuture<Boolean> createSnapshot(Snapshot snapshot);
    
    /**
     * Save the state of state machine to ensure the state machine is in a good state, then exit the system
     * this MUST exits the system to protect the safety of the algorithm
     * @param code 0 indicates the system is gracefully shutdown, -1 indicates there are some errors which cannot be recovered
     */
    public void exit(int code);
}
