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

public interface ServerStateManager {

    /**
     * Load cluster configuration for this server
     * @return the cluster configuration, never be null
     */
    public ClusterConfiguration loadClusterConfiguration();

    /**
     * Save the cluster configuration
     * @param configuration
     */
    public void saveClusterConfiguration(ClusterConfiguration configuration);

    /**
     * Save the server state
     * @param serverState
     */
    public void persistState(ServerState serverState);

    /**
     * Load server state
     * @return the server state, never be null
     */
    public ServerState readState();

    /**
     * Load the log store for current server
     * @return the log store, never be null
     */
    public SequentialLogStore loadLogStore();

    /**
     * Get current server id
     * @return server id for this server
     */
    public int getServerId();
}
