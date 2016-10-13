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

package net.data.technology.jraft.tests;

import static org.junit.Assert.*;

import java.util.Calendar;
import java.util.Random;

import org.junit.Test;

import net.data.technology.jraft.ClusterConfiguration;
import net.data.technology.jraft.ClusterServer;

public class ClusterConfigurationTests {

    @Test
    public void testSerialization() {
        ClusterConfiguration config = new ClusterConfiguration();
        Random random = new Random(Calendar.getInstance().getTimeInMillis());
        config.setLastLogIndex(random.nextLong());
        config.setLogIndex(random.nextLong());
        int servers = random.nextInt(10) + 1;
        for(int i = 0; i < servers; ++i){
            ClusterServer server = new ClusterServer();
            server.setId(random.nextInt());
            server.setEndpoint(String.format("Server %d", (i + 1)));
            config.getServers().add(server);
        }

        byte[] data = config.toBytes();
        ClusterConfiguration config1 = ClusterConfiguration.fromBytes(data);
        assertEquals(config.getLastLogIndex(), config1.getLastLogIndex());
        assertEquals(config.getLogIndex(), config1.getLogIndex());
        assertEquals(config.getServers().size(), config1.getServers().size());
        for(int i = 0; i < config.getServers().size(); ++i){
            ClusterServer s1 = config.getServers().get(i);
            ClusterServer s2 = config.getServers().get(i);
            assertEquals(s1.getId(), s2.getId());
            assertEquals(s1.getEndpoint(), s2.getEndpoint());
        }
    }

}
