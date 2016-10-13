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

package net.data.technology.jraft.extensions;

import static org.junit.Assert.*;

import java.util.Calendar;
import java.util.Random;

import org.junit.Test;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import net.data.technology.jraft.LogEntry;
import net.data.technology.jraft.LogValueType;
import net.data.technology.jraft.RaftMessageType;
import net.data.technology.jraft.RaftRequestMessage;
import net.data.technology.jraft.RaftResponseMessage;

public class GsonSerializationTests {
    private Random random = new Random(Calendar.getInstance().getTimeInMillis());

    @Test
    public void testResponseSerialization() {
        RaftResponseMessage response = new RaftResponseMessage();
        response.setMessageType(this.randomMessageType());
        response.setAccepted(this.random.nextBoolean());
        response.setDestination(this.random.nextInt());
        response.setSource(this.random.nextInt());
        response.setTerm(this.random.nextLong());
        response.setNextIndex(this.random.nextLong());

        Gson gson = new GsonBuilder().create();
        String payload = gson.toJson(response);
        RaftResponseMessage response1 = gson.fromJson(payload, RaftResponseMessage.class);
        assertEquals(response.getMessageType(), response1.getMessageType());
        assertEquals(response.isAccepted(), response1.isAccepted());
        assertEquals(response.getSource(), response1.getSource());
        assertEquals(response.getDestination(), response1.getDestination());
        assertEquals(response.getTerm(), response1.getTerm());
        assertEquals(response.getNextIndex(), response1.getNextIndex());
    }

    @Test
    public void testRequestSerialization() {
        RaftRequestMessage request = new RaftRequestMessage();
        request.setMessageType(this.randomMessageType());;
        request.setCommitIndex(this.random.nextLong());
        request.setDestination(this.random.nextInt());
        request.setLastLogIndex(this.random.nextLong());
        request.setLastLogTerm(this.random.nextLong());
        request.setSource(this.random.nextInt());
        request.setTerm(this.random.nextLong());
        LogEntry[] entries = new LogEntry[this.random.nextInt(20) + 1];
        for(int i = 0; i < entries.length; ++i){
            entries[i] = this.randomLogEntry();
        }

        request.setLogEntries(entries);
        Gson gson = new GsonBuilder().create();
        String payload = gson.toJson(request);
        RaftRequestMessage request1 = gson.fromJson(payload, RaftRequestMessage.class);
        assertEquals(request.getMessageType(), request1.getMessageType());
        assertEquals(request.getCommitIndex(), request1.getCommitIndex());
        assertEquals(request.getDestination(), request1.getDestination());
        assertEquals(request.getLastLogIndex(), request1.getLastLogIndex());
        assertEquals(request.getLastLogTerm(), request1.getLastLogTerm());
        assertEquals(request.getSource(), request1.getSource());
        assertEquals(request.getTerm(), request1.getTerm());
        for(int i = 0; i < entries.length; ++i){
            assertTrue(this.logEntriesEquals(entries[i], request1.getLogEntries()[i]));
        }
    }

    private boolean logEntriesEquals(LogEntry entry1, LogEntry entry2){
        boolean equals = entry1.getTerm() == entry2.getTerm() && entry1.getValueType() == entry2.getValueType();
        equals = equals && ((entry1.getValue() != null && entry2.getValue() != null && entry1.getValue().length == entry2.getValue().length) || (entry1.getValue() == null && entry2.getValue() == null));
        if(entry1.getValue() != null){
            int i = 0;
            while(equals && i < entry1.getValue().length){
                equals = entry1.getValue()[i] == entry2.getValue()[i];
                ++i;
            }
        }

        return equals;
    }

    private RaftMessageType randomMessageType(){
        byte value = (byte)this.random.nextInt(5);
        return RaftMessageType.fromByte((byte) (value + 1));
    }

    private LogEntry randomLogEntry(){
        byte[] value = new byte[this.random.nextInt(20) + 1];
        long term = this.random.nextLong();
        this.random.nextBytes(value);
        return new LogEntry(term, value, LogValueType.fromByte((byte)(this.random.nextInt(4) + 1)));
    }
}
