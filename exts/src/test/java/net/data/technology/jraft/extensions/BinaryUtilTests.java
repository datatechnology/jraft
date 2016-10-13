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

import net.data.technology.jraft.LogEntry;
import net.data.technology.jraft.LogValueType;
import net.data.technology.jraft.RaftMessageType;
import net.data.technology.jraft.RaftRequestMessage;
import net.data.technology.jraft.RaftResponseMessage;

public class BinaryUtilTests {
    private Random random = new Random(Calendar.getInstance().getTimeInMillis());

    @Test
    public void testIntegerConverter() {
        int value = random.nextInt();
        byte[] buffer = BinaryUtils.intToBytes(value);
        int restoredValue = BinaryUtils.bytesToInt(buffer, 0);
        assertEquals(value, restoredValue);
    }

    @Test
    public void testLongConverter(){
        long value = random.nextLong();
        byte[] buffer = BinaryUtils.longToBytes(value);
        long restoredValue = BinaryUtils.bytesToLong(buffer, 0);
        assertEquals(value, restoredValue);
    }

    @Test
    public void testBooleanConverter(){
        assertEquals((byte)1, BinaryUtils.booleanToByte(true));
        assertEquals((byte)0, BinaryUtils.booleanToByte(false));
        assertEquals(true, BinaryUtils.byteToBoolean((byte)1));
        assertEquals(false, BinaryUtils.byteToBoolean((byte)0));
    }

    @Test
    public void testLogEntryConverter(){
        LogEntry logEntry = this.randomLogEntry();
        byte[] data = BinaryUtils.logEntryToBytes(logEntry);
        LogEntry[] entries = BinaryUtils.bytesToLogEntries(data);
        assertTrue(entries != null);
        assertEquals(1, entries.length);
        assertTrue(logEntriesEquals(logEntry, entries[0]));
    }

    @Test
    public void testResponseConverter(){
        RaftResponseMessage response = new RaftResponseMessage();
        response.setMessageType(this.randomMessageType());
        response.setAccepted(this.random.nextBoolean());
        response.setDestination(this.random.nextInt());
        response.setSource(this.random.nextInt());
        response.setTerm(this.random.nextLong());
        response.setNextIndex(this.random.nextLong());

        byte[] data = BinaryUtils.messageToBytes(response);
        RaftResponseMessage response1 = BinaryUtils.bytesToResponseMessage(data);
        assertEquals(response.getMessageType(), response1.getMessageType());
        assertEquals(response.isAccepted(), response1.isAccepted());
        assertEquals(response.getSource(), response1.getSource());
        assertEquals(response.getDestination(), response1.getDestination());
        assertEquals(response.getTerm(), response1.getTerm());
        assertEquals(response.getNextIndex(), response1.getNextIndex());
    }

    @Test
    public void testRequestConverter(){
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
        byte[] data = BinaryUtils.messageToBytes(request);
        byte[] header = new byte[BinaryUtils.RAFT_REQUEST_HEADER_SIZE];
        System.arraycopy(data, 0, header, 0, header.length);
        byte[] logData = new byte[data.length - BinaryUtils.RAFT_REQUEST_HEADER_SIZE];
        System.arraycopy(data, BinaryUtils.RAFT_REQUEST_HEADER_SIZE, logData, 0, logData.length);
        Pair<RaftRequestMessage, Integer> result = BinaryUtils.bytesToRequestMessage(header);
        assertEquals(logData.length, result.getSecond().intValue());
        result.getFirst().setLogEntries(BinaryUtils.bytesToLogEntries(logData));
        assertEquals(request.getMessageType(), result.getFirst().getMessageType());
        assertEquals(request.getCommitIndex(), result.getFirst().getCommitIndex());
        assertEquals(request.getDestination(), result.getFirst().getDestination());
        assertEquals(request.getLastLogIndex(), result.getFirst().getLastLogIndex());
        assertEquals(request.getLastLogTerm(), result.getFirst().getLastLogTerm());
        assertEquals(request.getSource(), result.getFirst().getSource());
        assertEquals(request.getTerm(), result.getFirst().getTerm());
        for(int i = 0; i < entries.length; ++i){
            assertTrue(this.logEntriesEquals(entries[i], result.getFirst().getLogEntries()[i]));
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
