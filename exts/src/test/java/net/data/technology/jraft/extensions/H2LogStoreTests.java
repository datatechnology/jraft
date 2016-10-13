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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import org.junit.Test;

import net.data.technology.jraft.LogEntry;
import net.data.technology.jraft.LogValueType;

public class H2LogStoreTests {

    private Random random = new Random(Calendar.getInstance().getTimeInMillis());

    @Test
    public void testPackAndUnpack() throws IOException {
        Path container = Files.createTempDirectory("logstore");
        Files.deleteIfExists(container.resolve("test.mv.db"));
        Files.deleteIfExists(container.resolve("test.trace.db"));
        Path container1 = Files.createTempDirectory("logstore");
        Files.deleteIfExists(container1.resolve("test.mv.db"));
        Files.deleteIfExists(container1.resolve("test.trace.db"));
        H2LogStore store = new H2LogStore(container.resolve("test").toString());
        H2LogStore store1 = new H2LogStore(container1.resolve("test").toString());
        int logsCount = this.random.nextInt(1000) + 1000;
        for(int i = 0; i < logsCount; ++i){
            store.append(this.randomLogEntry());
            store1.append(this.randomLogEntry());
        }

        int logsCopied = 0;
        while(logsCopied < logsCount){
            byte[] pack = store.packLog(logsCopied + 1, 100);
            store1.applyLogPack(logsCopied + 1, pack);
            logsCopied = Math.min(logsCopied + 100,  logsCount);
        }

        assertEquals(store.getFirstAvailableIndex(), store1.getFirstAvailableIndex());
        for(int i = 1; i <= logsCount; ++i){
            LogEntry entry1 = store.getLogEntryAt(i);
            LogEntry entry2 = store1.getLogEntryAt(i);
            assertTrue("the " + String.valueOf(i) + "th value are not equal(total: " + String.valueOf(logsCount) + ")", logEntriesEquals(entry1, entry2));
        }

        store.close();
        store1.close();

        Files.deleteIfExists(container.resolve("test.mv.db"));
        Files.deleteIfExists(container.resolve("test.trace.db"));
        Files.deleteIfExists(container);
        Files.deleteIfExists(container1.resolve("test.mv.db"));
        Files.deleteIfExists(container1.resolve("test.trace.db"));
        Files.deleteIfExists(container1);
    }

    @Test
    public void testStore() throws IOException {
        Path container = Files.createTempDirectory("logstore");
        Files.deleteIfExists(container.resolve("test.mv.db"));
        Files.deleteIfExists(container.resolve("test.trace.db"));
        H2LogStore store = new H2LogStore(container.resolve("test").toString());
        assertTrue(store.getLastLogEntry().getTerm() == 0);
        assertTrue(store.getLastLogEntry().getValue() == null);
        assertEquals(1, store.getFirstAvailableIndex());
        assertTrue(store.getLogEntryAt(1) == null);

        // write some logs
        List<LogEntry> entries = new LinkedList<LogEntry>();
        for(int i = 0; i < this.random.nextInt(100) + 10; ++i){
            LogEntry entry = this.randomLogEntry();
            entries.add(entry);
            store.append(entry);
        }

        assertEquals(entries.size(), store.getFirstAvailableIndex() - 1);
        assertTrue(logEntriesEquals(entries.get(entries.size() - 1), store.getLastLogEntry()));

        // random item
        int randomIndex = this.random.nextInt(entries.size());
        assertTrue(logEntriesEquals(entries.get(randomIndex), store.getLogEntryAt(randomIndex + 1))); // log store's index starts from 1

        // random range
        randomIndex = this.random.nextInt(entries.size() - 1);
        int randomSize = this.random.nextInt(entries.size() - randomIndex);
        LogEntry[] logEntries = store.getLogEntries(randomIndex + 1, randomIndex + 1 + randomSize);
        for(int i = randomIndex; i < randomIndex + randomSize; ++i){
            assertTrue(logEntriesEquals(entries.get(i), logEntries[i - randomIndex]));
        }

        store.close();
        store = new H2LogStore(container.resolve("test").toString());

        assertEquals(entries.size(), store.getFirstAvailableIndex() - 1);
        assertTrue(logEntriesEquals(entries.get(entries.size() - 1), store.getLastLogEntry()));

        // random item
        randomIndex = this.random.nextInt(entries.size());
        assertTrue(logEntriesEquals(entries.get(randomIndex), store.getLogEntryAt(randomIndex + 1))); // log store's index starts from 1

        // random range
        randomIndex = this.random.nextInt(entries.size() - 1);
        randomSize = this.random.nextInt(entries.size() - randomIndex);
        logEntries = store.getLogEntries(randomIndex + 1, randomIndex + 1 + randomSize);
        for(int i = randomIndex; i < randomIndex + randomSize; ++i){
            assertTrue(logEntriesEquals(entries.get(i), logEntries[i - randomIndex]));
        }

        // test with edge
        randomSize = this.random.nextInt(entries.size());
        logEntries = store.getLogEntries(store.getFirstAvailableIndex() - randomSize, store.getFirstAvailableIndex());
        for(int i = entries.size() - randomSize, j = 0; i < entries.size(); ++i, ++j){
            assertTrue(logEntriesEquals(entries.get(i), logEntries[j]));
        }

        // test write at
        LogEntry logEntry = this.randomLogEntry();
        randomIndex = this.random.nextInt((int)store.getFirstAvailableIndex());
        store.writeAt(store.getStartIndex() + randomIndex, logEntry);
        assertEquals(randomIndex + 1 + store.getStartIndex(), store.getFirstAvailableIndex());
        assertTrue(logEntriesEquals(logEntry, store.getLastLogEntry()));

        store.close();
        Files.deleteIfExists(container.resolve("test.mv.db"));
        Files.deleteIfExists(container.resolve("test.trace.db"));
        Files.deleteIfExists(container);
    }

    @Test
    public void testCompactRandom() throws Exception{
        Path container = Files.createTempDirectory("logstore");
        Files.deleteIfExists(container.resolve("test.mv.db"));
        Files.deleteIfExists(container.resolve("test.trace.db"));
        H2LogStore store = new H2LogStore(container.resolve("test").toString());

        // write some logs
        List<LogEntry> entries = new LinkedList<LogEntry>();
        for(int i = 0; i < 300; ++i){
            LogEntry entry = this.randomLogEntry();
            entries.add(entry);
            store.append(entry);
        }

        long lastLogIndex = entries.size();
        long indexToCompact = this.random.nextInt((int)lastLogIndex - 10) + 1;
        store.compact(indexToCompact);

        assertEquals(indexToCompact + 1, store.getStartIndex());
        assertEquals(entries.size(), store.getFirstAvailableIndex() - 1);

        for(int i = 0; i < store.getFirstAvailableIndex() - indexToCompact - 1; ++i){
            LogEntry entry = store.getLogEntryAt(store.getStartIndex() + i);
            assertTrue(logEntriesEquals(entries.get(i + (int)indexToCompact), entry));
        }

        int randomIndex = this.random.nextInt((int)(store.getFirstAvailableIndex() - indexToCompact - 1));
        LogEntry logEntry = this.randomLogEntry();
        store.writeAt(store.getStartIndex() + randomIndex, logEntry);
        entries.set(randomIndex + (int)indexToCompact, logEntry);
        while(entries.size() > randomIndex + (int)indexToCompact + 1){
            entries.remove(randomIndex + (int)indexToCompact + 1);
        }

        for(int i = 0; i < store.getFirstAvailableIndex() - indexToCompact - 1; ++i){
            LogEntry entry = store.getLogEntryAt(store.getStartIndex() + i);
            assertTrue(logEntriesEquals(entries.get(i + (int)indexToCompact), entry));
        }

        for(int i = 0; i < this.random.nextInt(100) + 10; ++i){
            LogEntry entry = this.randomLogEntry();
            entries.add(entry);
            store.append(entry);
        }

        for(int i = 0; i < store.getFirstAvailableIndex() - indexToCompact - 1; ++i){
            LogEntry entry = store.getLogEntryAt(store.getStartIndex() + i);
            assertTrue(logEntriesEquals(entries.get(i + (int)indexToCompact), entry));
        }

        store.close();
        Files.deleteIfExists(container.resolve("test.mv.db"));
        Files.deleteIfExists(container.resolve("test.trace.db"));
        Files.deleteIfExists(container);
    }

    @Test
    public void testCompactAll() throws Exception{
        Path container = Files.createTempDirectory("logstore");
        Files.deleteIfExists(container.resolve("test.mv.db"));
        Files.deleteIfExists(container.resolve("test.trace.db"));
        H2LogStore store = new H2LogStore(container.resolve("test").toString());

        // write some logs
        List<LogEntry> entries = new LinkedList<LogEntry>();
        for(int i = 0; i < this.random.nextInt(1000) + 100; ++i){
            LogEntry entry = this.randomLogEntry();
            entries.add(entry);
            store.append(entry);
        }

        assertEquals(1, store.getStartIndex());
        assertEquals(entries.size(), store.getFirstAvailableIndex() - 1);
        assertTrue(logEntriesEquals(entries.get(entries.size() - 1), store.getLastLogEntry()));
        long lastLogIndex = entries.size();
        store.compact(lastLogIndex);

        assertEquals(entries.size() + 1, store.getStartIndex());
        assertEquals(entries.size(), store.getFirstAvailableIndex() - 1);

        for(int i = 0; i < this.random.nextInt(100) + 10; ++i){
            LogEntry entry = this.randomLogEntry();
            entries.add(entry);
            store.append(entry);
        }

        assertEquals(lastLogIndex + 1, store.getStartIndex());
        assertEquals(entries.size(), store.getFirstAvailableIndex() - 1);
        assertTrue(logEntriesEquals(entries.get(entries.size() - 1), store.getLastLogEntry()));

        long index = store.getStartIndex() + this.random.nextInt((int)(store.getFirstAvailableIndex() - store.getStartIndex()));
        assertTrue(logEntriesEquals(entries.get((int)index - 1), store.getLogEntryAt(index)));

        store.close();
        Files.deleteIfExists(container.resolve("test.mv.db"));
        Files.deleteIfExists(container.resolve("test.trace.db"));
        Files.deleteIfExists(container);
    }

    private LogEntry randomLogEntry(){
        byte[] value = new byte[this.random.nextInt(20) + 1];
        long term = this.random.nextLong();
        this.random.nextBytes(value);
        LogValueType type = LogValueType.fromByte((byte)(this.random.nextInt(4) + 1));
        return new LogEntry(term, value, type);
    }

    private static boolean logEntriesEquals(LogEntry entry1, LogEntry entry2){
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

}
