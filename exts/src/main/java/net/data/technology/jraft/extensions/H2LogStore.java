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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import net.data.technology.jraft.LogEntry;
import net.data.technology.jraft.LogValueType;
import net.data.technology.jraft.SequentialLogStore;

public class H2LogStore implements SequentialLogStore {

    private static final String TABLE_NAME = "LogStore";
    private static final String CREATE_SEQUENCE_SQL = "CREATE SEQUENCE LogSequence START WITH 1 INCREMENT BY 1";
    private static final String CREATE_TABLE_SQL = "CREATE TABLE LogStore(id bigint default LogSequence.nextval primary key, term bigint, dtype tinyint, data blob)";
    private static final String TRIM_TABLE_SQL = "DELETE FROM LogStore WHERE id > ?";
    private static final String COMPACT_TABLE_SQL = "DELETE FROM LogStore WHERE id <= ?";
    private static final String UPDATE_SEQUENCE_SQL = "ALTER SEQUENCE LogSequence RESTART WITH ? INCREMENT BY 1";
    private static final String INSERT_ENTRY_SQL = "INSERT INTO LogStore(term, dtype, data) values(?, ?, ?)";
    private static final String UPDATE_ENTRY_SQL = "UPDATE LogStore SET term=?,dtype=?,data=? WHERE id=?";
    private static final String SELECT_RANGE_SQL = "SELECT * FROM LogStore WHERE id >= ? AND id < ?";
    private static final String SELECT_ENTRY_SQL = "SELECT * FROM LogStore WHERE id=?";

    private Connection connection;
    private Logger logger;
    private AtomicLong startIndex;
    private AtomicLong nextIndex;
    private LogEntry lastEntry;

    public H2LogStore(String path){
        this.logger = LogManager.getLogger(this.getClass());
        this.startIndex = new AtomicLong();
        this.nextIndex = new AtomicLong();
        this.lastEntry = new LogEntry(0, null, LogValueType.Application);
        try{
            Class.forName("org.h2.Driver");
            this.connection = DriverManager.getConnection("jdbc:h2:" + path, "sa", "");
            this.connection.setAutoCommit(false);
            boolean isNew = true;
            ResultSet tables = this.connection.createStatement().executeQuery("SHOW TABLES");
            while(tables.next()){
                if(TABLE_NAME.equalsIgnoreCase(tables.getString(1))){
                    isNew = false;
                    break;
                }
            }

            tables.close();
            if(isNew){
                this.connection.createStatement().execute(CREATE_SEQUENCE_SQL);
                this.connection.createStatement().execute(CREATE_TABLE_SQL);
                this.connection.commit();
                this.startIndex.set(1);
                this.nextIndex.set(1);
            }else{
                ResultSet rs = this.connection.createStatement().executeQuery("SELECT MIN(id), MAX(id) FROM LogStore");
                if(rs.next()){
                    this.startIndex.set(rs.getLong(1));
                    this.nextIndex.set(rs.getLong(2) + 1);
                }else{
                    this.startIndex.set(1);
                    this.nextIndex.set(1);
                }

                rs.close();
                rs = this.connection.createStatement().executeQuery("SELECT TOP 1 * FROM LogStore ORDER BY id DESC");
                if(rs.next()){
                    this.lastEntry = new LogEntry(rs.getLong(2), rs.getBytes(4), LogValueType.fromByte(rs.getByte(3)));
                }

                rs.close();
            }
        }catch(Throwable error){
            this.logger.error("failed to load or create log store database", error);
            throw new RuntimeException("failed to load or create a log store", error);
        }
    }
    @Override
    public long getFirstAvailableIndex() {
        return this.nextIndex.get();
    }

    @Override
    public long getStartIndex() {
        return this.startIndex.get();
    }

    @Override
    public LogEntry getLastLogEntry() {
        return this.lastEntry;
    }

    @Override
    public long append(LogEntry logEntry) {
        try{
            PreparedStatement ps = this.connection.prepareStatement(INSERT_ENTRY_SQL);
            ps.setLong(1, logEntry.getTerm());
            ps.setByte(2, logEntry.getValueType().toByte());
            ps.setBytes(3, logEntry.getValue());
            ps.execute();
            this.connection.commit();
            this.lastEntry = logEntry;
            return this.nextIndex.getAndIncrement();
        }catch(Throwable error){
            this.logger.error("failed to insert a new entry", error);
            throw new RuntimeException("log store error", error);
        }
    }

    @Override
    public void writeAt(long index, LogEntry logEntry) {
        if(index >= this.nextIndex.get() || index < this.startIndex.get()){
            throw new IllegalArgumentException("index out of range");
        }

        try{
            PreparedStatement ps = this.connection.prepareStatement(UPDATE_ENTRY_SQL);
            ps.setLong(1, logEntry.getTerm());
            ps.setByte(2, logEntry.getValueType().toByte());
            ps.setBytes(3, logEntry.getValue());
            ps.setLong(4, index);
            ps.execute();
            ps = this.connection.prepareStatement(TRIM_TABLE_SQL);
            ps.setLong(1, index);
            ps.execute();
            ps = this.connection.prepareStatement(UPDATE_SEQUENCE_SQL);
            ps.setLong(1, index + 1);
            ps.execute();
            this.connection.commit();
            this.nextIndex.set(index + 1);
            this.lastEntry = logEntry;
        }catch(Throwable error){
            this.logger.error("failed to write an entry at a specific index", error);
            throw new RuntimeException("log store error", error);
        }
    }

    @Override
    public LogEntry[] getLogEntries(long start, long end) {
        if(start > end || start < this.startIndex.get()){
            throw new IllegalArgumentException("index out of range");
        }

        try{
            PreparedStatement ps = this.connection.prepareStatement(SELECT_RANGE_SQL);
            ps.setLong(1, start);
            ps.setLong(2, end);
            ResultSet rs = ps.executeQuery();
            List<LogEntry> entries = new ArrayList<LogEntry>();
            while(rs.next()){
                entries.add(new LogEntry(rs.getLong(2), rs.getBytes(4), LogValueType.fromByte(rs.getByte(3))));
            }

            rs.close();
            return entries.toArray(new LogEntry[0]);
        }catch(Throwable error){
            this.logger.error("failed to retrieve a range of entries", error);
            throw new RuntimeException("log store error", error);
        }
    }

    @Override
    public LogEntry getLogEntryAt(long index) {
        if(index < this.startIndex.get()){
            throw new IllegalArgumentException("index out of range");
        }

        try{
            PreparedStatement ps = this.connection.prepareStatement(SELECT_ENTRY_SQL);
            ps.setLong(1, index);
            ResultSet rs = ps.executeQuery();
            while(rs.next()){
                return new LogEntry(rs.getLong(2), rs.getBytes(4), LogValueType.fromByte(rs.getByte(3)));
            }

            rs.close();
            return null;
        }catch(Throwable error){
            this.logger.error("failed to retrieve an entry at a specific index", error);
            throw new RuntimeException("log store error", error);
        }
    }

    @Override
    public byte[] packLog(long index, int itemsToPack) {
        if(index < this.startIndex.get() || index >= this.nextIndex.get()){
            throw new IllegalArgumentException("index out of range");
        }

        try{
            ByteArrayOutputStream memoryStream = new ByteArrayOutputStream();
            GZIPOutputStream gzipStream = new GZIPOutputStream(memoryStream);
            PreparedStatement ps = this.connection.prepareStatement(SELECT_RANGE_SQL);
            ps.setLong(1, index);
            ps.setLong(2, index + itemsToPack);
            ResultSet rs = ps.executeQuery();
            while(rs.next()){
                byte[] value = rs.getBytes(4);
                int size = value.length + Long.BYTES + 1 + Integer.BYTES;
                ByteBuffer buffer = ByteBuffer.allocate(size);
                buffer.putInt(size);
                buffer.putLong(rs.getLong(2));
                buffer.put(rs.getByte(3));
                buffer.put(value);
                gzipStream.write(buffer.array());
            }

            rs.close();
            gzipStream.flush();
            memoryStream.flush();
            gzipStream.close();
            return memoryStream.toByteArray();
        }catch(Throwable error){
            this.logger.error("failed to pack log entries", error);
            throw new RuntimeException("log store error", error);
        }
    }

    @Override
    public void applyLogPack(long index, byte[] logPack) {
        if(index < this.startIndex.get()){
            throw new IllegalArgumentException("logIndex out of range");
        }

        try{
            ByteArrayInputStream memoryStream = new ByteArrayInputStream(logPack);
            GZIPInputStream gzipStream = new GZIPInputStream(memoryStream);
            byte[] sizeBuffer = new byte[Integer.BYTES];
            PreparedStatement ps = this.connection.prepareStatement(TRIM_TABLE_SQL);
            ps.setLong(1, index - 1);
            ps.execute();
            ps = this.connection.prepareStatement(UPDATE_SEQUENCE_SQL);
            ps.setLong(1, index);
            ps.execute();
            while(this.read(gzipStream, sizeBuffer)){
                int size = BinaryUtils.bytesToInt(sizeBuffer, 0);
                byte[] entryData = new byte[size - Integer.BYTES];
                if(!this.read(gzipStream, entryData)){
                    throw new RuntimeException("bad log pack, no able to read the log entry data");
                }

                ByteBuffer buffer = ByteBuffer.wrap(entryData);
                long term = buffer.getLong();
                byte valueType = buffer.get();
                byte[] value = new byte[size - Long.BYTES - 1 - Integer.BYTES];
                buffer.get(value);
                ps = this.connection.prepareStatement(INSERT_ENTRY_SQL);
                ps.setLong(1, term);
                ps.setByte(2, valueType);
                ps.setBytes(3, value);
                ps.execute();
                this.lastEntry = new LogEntry(term, value, LogValueType.fromByte(valueType));
            }

            this.connection.commit();
            gzipStream.close();
        }catch(Throwable error){
            this.logger.error("failed to apply log pack", error);
            throw new RuntimeException("log store error", error);
        }
    }

    @Override
    public boolean compact(long lastLogIndex) {
        if(lastLogIndex < this.startIndex.get()){
            throw new IllegalArgumentException("index out of range");
        }

        try{
            PreparedStatement ps = this.connection.prepareStatement(COMPACT_TABLE_SQL);
            ps.setLong(1, lastLogIndex);
            ps.execute();
            if(this.nextIndex.get() - 1 <= lastLogIndex){
                ps = this.connection.prepareStatement(UPDATE_SEQUENCE_SQL);
                ps.setLong(1, lastLogIndex + 1);
                ps.execute();
            }

            this.connection.commit();
            this.startIndex.set(lastLogIndex + 1);
            if(this.nextIndex.get() - 1 <= lastLogIndex){
                this.nextIndex.set(lastLogIndex + 1);
            }

            // reload last entry
            ResultSet rs = this.connection.createStatement().executeQuery("SELECT TOP 1 * FROM LogStore ORDER BY id DESC");
            if(rs.next()){
                this.lastEntry = new LogEntry(rs.getLong(2), rs.getBytes(4), LogValueType.fromByte(rs.getByte(3)));
            }else{
                this.lastEntry = new LogEntry(0, null, LogValueType.Application);
            }

            rs.close();
            return true;
        }catch(Throwable error){
            this.logger.error("failed to compact the log store", error);
            throw new RuntimeException("log store error", error);
        }
    }

    public void close(){
        if(this.connection != null){
            try {
                this.connection.close();
            } catch (SQLException e) {
                this.logger.error("failed to close the connection", e);
            }
        }
    }

    private boolean read(InputStream stream, byte[] buffer){
        try{
            int offset = 0;
            int bytesRead = 0;
            while(offset < buffer.length && (bytesRead = stream.read(buffer, offset, buffer.length - offset)) != -1){
                offset += bytesRead;
            }

            if(offset == 0 && bytesRead == -1){
                return false;
            }

            if(offset < buffer.length){
                this.logger.error(String.format("only %d bytes are read while %d bytes are desired, bad file", offset, buffer.length));
                throw new RuntimeException("bad stream, insufficient file data for reading");
            }

            return true;
        }catch(IOException exception){
            this.logger.error("failed to read and fill the buffer", exception);
            throw new RuntimeException(exception.getMessage(), exception);
        }
    }
}
