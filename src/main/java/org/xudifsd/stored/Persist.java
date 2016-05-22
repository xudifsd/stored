package org.xudifsd.stored;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.IllegalFormatException;
import java.util.List;

public class Persist {
    private static final Logger LOG = LoggerFactory.getLogger(Persist.class);

    private final String dirPath;
    private long currentTerm = 0;
    private String votedFor = null;

    public static final String currentTermFileName = "currentTerm";
    public static final String votedForFileName = "votedFor";
    public static final String logFileName = "stored.binlog";
    public static final String logIndexFileName = "stored.index";

    private File currentTermFile;
    private File votedForFile;
    private File logFile;
    private File logIndexFile;

    public Persist(String dirPath) {
        this.dirPath = dirPath;
    }

    public long getCurrentTerm() {
        return currentTerm;
    }

    public String getVotedFor() {
        return votedFor;
    }

    public void writeCurrentTerm(long currentTerm) throws IOException {
        this.currentTerm = currentTerm;
        DataOutputStream out = new DataOutputStream(new FileOutputStream(currentTermFile));
        out.writeLong(currentTerm);
        out.close();
    }

    public void writeVotedFor(String votedFor) throws IOException {
        this.votedFor = votedFor;
        votedForFile = new File(dirPath + File.separator + votedForFileName);
        if (this.votedFor == null) {
            if (votedForFile.exists()) {
                votedForFile.delete();
            }
            return;
        } // else {
        if (votedFor.contains("\n")) {
            throw new RuntimeException("votedFor could not contains newline");
        }

        FileWriter writer = new FileWriter(votedForFile);
        writer.write(votedFor);
        writer.close();
    }

    public void restore(StateMachine stateMachine) throws IOException {
        File dir = new File(dirPath);
        if (!dir.isDirectory()) {
            throw new RuntimeException(dirPath + " is not a directory");
        }

        currentTermFile = new File(dirPath + File.separator + currentTermFileName);
        if (!currentTermFile.exists()) {
            currentTerm = 0;
        } else {
            DataInputStream in = new DataInputStream(new FileInputStream(currentTermFile));
            currentTerm = in.readLong();
            in.close();
        }

        votedForFile = new File(dirPath + File.separator + votedForFileName);
        if (!votedForFile.exists()) {
            votedFor = null;
        } else {
            BufferedReader in = new BufferedReader(new FileReader(currentTermFile));
            votedFor = in.readLine();
            in.close();
        }

        logFile = new File(dirPath + File.separator + logFileName);
        logIndexFile = new File(dirPath + File.separator + logIndexFileName);
        // TODO apply persisted log to stateMachine
    }

    public void commitLogEntries(long lastCommitIndex, List<ByteBuffer> ops)
            throws IOException {
        long offset = 0;
        if (lastCommitIndex != 0) {
            // TODO implement index file
            throw new IllegalArgumentException("index file not implemented");
        }
        RandomAccessFile log = new RandomAccessFile(logFile, "rw");
        log.seek(offset);

        for (int i = 0; i < ops.size(); ++i) {
            byte[] data = ops.get(i).array();
            log.writeInt(data.length);
            log.write(data, 0, data.length);
        }
        log.close();
    }

    public List<byte[]> readLogEntries(long startIndex, int size) throws IOException {
        long offset = 0;
        if (startIndex != 0) {
            // TODO implement index file
            throw new IllegalArgumentException("index file not implemented");
        }
        RandomAccessFile log = new RandomAccessFile(logFile, "r");
        log.seek(offset);

        List<byte[]> result = new ArrayList<byte[]>(size);
        for (int i = 0; i < size; ++i) {
            int len = log.readInt();
            byte[] data = new byte[len];

            for (int readCount = 0; readCount < len;) {
                int value = log.read(data, readCount, len - readCount);
                if (value == -1) {
                    throw new IOException("log file corrupted");
                }
                readCount += value;
            }
            result.add(data);
        }
        log.close();
        return result;
    }
}
