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

    public static final String currentTermFileName = "stored.currentTerm";
    public static final String votedForFileName = "stored.votedFor";
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

    // should call this method before any other
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

    /**
     * Log file format is [size1][content of size1][size2][content of size2]...
     * size{1..} is of size 4 since int has size 4. So current log file support
     * content size up to 2^31 - 1, since java do not support unsigned int.
     *
     * Index file format is [offset1][offset2][offset3]...
     * offset{1..} is of size 8 since long has size 8. So current we can save
     * 2^63 - 1 number of log entries, since java do not support unsigned long.
     * Offset in index file indicate offset of next entry, For example, if log
     * file has entries of size 8, 5, 3... then index file should contains
     * (8 + 4), (8 + 5 + 4 * 2), (8 + 5 + 3 + 4 * 3)...
     * */

    /**
     * This method would overwrite all entries committed after lastCommitIndex.
     * lastCommitIndex should be index of highest log entry known to be
     * committed. So, if we want to commit second entry, we should pass 1 as
     * lastCommitIndex
     * */
    public void commitLogEntries(long lastCommitIndex, List<ByteBuffer> ops)
            throws IOException {
        long offset = 0;
        if (lastCommitIndex != 0 &&
                (!logIndexFile.exists() || logIndexFile.length() < lastCommitIndex * 8)) {
            throw new IOException("index file corrupted");
        }

        RandomAccessFile logIndex = new RandomAccessFile(logIndexFile, "rw");
        if (lastCommitIndex != 0) {
            if (lastCommitIndex < 0) {
                throw new IllegalArgumentException("lastCommitIndex should not less than 0");
            }
            logIndex.seek((lastCommitIndex - 1) * 8);
            offset = logIndex.readLong();
            logIndex.setLength(lastCommitIndex * 8);
        }

        RandomAccessFile log = new RandomAccessFile(logFile, "rw");
        log.setLength(offset);
        log.seek(offset);

        for (int i = 0; i < ops.size(); ++i) {
            // TODO add checksum support https://docs.oracle.com/javase/7/docs/api/java/util/zip/CRC32.html
            byte[] data = ops.get(i).array();
            log.writeInt(data.length);
            log.write(data, 0, data.length);
            logIndex.writeLong(log.length());
        }
        logIndex.close();
        log.close();
    }

    // read size of entry from index startIndex
    public List<byte[]> readLogEntries(long startIndex, int size) throws IOException {
        long offset = 0;
        if (startIndex != 0 &&
                (!logIndexFile.exists() || logIndexFile.length() < startIndex * 8)) {
            throw new IOException("index file corrupted");
        }

        RandomAccessFile logIndex = new RandomAccessFile(logIndexFile, "r");
        if (startIndex != 0) {
            if (startIndex < 0) {
                throw new IllegalArgumentException("startIndex should not less than 0");
            }
            logIndex.seek((startIndex - 1) * 8);
            offset = logIndex.readLong();
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
