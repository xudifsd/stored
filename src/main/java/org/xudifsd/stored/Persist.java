package org.xudifsd.stored;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class Persist {
    private final String dirPath;
    private long currentTerm = 0;
    private String votedFor = null;

    public static final String currentTermFileName = "currentTerm";
    public static final String votedForFileName = "votedFor";

    private File currentTermFile;
    private File votedForFile;

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
        votedForFile = new File(dirPath + File.pathSeparator + votedForFileName);
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

        currentTermFile = new File(dirPath + File.pathSeparator + currentTermFileName);
        if (!currentTermFile.exists()) {
            currentTerm = 0;
        } else {
            DataInputStream in = new DataInputStream(new FileInputStream(currentTermFile));
            currentTerm = in.readLong();
            in.close();
        }

        votedForFile = new File(dirPath + File.pathSeparator + votedForFileName);
        if (!votedForFile.exists()) {
            votedFor = null;
        } else {
            BufferedReader in = new BufferedReader(new FileReader(currentTermFile));
            votedFor = in.readLine();
            in.close();
        }

        // TODO apply persisted log to stateMachine
    }
}
