namespace java org.xudifsd.stored.thrift

struct AppendEntriesResp {
    1: required i64 term
    2: required bool success
}

struct RequestVoteResp {
    1: required i64 term
    2: required bool voteGranted
}

service RaftProtocol {

AppendEntriesResp appendEntries(1: i64 term, 2: string leaderId, 3: i64 prevLogIndex, 4: i64 prevLogTerm, 5: list<binary> entries, 6: i64 leaderCommit);

RequestVoteResp requestVote(1: i64 term, 2: string candidateId, 3: i64 lastLogIndex, 4: i64 lastLogTerm);

}
