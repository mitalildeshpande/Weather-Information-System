import socket
import time
import kthread
import json
import os
from listerneraccept import *
import random

msg = json.load(open("Message.json"))

class Node:
    def __init__(self, sender):
        self.node_name = sender
        self.leadername = 0
        self.term = 1
        self.mode = 3
        self.votedFor = -1
        self.nodes = ["Node1", "Node2", "Node3", "Node4", "Node5"]
        self.peers = [i for i in self.nodes if i!=self.node_name]
        self.request_votes = self.peers[:]
        self.logs = []
        self.entries=[]
        self.heartbeat = 4
        self.timeout = random.uniform(2*self.heartbeat,3*self.heartbeat)
        self.isshutdown=True
        self.majority = len(self.nodes)//2+1
        self.nextIndex=[1,1,1,1,1]
        self.matchIndex=[0,0,0,0,0]
        self.commitIndex=-1
        self.lastApplied=0
        self.prevlogIndex=0
        self.prevlogTerm=0
        self.successcount=0
        self.listenerthread = kthread.KThread(target = self.listener, args = (accept,))
        self.listenerthread.start()
        self.issleep=0
        self.flag=0
        

    def listener(self, accept):
        srv = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        srv.bind((self.node_name, 5000))
        print("Listening",self.node_name)
        while 1:
            data, addr = srv.recvfrom(1024)
            decoded_msg = json.loads(data.decode('utf-8'))
            thread = kthread.KThread(target = accept, args = (self, decoded_msg, addr))
            thread.start()
        srv.close()

    def follower(self):
        print("I am a follower now")
        self.mode = 3
        self.last_update = time.time()
        
        # Election starts from the beginning
        while time.time() - self.last_update < self.timeout:
            pass
        self.node_election()
        # heartbeats from the leader are monitered
        while 1:
            self.last_update = time.time()
            while time.time() - self.last_update < self.timeout:
                pass
            if self.electionthread.is_alive():
                self.electionthread.kill()
            self.node_election()
    def convert(self):
        if self.mode == 1:
            if self.leader_mode.is_alive():
                self.leader_mode.kill()
            self.follower_mode = kthread.KThread(target=self.follower, args= ())
            self.follower_mode.start()
        elif self.mode== 2:
            if self.electionthread.is_alive():
                self.electionthread.kill()
            self.last_update_time = time.time()
            self.state = 3
    def heartbeats(self):
        while 1:
            receipts = self.peers[:]
            for i in range(len(receipts)):
                msg['sender_name'] = self.node_name
                msg['request'] = "APPEND_RPC"
                msg['term'] = self.term
                msg['LeaderId']=self.leadername
                msg['Entries']=None
                nodenum=int(receipts[i][-1])
                if len(self.logs)-1>=self.nextIndex[nodenum-1]:
                    msg['Entries']=self.logs[self.nextIndex[nodenum-1]]
                    if len(self.logs)>1:
                        msg['prevLogTerm']=self.logs[self.nextIndex[nodenum-1]-1][0]
                        msg['prevLogIndex']=self.nextIndex[nodenum-1]-1
                msg['commitIndex']=self.commitIndex
                sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                sock.sendto(json.dumps(msg).encode('utf-8'), (receipts[i], 5000))
            time.sleep(self.heartbeat)
            self.successcount=0

    def node_election(self):
        self.mode = 2 
        print("I am a candidate now")
        self.electionthread = kthread.KThread(target = self.thread_election, args =())
        # change the term and self vote before requesting votes from other nodes 
        if len(self.peers) != 0:
            self.term += 1
            self.votedFor = self.node_name
            self.numVotes = 1
            self.electionthread.start()

    def thread_election(self):
        print("Timeout, starting a new election at term {}, the id is {}".format( self.term, self.node_name))
        self.mode = 2 
        self.request_votes =self.peers[:]
        requestor = self.node_name
        while 1:
            for peer in self.peers:
                if peer in self.request_votes:
                    msg['sender_name'] = self.node_name
                    msg['request'] = "VOTE_REQUEST"
                    msg['term'] = self.term
                    msg['candidateId']=self.node_name
                    msg['lastLogIndex']=len(self.logs)-1
                    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                    sock.sendto(json.dumps(msg).encode('utf-8'),(peer, 5000))
            time.sleep(1)

    def leader(self):
        print("I am a leader now")
        self.mode = 1
        self.heartbeats()
    def run(self):
        time.sleep(1)
        self.follower_mode = kthread.KThread(target = self.follower, args= ())
        self.follower_mode.start()

if __name__ == "__main__":
    
    sender = os.environ.get('NODE_NAME')
    node = Node(sender)
    node.run()