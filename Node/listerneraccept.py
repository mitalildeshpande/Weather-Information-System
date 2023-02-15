import socket
import time
import kthread
import json


# Read Message Template
message = json.load(open("Message.json"))

def accept(node,recvdmsg, addr):
    if node.isshutdown:
        Msg = recvdmsg
        req = Msg['request']
        sender = Msg['sender_name']
        term = Msg['term']
        
        if req == "VOTE_REQUEST": # RequestVote
            print("Vote Request received from {}".format(sender))
            logindex=Msg['lastLogIndex']
            if sender not in node.peers:
                return
            if term<node.term or(term==node.term and len(node.logs)-1>logindex):
                print("reject vote due to term")
                granted=0
            elif term == node.term:
                if node.votedFor == -1 or node.votedFor == sender:
                    granted = 1
                else:
                    granted = 0
            else:
                node.term = term
                node.convert()
                granted = 1
                node.votedFor = sender
            if granted == 1:
                print("Granted")
            else:
                print("Not Granted")
            reply = str(granted)
            message['sender_name'] = node.node_name
            message['request'] = "VOTE_ACK"
            message['term'] = node.term
            message['value'] = reply
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.sendto(json.dumps(message).encode('utf-8'),(sender, 5000))

        elif req == "VOTE_ACK": #VoteResponseMsg
            print("vote Response received from {}".format(sender))
            msg = Msg['value']
            if not int(msg):
                if term > node.term:
                    node.term = term
                    if node.mode == 2:
                        node.convert()
            else:
                if node.mode == 2:
                    node.request_votes.remove(sender)
                    node.numVotes += 1
                    if node.numVotes >= node.majority:
                        node.mode = 1
                        print("Got majority Votes,I am the leader at term{}".format(node.term))
                        node.nextIndex=[node.commitIndex+1]*5

                        node.leadername = node.node_name
                        if node.follower_mode.is_alive():
                            node.follower_mode.kill()
                        if node.electionthread.is_alive():
                            node.electionthread.kill()
                        node.leader_mode = kthread.KThread(target = node.leader, args = ())
                        node.leader_mode.start()
                

        elif req == "APPEND_RPC":
          
            if term >= node.term:
                if node.mode == 3:
                    node.last_update = time.time()
                if Msg['prevLogIndex'] != -1 and (Msg['prevLogIndex']-1 > len(node.logs) or Msg['prevLogTerm'] != node.logs[Msg['prevLogIndex']][0]):
                        print("Not appending the logs because previous logs didnot match")
                        success = "False"
                else:
                   
                    
                    node.term = term
                    node.convert()
                    success = "True"
                    node.commitIndex=Msg['commitIndex']
                    if Msg['Entries']!=None:
                        target="Node1"
                        if node.leadername=="Node1":
                            target="Node2"
                        if Msg['Entries'][1]=='04' and node.node_name ==target and node.flag==0:
                            Msg["Entries"]=["4","0","duplicatevalue"]
                            node.flag=1
                        if Msg['prevLogIndex']!=-1:
                            node.logs=node.logs[:Msg['prevLogIndex']+1]
                        print("The log Entry is",Msg['Entries'])
                        node.logs.append(Msg['Entries'])
                        message['appendlog']='True' 
                        print("The logs in the node are",node.logs)
                    
                    else:
                        message['appendlog']='False' 
                    
                    node.leadername = sender

            else:
                success = "False"
                message['appendlog']='False'
            message['sender_name'] = node.node_name
            message['request'] = "APPEND_ACK"
            message['term'] = node.term
            message['value'] = success 
                 
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.sendto(json.dumps(message).encode('utf-8'), (sender, 5000))

        elif req == "APPEND_ACK":
            success = Msg['value']
            if success == "False":
                if term > node.term:
                    node.convert()
                else:
                    sendernumber=int(sender[-1])-1
                    node.nextIndex[sendernumber]=node.nextIndex[sendernumber]-1
            else:
                if Msg['appendlog']=='True':
                    node.successcount+=1
                    sendernumber=int(sender[-1])-1
                    node.nextIndex[sendernumber]=node.nextIndex[sendernumber]+1
                    if node.successcount==3:
                        node.commitIndex+=1
                else:
                    node.successcount=0
            print("the next index array of leadernode is",node.nextIndex)
                    
        elif req=="TIMEOUT":
            print("Timing out",node.node_name)
            node.node_election()
        elif req == "SHUTDOWN":
            print("shutting Down",node.node_name)
            if node.listenerthread.is_alive():
                node.isshutdown=False
            if node.follower_mode.is_alive():
                node.follower_mode.kill()
                print("killed follower thread")
            if node.leadername==node.node_name and node.leader_mode.is_alive():
                node.leader_mode.kill()
                print("killed leader thread")
            if node.leadername==node.node_name and node.electionthread.is_alive():
                node.electionthread.kill()
                print("killed election thread")
        elif req == "CONVERT_FOLLOWER":
            node.convert()    
        elif req == "LEADER_INFO": 
            message['sender_name'] = node.node_name
            message['request']="LEADER_INFO"
            message['key'] = "Leader"
            message['term'] = node.term
            message['value'] = node.leadername
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.sendto(json.dumps(message).encode('utf-8'), ("Controller", 5000))
        elif req=="STORE":
            if node.node_name==node.leadername:
                entry=[Msg['term'],Msg['key'],Msg['value']]
                node.logs.append(entry)
                print(node.logs)
                
            else:
                 message['sender_name'] = node.node_name
                 message['request']="LEADER_INFO"
                 message['key'] = "Leader"
                 message['term'] = node.term
                 message['value'] = node.leadername
                 s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                 s.sendto(json.dumps(message).encode('utf-8'), ("Controller", 5000))
        elif req=="RETRIEVE":
            if node.node_name==node.leadername:
                message['sender_name'] = node.node_name
                message['request']="RETRIEVE"
                message['key']="COMMITED_LOGS"
                message['value']=node.logs
                s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                s.sendto(json.dumps(message).encode('utf-8'), ("Controller", 5000))
                
            else:
                 message['sender_name'] = node.node_name
                 message['request']="LEADER_INFO"
                 message['key'] = "Leader"
                 message['term'] = node.term
                 message['value'] = node.leadername
                 s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                 s.sendto(json.dumps(message).encode('utf-8'), ("Controller", 5000))
        
            
        