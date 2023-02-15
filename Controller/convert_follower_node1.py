import json
import socket
import traceback
import threading
import kthread
import time

# Wait following seconds below sending the controller request
time.sleep(15)


leader_id = 0 #leader_value is stored here 
leaderterm=0

def listener(skt):
    print(f"Starting Listener in Controller")
    global leader_id

    global leaderterm
    while True:
        try:
            msg, addr = skt.recvfrom(1024)
        except:
            print(f"ERROR while fetching from socket : {traceback.print_exc()}")

        # Decoding the Message received from Node 1
        decoded_msg = json.loads(msg.decode('utf-8'))
        print(f"Message Received : {decoded_msg} From : {addr}")
        if decoded_msg['request'] == 'LEADER_INFO':
            leader_id = decoded_msg['value']
            leaderterm=decoded_msg['term']
        if decoded_msg['request'] == 'RETRIEVE':
            retrieved_logs = decoded_msg['value']
            print(retrieved_logs)
        
       

    print("Exiting Listener Function")

# Read Message Template
msg = json.load(open("Message.json"))

# Initialize
sender = "Controller"
target = "Node1"
port = 5000

# Request
msg['sender_name'] = sender
msg['request'] = "LEADER_INFO"
print(f"Request Created : {msg}")

# Socket Creation and Binding
skt = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
skt.bind((sender, port))

kthread.KThread(target=listener, args=[skt]).start()

# Send Message
try:
    # Encoding and sending the message
    skt.sendto(json.dumps(msg).encode('utf-8'), (target,port))
    time.sleep(5)

except:
    #  socket.gaierror: [Errno -3] would be thrown if target IP container does not exist or exits, write your listener
    print(f"ERROR WHILE SENDING REQUEST ACROSS : {traceback.format_exc()}")

#Initialize
sender = "Controller"
target = leader_id
port = 5000

# Request
msg['sender_name'] = sender
msg['request'] = "STORE"
msg['term']=leaderterm
msg['key']='01'
msg['value']='value'
print(f"Request Created : {msg}")
try:
    
    skv = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
    skv.sendto(json.dumps(msg).encode('utf-8'), (target,port))
    #time.sleep(10)
except:
    print(f"ERROR WHILE SENDING REQUEST ACROSS : {traceback.format_exc()}")


msg['sender_name'] = sender
msg['request'] = "STORE"
msg['term']=leaderterm
msg['key']='02'
msg['value']='secondvalue'
print(f"Request Created : {msg}")
try:
    
    skv = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
    skv.sendto(json.dumps(msg).encode('utf-8'), (target,port))
    time.sleep(15)
except:
    print(f"ERROR WHILE SENDING REQUEST ACROSS : {traceback.format_exc()}")


msg['sender_name'] = sender
msg['request'] = "RETRIEVE"
print(f"Request Created : {msg}")
try:
    
    skv = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
    skv.sendto(json.dumps(msg).encode('utf-8'), (target,port))
    time.sleep(5)
except:
    print(f"ERROR WHILE SENDING REQUEST ACROSS : {traceback.format_exc()}")
# #Initialize
sender = "Controller"
target = leader_id
port = 5000

# Request
msg['sender_name'] = sender
msg['request'] = "CONVERT_FOLLOWER"
print(f"Request Created : {msg}")
try:
    
    skv = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
    skv.sendto(json.dumps(msg).encode('utf-8'), (target,port))
    time.sleep(10)

except:
   
    print(f"ERROR WHILE SENDING REQUEST ACROSS : {traceback.format_exc()}")

sender = "Controller"
target = "Node1"
port = 5000

msg['sender_name'] = sender
msg['request'] = "LEADER_INFO"
print(f"Request Created : {msg}")

# Send Message
try:
    # Encoding and sending the message
    skt = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
    
    skt.sendto(json.dumps(msg).encode('utf-8'), (target,port))
    time.sleep(10)

except:
    #  socket.gaierror: [Errno -3] would be thrown if target IP container does not exist or exits, write your listener
    print(f"ERROR WHILE SENDING REQUEST ACROSS : {traceback.format_exc()}")


msg['sender_name'] = sender
msg['request'] = "STORE"
msg['term']=leaderterm
msg['key']='03'
msg['value']='Thirdvalue'
print(f"Request Created : {msg}")
try:
    
    skv = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
    skv.sendto(json.dumps(msg).encode('utf-8'), (leader_id,port))
    time.sleep(5)
except:
    print(f"ERROR WHILE SENDING REQUEST ACROSS : {traceback.format_exc()}")


sender = "Controller"
if leader_id=="Node1":
    target="Node2"
else:
    target = "Node1"

#Request

# msg['sender_name'] = sender
# msg['request'] = "STOREDUMMY"
# msg['term']=leaderterm
# msg['key']='00'
# msg['value']='DUMMYvalue'
# print(f"Request Created : {msg}")

# # Socket Creation and Binding


# kthread.KThread(target=listener, args=[skt]).start()

# # Send Message
# try:
#     # Encoding and sending the message
#     skt = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
#     skt.sendto(json.dumps(msg).encode('utf-8'), (target,port))
#     time.sleep(15)

# except:
#     #  socket.gaierror: [Errno -3] would be thrown if target IP container does not exist or exits, write your listener
#     print(f"ERROR WHILE SENDING REQUEST ACROSS : {traceback.format_exc()}")

msg['sender_name'] = sender
msg['request'] = "STORE"
msg['term']=leaderterm
msg['key']='04'
msg['value']='Fourth value'
print(f"Request Created : {msg}")
target=leader_id
try:
    
    skv = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
    skv.sendto(json.dumps(msg).encode('utf-8'), (target,port))
    #time.sleep(20)
except:
    print(f"ERROR WHILE SENDING REQUEST ACROSS : {traceback.format_exc()}")


msg['sender_name'] = sender
msg['request'] = "STORE"
msg['term']=leaderterm
msg['key']='05'
msg['value']='fifthvalue'
print(f"Request Created : {msg}")
try:
    
    skv = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
    skv.sendto(json.dumps(msg).encode('utf-8'), (target,port))
    time.sleep(5)
except:
    print(f"ERROR WHILE SENDING REQUEST ACROSS : {traceback.format_exc()}")














# msg['sender_name'] = sender
# msg['request'] = "TIMEOUT"
# target="Node1"
# print(f"Request Created : {msg}")

# try:
   
#     skv = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
#     skv.sendto(json.dumps(msg).encode('utf-8'), (target,port))
#     time.sleep(20)

# except:
#     #  socket.gaierror: [Errno -3] would be thrown if target IP container does not exist or exits, write your listener
#     print(f"ERROR WHILE SENDING REQUEST ACROSS : {traceback.format_exc()}")

# msg['sender_name'] = sender
# msg['request'] = "SHUTDOWN"
# target="Node1"
# print(f"Request Created : {msg}")

# try:
   
#     skv = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
#     skv.sendto(json.dumps(msg).encode('utf-8'), (target,port))

# except:
#     #  socket.gaierror: [Errno -3] would be thrown if target IP container does not exist or exits, write your listener
#     print(f"ERROR WHILE SENDING REQUEST ACROSS : {traceback.format_exc()}")