import socket
import sys
import json
import threading
from threading import Thread, Lock
import time
from tt import TimeTable
from block import Transaction, BlockChain, BallotNum

PID = int(sys.argv[1])
print("Process id: ", PID)

IP = "127.0.0.1"
BUFFER_SIZE = 1024
CLIENTS = []
LAMPORT = 0
BALANCE = 10
MAJORITY = 1
BALLOT_NUM = BallotNum()
ACCEPT_NUM = BallotNum()
ACCEPT_VAL = []
ACK_COUNT = 0
ACCEPT_COUNT = 0
MAX_ACK_NUM = BallotNum()
MAX_ACK_VAL = []
isLeader = False
transaction_log = []

PORT = 5000+PID
clientConn = {}
pidConn = {}     

def sendMessage(msg, conn):
    time.sleep(5)
    conn.sendall(msg.encode('utf-8'))

def sendLog(pid):
    # incrementLamport()
    events = []
    node = chain.get_head()
    while node is not None:
        if not hasrec(pid, node.data):
            events.append(node.data.toJSON())
        node = node.next_node
    data = {
        'clock': LAMPORT,
        'table': tt.toJSON(),
        'events': events
    }
    message = json.dumps(data)
    threading.Thread(target = sendMessage, args = (message, pidConn[pid],)).start()
    print ('Message sent to client '+str(pid))

def sendPrepare(ballotNum):
    data = {
        'type': 'prepare',
        'ballot': ballotNum.toJSON()
    }
    message = json.dumps(data)
    for client in CLIENTS:
        threading.Thread(target = sendMessage, args = (message, client,)).start()
    print ('Prepare message sent to clients')   
    
    
def processInput(data):
    dataList = data.split(',')
    if dataList[0] == 'transfer':
        # Update getBalance to get balance
        receiver = int(dataList[1])
        amount = int(dataList[2])
        amountBefore = chain.getBalance(PID)
        if amountBefore >= amount and PID != receiver:
            transaction_log.append(Transaction(PID, receiver, amount))
            print("SUCCESS")
            print("Balance before: $"+str(amountBefore))
            print("Balance after: $"+str(amountBefore-amount))
        else:
            # Run Paxos
            runPaxos()
            amountBefore = chain.getBalance(PID)
            if amountBefore >= amount and PID != receiver:
                transaction_log.append(Transaction(PID, receiver, amount))
                print("SUCCESS")
                print("Balance before: $"+str(amountBefore))
                print("Balance after: $"+str(amountBefore-amount))
            else:
                print("INCORRECT")
            
    elif dataList[0] == 'balance':
        if len(dataList) == 1:
            dataList.append(str(PID))
        print("Balance: $"+str(chain.getBalance(int(dataList[1]))))
    

def processMessage(pid, data):    
    print ('Message from client ' + str(pid))
    data = json.loads(data)
    if data['type'] == 'prepare':
        ballotNum = BallotNum.load(data['ballot'])
        if ballotNum.isHigher(BALLOT_NUM):
            BALLOT_NUM = ballotNum
            val = []
            for aval in ACCEPT_VAL:
                val.append(aval.toJSON())
            data = {
                'type': 'ack',
                'ballot': BALLOT_NUM.toJSON(),
                'accept_ballot': ACCEPT_NUM.toJSON(),
                'accept_val': val 
            }
            message = json.dumps(data)
            threading.Thread(target = sendMessage, args = (message, pidConn[pid],)).start()
            print ('Ack message sent to client '+str(pid))
            
    elif data['type'] == 'ack':
        #  check for majority and send accept to followers
        ACK_COUNT += 1
        acceptBallot = BallotNum.load(data['accept_ballot'])
        acceptVal = data['accept_val']
        if len(acceptVal) != 0 and acceptBallot.isHigher(MAX_ACK_NUM):
            MAX_ACK_NUM = acceptBallot
            MAX_ACK_VAL = acceptVal[:]
        
        if ACK_COUNT >= MAJORITY:  
            log = []          
            if len(MAX_ACK_VAL) != 0:
                log = MAX_ACK_VAL[:]
            else:
                for val in transaction_log:
                    log.append(val.toJSON())

            data = {
                'type': 'leader_accept',
                'ballot': BALLOT_NUM.toJSON(),
                'value': log   
            }
            message = json.dumps(data)
            for client in CLIENTS:
                threading.Thread(target = sendMessage, args = (message, client,)).start()
            print ('Accept message sent to followers')
            ACK_COUNT = 0
            
         
    elif data['type'] == 'leader_accept':
        ballotNum = BallotNum.load(data['ballot'])
        if ballotNum.isHigher(BALLOT_NUM):
            BALLOT_NUM = ballotNum
            ACCEPT_NUM = ballotNum
            val = data['value'][:]
            ACCEPT_VAL = [ Transaction.load(val) for val in data['value'] ]
            data = {
                'type': 'accept',
                'ballot': BALLOT_NUM.toJSON(),
                'value': val
            }
            message = json.dumps(data)
            threading.Thread(target = sendMessage, args = (message, pidConn[pid],)).start()
            print ('Accept message sent to client '+str(pid))        
    
    elif data['type'] == 'accept':
        # do stuff and relay message to leader
        ACCEPT_COUNT += 1       
        if ACCEPT_COUNT >= MAJORITY:
            data = {
                'type': 'decide',
                'ballot': BALLOT_NUM.toJSON()  
            }
            message = json.dumps(data)
            for client in CLIENTS:
                threading.Thread(target = sendMessage, args = (message, client,)).start()
            print ('Decide message sent to followers')
            ACCEPT_COUNT = 0
    
    elif data['type'] = 'decide':
        chain.append(ACCEPT_VAL)
                  

                
def listenToClient(pid, conn):
    with conn:
        while True:
            try:
                data = conn.recv(BUFFER_SIZE).decode('utf-8')
                if not data:
                    break
                processMessage(pid, data)
            except socket.error:
                print ("Socket error in receiving message")
                break
        if conn in CLIENTS:
            CLIENTS.remove(conn)


def createServer(pid):
    try: 
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(('', PORT))         
        print("socket binded to %s" %(PORT))
        s.listen(1)      
        print("socket successfully created")
    except socket.error as err: 
        print("socket creation failed with error %s" %(err)) 

    while True:
        conn, addr = s.accept()
        data = conn.recv(BUFFER_SIZE).decode('utf-8')
        if not data:
            break
        dataList = data.split(',')
        if dataList[0] == 'pid':
            clientConn[conn] = int(dataList[1])
            pidConn[int(dataList[1])] = conn
            print('Accepted connection from client ', dataList[1])
        CLIENTS.append(conn)
        print("#clients connected: ", len(CLIENTS))
        threading.Thread(target = listenToClient,args = (int(dataList[1]),conn,)).start()


def connectToClient(pid, ip, port):
    c_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    c_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    c_socket.connect((ip, port))
    c_socket.sendall(("pid,"+str(PID)).encode('utf-8'))
    CLIENTS.append(c_socket)
    clientConn[c_socket] = pid
    pidConn[pid] = c_socket
    threading.Thread(target = listenToClient,args = (pid,c_socket,)).start()
   
     
if __name__ == "__main__":
    # Reading the client configurations
    f = open(sys.argv[2], 'r')
    configList = f.readlines()
    config = configList[PID-1].strip('\n').split(',')
    if len(config) != 3:
        print("Incorrect configuration")
        sys.exit()
    IP = config[0]
    PORT = int(config[1])
    BALANCE = int(config[2])
    
    # Creating server to listen for connections
    server_thread = threading.Thread(target = createServer,args = (PID,)) 
    server_thread.start() 

    # Connect to existing clients
    for i in range(1, PID):
        clientConfig = configList[i-1].strip('\n').split(',')
        connectToClient(i, clientConfig[0], int(clientConfig[1]))
    print("#clients connected: ", len(CLIENTS))
    print("Balance: $"+str(BALANCE))
    
    # Listen for client inputs
    chain = BlockChain(BALANCE)

    while True:
        message = input("Enter transaction: ")
        processInput(message)

