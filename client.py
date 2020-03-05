import socket
import sys
import json
import threading
from threading import Thread, Lock
import time
from block import Transaction, BlockChain, BallotNum

PID = int(sys.argv[1])
print("Process id: ", PID)

IP = "127.0.0.1"
BUFFER_SIZE = 1024
NUM_CLIENTS = 3
CLIENTS = []
LAMPORT = 0
BALANCE = 10
MAJORITY = 1
SEQ_NUM = 0
BALLOT_NUM = BallotNum()
ACCEPT_NUM = BallotNum()
ACCEPT_VAL = []
ACK_COUNT = 0
ACCEPT_COUNT = 0
MAX_ACK_NUM = BallotNum()
MAX_ACK_VAL = []
isLeader = False
transaction_log = []
INPUT = ""

PORT = 5000+PID
clientConn = {}
pidConfig = {}
INTERVAL = 0     

#ADDED BY MAYURESH
# To be changed when FOLLOWER receives ACCEPT from LEADER
FOLLOWER_FLAG_ACCEPT = False
# To be changed when FOLLOWER receives COMMIT from LEADER
FOLLOWER_FLAG_COMMIT = False
# To be changed when FOLLOWER receives new PREPARE message
FOLLOWER_FLAG_RESET_TIMER = False
# To be changed when decided to CRASH and regained conciousness
LEADER_CRASH_FLAG = False


def sendMessage(msg, pid):
    time.sleep(5)
    try:
        s = socket.socket()
        s.connect((IP, pidConfig[pid]))
        s.sendall(msg.encode('utf-8'))
        s.close()
    except:
        print("Client" + str(pid) + " is down!")
    
def incrementAckCount():
	global ACK_COUNT
	mutex = Lock()
	mutex.acquire()
	ACK_COUNT += 1
	mutex.release()
	print ('ACK_COUNT: ' + str(ACK_COUNT))
 
def incrementAcceptCount():
	global ACCEPT_COUNT
	mutex = Lock()
	mutex.acquire()
	ACCEPT_COUNT += 1
	mutex.release()
	print ('ACCEPT_COUNT: ' + str(ACCEPT_COUNT))
 
def incrementInterval(cnt):
	global INTERVAL
	mutex = Lock()
	mutex.acquire()
	INTERVAL += cnt
	mutex.release()
	print ('Interval: ' + str(INTERVAL))

def sendPrepare():
    BALLOT_NUM.num += 1
    BALLOT_NUM.pid = PID
    SEQ_NUM = chain.getLastSeqNum() + 1
    data = {
        'pid': PID,
        'type': 'prepare',
        'ballot': BALLOT_NUM.toJSON(),
        'seq_num': SEQ_NUM
    }
    message = json.dumps(data)
    for i in range(1, NUM_CLIENTS+1):
        if i != PID:
            threading.Thread(target = sendMessage, args = (message, i,)).start()
    print ('Prepare message sent to clients')  
    threading.Thread(target = startTimerForAck, args = (15,)).start() 
    
def startTimerForAck(start=15):
    global INTERVAL
    global ACK_COUNT
    global MAJORITY
    #ADDED BY MAYURESH
    global LEADER_CRASH_FLAG    

    INTERVAL = start
    while LEADER_CRASH_FLAG == False: #CHANGED BY MAYURESH
        time.sleep(1)
        INTERVAL -= 1
        print ('INTERVAL: ' + str(INTERVAL))
        if INTERVAL <= 0:
            if ACK_COUNT >= MAJORITY:  
                log = []          
                if len(MAX_ACK_VAL) != 0:
                    log = MAX_ACK_VAL[:]
                else:
                    for val in transaction_log:
                        log.append(val.toJSON())

                data = {
                    'pid': PID,
                    'type': 'accept',
                    'ballot': BALLOT_NUM.toJSON(),
                    'seq_num': SEQ_NUM,
                    'value': log   
                }
                message = json.dumps(data)
                for i in range(1, NUM_CLIENTS+1):
                    if i != PID:
                        threading.Thread(target = sendMessage, args = (message, i,)).start()
                print ('Accept message sent to followers')
                ACK_COUNT = 0
                threading.Thread(target = startTimerForAccept, args = (15,)).start()
            break

def startTimerForAccept(start=15):
    global INTERVAL
    global ACCEPT_COUNT
    global MAJORITY
    global INPUT
    global SEQ_NUM  
    #ADDED BY MAYURESH
    global LEADER_CRASH_FLAG

    INTERVAL = start
    while LEADER_CRASH_FLAG == False: #CHANGED BY MAYURESH
        time.sleep(1)
        INTERVAL -= 1
        print ('INTERVAL: ' + str(INTERVAL))
        if INTERVAL <= 0:
            if ACCEPT_COUNT >= MAJORITY:
                val = []
                for aval in transaction_log:
                    val.append(aval.toJSON())
                data = {
                    'pid': PID,
                    'type': 'commit',
                    'ballot': BALLOT_NUM.toJSON(),
                    'seq_num': SEQ_NUM,
                    'value': val  
                }
                message = json.dumps(data)
                for i in range(1, NUM_CLIENTS+1):
                    if i != PID:
                        threading.Thread(target = sendMessage, args = (message, i,)).start()
                print ('Decide message sent to followers')
                ACCEPT_COUNT = 0
                SEQ_NUM = chain.append(SEQ_NUM, transaction_log)
                transaction_log.clear()
                chain.printChain()
                if INPUT != "":
                    print(f"Pending transaction: {INPUT}")
                    processInput(INPUT)
            break

# ADDED BY MAYURESH    
def startTimerForFollowerAccept(start=15):
    global FOLLOWER_INTERVAL
    global FOLLOWER_FLAG_RESET_TIMER = False #If new PREPARE message is accepted then it doesnt start paxos
    FOLLOWER_INTERVAL = start
    while FOLLOWER_FLAG_ACCEPT != True:
        time.sleep(1)
        FOLLOWER_INTERVAL -= 1
        print ('FOLLOWER_INTERVAL: ' + str(FOLLOWER_INTERVAL))
        if FOLLOWER_INTERVAL <= 0:
            break
    if FOLLOWER_INTERVAL <= 0 and FOLLOWER_FLAG_RESET_TIMER = False:
        FOLLOWER_FLAG_ACCEPT = False
        # START PAXOS
        sendPrepare()
        # RESET ALL GLOBAL VARIABLES   ???????????????????????????????????????

# ADDED BY MAYURESH       
def startTimerForCommit(start = 15):
    global FOLLOWER_INTERVAL
    global FOLLOWER_FLAG_RESET_TIMER = False #If new PREPARE message is accepted then it doesnt start paxos
    FOLLOWER_INTERVAL = start
    while FOLLOWER_FLAG_COMMIT != True:
        time.sleep(1)
        FOLLOWER_INTERVAL -= 1
        print ('FOLLOWER_INTERVAL: ' + str(FOLLOWER_INTERVAL))
        if FOLLOWER_INTERVAL <= 0:
            break
    if FOLLOWER_INTERVAL <= 0 and FOLLOWER_FLAG_RESET_TIMER = False:
        FOLLOWER_FLAG_COMMIT = False
        # START PAXOS
        sendPrepare()
        # RESET ALL GLOBAL VARIABLES   ???????????????????????????????????????

def processMessage(data):  
    global BALLOT_NUM
    global ACCEPT_NUM
    global ACCEPT_VAL
    global MAX_ACK_NUM
    global MAX_ACK_VAL 
    global ACK_COUNT
    global ACCEPT_COUNT 
    global INTERVAL
    
    data = json.loads(data)
    pid = data['pid']
    print ('Message from client ' + str(pid))

    if data['type'] == 'prepare':
        ballotNum = BallotNum.load(data['ballot'])
        if ballotNum.isHigher(BALLOT_NUM):
            #ADDED BY MAYURESH
            # WE NEED TO RESET EARLIER TIMERS HERE SO THAT FOLLOWER DOESNT START PAXOS
            FOLLOWER_FLAG_RESET_TIMER = True
                
            BALLOT_NUM = ballotNum
            val = []
            for aval in ACCEPT_VAL:
                val.append(aval.toJSON())
            data = {
                'pid': PID,
                'type': 'ack',
                'ballot': BALLOT_NUM.toJSON(),
                'seq_num': chain.getLastSeqNum(),
                'accept_ballot': ACCEPT_NUM.toJSON(),
                'accept_val': val 
            }
            message = json.dumps(data)
            threading.Thread(target = sendMessage, args = (message, pid,)).start()
            print ('Ack message sent to client '+str(pid))
            #ADDED BY MAYURESH
            print("Timer started for ACCEPT messages")
            threading.Thread(target = startTimerForFollowerAccept, args = (15,)).start()
            
         
    elif data['type'] == 'ack':
        #  check for majority and send accept to followers
        incrementInterval(5)
        incrementAckCount()
        acceptBallot = BallotNum.load(data['accept_ballot'])
        acceptVal = data['accept_val']
        if len(acceptVal) != 0 and acceptBallot.isHigher(MAX_ACK_NUM):
            MAX_ACK_NUM = acceptBallot
            MAX_ACK_VAL = acceptVal[:]
                   
    elif data['type'] == 'accept':
        ballotNum = BallotNum.load(data['ballot'])
        if ballotNum.isHigher(BALLOT_NUM):
            BALLOT_NUM = ballotNum
            ACCEPT_NUM = ballotNum
            val = []
            for aval in transaction_log:
                val.append(aval.toJSON())
            ACCEPT_VAL = [ Transaction.load(val) for val in data['value'] ]
            data = {
                'pid': PID,
                'type': 'accepted',
                'ballot': BALLOT_NUM.toJSON(),
                'seq_num': chain.getLastSeqNum(),
                'value': val
            }
            message = json.dumps(data)
            threading.Thread(target = sendMessage, args = (message, pid,)).start()
            print ('Accepted message sent to client '+str(pid))
            #ADDED BY MAYURESH
            print("Timer started for COMMIT messages")
            threading.Thread(target = startTimerForCommit, args = (15,)).start()        
  
    elif data['type'] == 'accepted':
        incrementInterval(5)
        incrementAcceptCount()  
        for aval in data['value']:
            transaction_log.append(Transaction.load(aval)) 
  
    elif data['type'] == 'commit':
        print ('Decide message from leader')
        # Follower and Leader must check what SEQUENCE NUMBERS ARE MISSING FROM THEIR LOGS
        # FOLLOWER has LESSER SEQUENCE NUMBER THAN LEADER [can happen!]
        # LEADER has LESSER SEQUENCE NUMBER THAN FOLLOWER [can happen!]
        # WHOEVER HAS LESSER SENDS THE SEQUENCE NUMBERS AND REQUESTS FOR THE BLOCK
        # SO ONE MORE PROCESS MESSAGE TYPE
        val = []
        for aval in data['value']:
            val.append(Transaction.load(aval))
        chain.append(data['seq_num'], val)
        transaction_log.clear()
        chain.printChain()


  
def getBalance(pid):
    amount = chain.getBalance(pid)
    for log in transaction_log:
        amount -= log.amount
    return amount                  
    
def processInput(data):
    dataList = data.split(',')
    if dataList[0] == 't':
        # Update getBalance to get balance
        receiver = int(dataList[1])
        amount = int(dataList[2])
        amountBefore = getBalance(PID)
        if amountBefore >= amount and PID != receiver:
            transaction_log.append(Transaction(PID, receiver, amount))
            print("SUCCESS")
            print("Balance before: $"+str(amountBefore))
            print("Balance after: $"+str(amountBefore-amount))
        else:
            # Run Paxos
            sendPrepare()
            
    elif dataList[0] == 'b':
        if len(dataList) == 1:
            dataList.append(str(PID))
        print("Balance: $"+str(getBalance(int(dataList[1]))))

    # CHANGED BY MAYURESH    
    elif dataList[0] == "s": #TO CRASH the client
        # ALSO ASK FOR TIME IT WANTS TO STOP
        LEADER_CRASH_FLAG = True
        FOLLOWER_FLAG_ACCEPT = True #CRASH, SO WE WILL SET THIS TO TRUE SO THAT IT DOESNT START PAXOS
        FOLLOWER_FLAG_COMMIT = True #CRASH, SO WE WILL SET THIS TO TRUE SO THAT IT DOESNT START PAXOS
        FOLLOWER_FLAG_RESET_TIMER = True #CRASH, SO WE WILL SET THIS TO TRUE SO THAT IT DOESNT START PAXOS
        time.sleep(int(input("Enter time to crash"))) #??????????????????????????
        print("Server regained conciousness")
        LEADER_CRASH_FLAG = False
        #DO NOTHING from NOW till new transaction

def createServer(pid):
    try: 
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(('', PORT))         
        print("socket binded to %s" %(PORT))
        s.listen(5)      
        print("socket successfully created")
    except socket.error as err: 
        print("socket creation failed with error %s" %(err)) 

    while True:
        try:
            conn, addr = s.accept()
            data = conn.recv(BUFFER_SIZE).decode('utf-8')
            if not data:
                break
            processMessage(data)
        except expression as identifier:
            print ("Socket error in receiving message")
   
     
if __name__ == "__main__":
    # Reading the client configurations
    f = open(sys.argv[2], 'r')
    configList = f.readlines()
    NUM_CLIENTS = len(configList)
    for i in range(1, NUM_CLIENTS+1):
        config = configList[i-1].strip().split(',')
        if len(config) != 3:
            print("Incorrect configuration")
            sys.exit()
        pidConfig[i] = int(config[1])
        
    config = configList[PID-1].strip().split(',')
    IP = config[0]
    PORT = int(config[1])
    BALANCE = int(config[2])
    
    # Creating server to listen for connections
    server_thread = threading.Thread(target = createServer,args = (PID,)) 
    server_thread.start() 

    print("Balance: $"+str(BALANCE))
    chain = BlockChain(BALANCE)

    while True:
        INPUT = input()
        processInput(INPUT)

