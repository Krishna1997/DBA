import socket
import sys
import json
import threading
from threading import Thread, Lock
import time
from tt import TimeTable
from block import Transaction, BlockChain

PID = int(sys.argv[1])
print("Process id: ", PID)

IP = "127.0.0.1"
BUFFER_SIZE = 1024
CLIENTS = []
LAMPORT = 0
BALANCE = 10

PORT = 5000+PID
clientConn = {}
pidConn = {}     

def incrementLamport():
	global LAMPORT
	mutex = Lock()
	mutex.acquire()
	LAMPORT += 1
	mutex.release()
	print ('Lamport Clock: ' + str(LAMPORT))
 
def hasrec(pid, transaction):
    return tt.get(pid-1, transaction.sender-1) >= transaction.clock

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


def processInput(data):
    dataList = data.split(',')
    if dataList[0] == 'transfer':
        amountBefore = chain.getBalance(PID)
        if amountBefore >= int(dataList[2]) and PID != int(dataList[1]):
            incrementLamport()
            chain.append(Transaction(PID, int(dataList[1]), int(dataList[2]), LAMPORT))
            tt.update(PID-1, LAMPORT)
            print("SUCCESS")
            print("Balance before: $"+str(amountBefore))
            print("Balance after: $"+str(amountBefore-int(dataList[2])))
        else:
            print("INCORRECT")
    elif dataList[0] == 'balance':
        if len(dataList) == 1:
            dataList.append(str(PID))
        print("Balance: $"+str(chain.getBalance(int(dataList[1]))))
    elif dataList[0] == 'message':
        if int(dataList[1]) != PID:
            sendLog(int(dataList[1]))
        
        

def processMessage(pid, data):
    global LAMPORT
    
    print ('Message from client ' + str(pid))
    print ('Blockchain before: ', chain.toList())
    data = json.loads(data)
    # mutex = Lock()
    # mutex.acquire()
    # LAMPORT = max(LAMPORT, data['clock']) + 1
    # mutex.release()
    # print ('Lamport Clock: ' + str(LAMPORT))
    events = data['events']
    for event in events:
        event = Transaction.load(json.loads(event))
        if not hasrec(PID, event):
            chain.append(event)
    print ('Blockchain after: ', chain.toList())
    new_table = TimeTable.load(json.loads(data['table']), len(CLIENTS)+1)
    print('Time Table before: ', tt.table)
    print('Time Table sync: ', new_table.table)
    tt.sync(new_table, PID-1, pid-1)
    print('Time Table after: ', tt.table)    

                
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
    f = open(sys.argv[2], 'r')
    configList = f.readlines()
    config = configList[PID-1].strip('\n').split(',')
    if len(config) != 3:
        print("Incorrect configuration")
        sys.exit()
    IP = config[0]
    PORT = int(config[1])
    BALANCE = int(config[2])
    
    server_thread = threading.Thread(target = createServer,args = (PID,)) 
    server_thread.start() 

    for i in range(1, PID):
        clientConfig = configList[i-1].strip('\n').split(',')
        connectToClient(i, clientConfig[0], int(clientConfig[1]))
    print("#clients connected: ", len(CLIENTS))
    print("Balance: $"+str(BALANCE))
    
    tt = TimeTable(len(configList))
    chain = BlockChain()

    while True:
        message = input()
        processInput(message)

