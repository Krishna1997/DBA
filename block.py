import json

class Transaction:
    def __init__(self, sender=None, receiver=None, amount=None, clock=None):
        self.sender = sender
        self.receiver = receiver
        self.amount = amount
        self.clock = clock

    def toJSON(self):
        return json.dumps(self, default=lambda o: o.__dict__,sort_keys=True)
    
    def toTuple(self):
        return (self.sender, self.receiver, self.amount, self.clock)
    
    @staticmethod
    def load(js):
        trans = Transaction()
        trans.__dict__ = js
        return trans

class BallotNum:
    def __init__(self, num=0, pid=0):
        self.num = num
        self.pid = pid

    def toJSON(self):
        return json.dumps(self, default=lambda o: o.__dict__,sort_keys=True)
    
    def toTuple(self):
        return (self.num, self.pid)
    
    @staticmethod
    def load(js):
        ballot = BallotNum()
        ballot.__dict__ = js
        return ballot
    
    def reset():
        self.num = 0
        self.pid = 0
    
    def isHigher(ballotNum):
        high = False
        if self.num > ballotNum.num:
            high = True
        elif self.num == ballotNum.num and self.pid > ballotNum.pid:
            high = True
        return high


class Node(object):
    def __init__(self, seq_num=0, data=None, next_node=None):
        self.seq_num = seq_num
        self.data = data
        self.next_node = next_node

class BlockChain(object):
    def __init__(self, initial_balance=10, head=None):
        self.head = head
        self.tail = head
        self.initial_balance = initial_balance
    
    def get_head(self):
        return self.head
        
    def append(self, data):
        newNode = Node(1, data)
        if self.head is None:
            self.head = newNode
            self.tail = newNode
        else:
            newNode.seq_num = self.tail.seq_num + 1
            self.tail.next_node = newNode
            self.tail = newNode

    def getBalance(self, user):
        amount = self.initial_balance
        node = self.head
        while node is not None:
            if node.data.sender == user:
                amount -= node.data.amount
            elif node.data.receiver == user:
                amount += node.data.amount
            node = node.next_node
        return amount

    def toList(self):
        chain = []
        node = self.head
        while node is not None:
            chain.append(node.data.toTuple())
            node = node.next_node
        return chain