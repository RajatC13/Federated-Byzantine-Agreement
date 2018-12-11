from twisted.internet.protocol import DatagramProtocol
from twisted.internet import reactor
import pickledb
import sys


class Statement:
    def __init__(self, val):
        self.sno = val
        self.command = ""
        self.status = ""
        self.votes = []
        self.accepted = []

class Server(DatagramProtocol):
    def __init__(self, sid):
        self.id = sid
        self.designation = ""
        self.ledger = []
        self.dbname = 'assignment3_add_'+str(self.id)+'.db'
        self.db = pickledb.load(self.dbname, False)

    def startProtocol(self):
        print('server listening')
        self.transport.setTTL(5)
        self.transport.joinGroup("228.0.0.5")

    def datagramReceived(self, datagram, address):
        # Message structure <seq num, command, new/vote/accept, port>
        stmt = datagram.decode("utf-8")
        #print(stmt)
        cmdlist = stmt.split(",")

        if cmdlist[2] == 'EST':
            if(self.id == cmdlist[3]):
                self.designation = "Primary"
            else:
                self.designation = "Follower"
        elif cmdlist[2] == 'NEW' and self.designation == "Primary":
            str = cmdlist[0]+","+cmdlist[1]+","+"INIT"+","+self.id
            self.transport.write(str.encode("utf-8"), ("228.0.0.5", 8005))
        elif cmdlist[2] == 'INIT':
            s = Statement(int(cmdlist[0]))
            s.status = "U"
            print(f'New Statement {cmdlist[0]} {cmdlist[1]}')
            print(f'Voting statement {cmdlist[0]}')
            str = cmdlist[0]+","+cmdlist[1]+","+"VOTE"+","+self.id
            self.transport.write(str.encode("utf-8"), ("228.0.0.5", 8005))
            self.ledger.append(s)
        elif cmdlist[2] == 'VOTE':
            print(f'VOTE for {cmdlist[0]} from {cmdlist[3]}')
            for i in self.ledger:
                if i.sno == int(cmdlist[0]) and int(cmdlist[3]) not in i.votes:
                    i.votes.append(int(cmdlist[3]))
                    if len(i.votes) > 2 and i.status == "U":
                        i.status = "A"
                        print(f'Accepting statement {cmdlist[0]}')
                        str = cmdlist[0]+","+cmdlist[1]+","+"ACCEPT"+","+self.id
                        self.transport.write(str.encode("utf-8"), ("228.0.0.5", 8005))
        elif cmdlist[2] == 'ACCEPT':
            print(f'ACCEPT for {cmdlist[0]} from {cmdlist[3]}')
            for i in self.ledger:
                if i.sno == int(cmdlist[0]) and int(cmdlist[3]) not in i.accepted:
                    i.accepted.append(int(cmdlist[3]))
                    if len(i.accepted) > 2 and i.status == "A":
                        i.status = "C"
                        print(f'Confirming statement {cmdlist[0]}')
                        str = cmdlist[0]+","+cmdlist[1]+","+"CONFIRM"+","+self.id
                        self.transport.write(str.encode("utf-8"), ("228.0.0.5", 8005))
                        keyval = cmdlist[1].split(":")
                        self.db.set(keyval[0], int(self.db.get(keyval[0]))+int(keyval[1]))
        elif cmdlist[2] == 'END':
            #print(self.db.dgetall(self.dbname))
            self.db.dump()




# We use listenMultiple=True so that we can run MulticastServer.py and
# MulticastClient.py on same machine:
if __name__ == '__main__':
    sid = sys.argv[1]
    myserver = Server(sid)
    reactor.listenMulticast(8005, myserver,listenMultiple=True)
    reactor.run()
