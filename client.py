from twisted.internet.protocol import DatagramProtocol
from twisted.internet import reactor
import pickledb, sys

db = pickledb.load('client.db', False)

class Client(DatagramProtocol):

    def __init__(self, p):
        self.primary = p

    def startProtocol(self):
        # Join the multicast address, so we can receive replies:
        self.transport.joinGroup("228.0.0.5")
        # Send to 228.0.0.5:8005 - all listeners on the multicast address
        # (including us) will receive this message.
        print("1. foo:$10")
        print("2. bar:$30")
        print("3. foo:$20")
        print("4. bar:$20")
        print("5. foo:$30")
        print("6. bar:$10")


        self.transport.write(b'x,x,EST,3000', ("228.0.0.5", 8005))
        self.transport.write(b'1,foo:10,NEW,3000', ("228.0.0.5", 8005))
        self.transport.write(b'2,bar:30,NEW,3000', ("228.0.0.5", 8005))
        self.transport.write(b'3,foo:20,NEW,3000', ("228.0.0.5", 8005))
        self.transport.write(b'4,bar:20,NEW,3000', ("228.0.0.5", 8005))
        self.transport.write(b'5,foo:30,NEW,3000', ("228.0.0.5", 8005))
        self.transport.write(b'6,bar:10,NEW,3000', ("228.0.0.5", 8005))

        #self.transport.write(b'x,x,END,x',("228.0.0.5", 8005))


    def datagramReceived(self, datagram, address):
        cmdlist = datagram.decode("utf-8")
        c = cmdlist.split(",")
        if(c[2]=='CONFIRM'):
            print(cmdlist)
            self.transport.write(b'x,x,END,x',("228.0.0.5", 8005))

    def printdb(self):
        print(db.getall())

if __name__ == '__main__':
    p = sys.argv[1]
    myclient = Client(p)
    reactor.listenMulticast(8005, myclient, listenMultiple=True)
    reactor.run()
