import BaseHTTPServer
import sys
import getopt
import threading
import signal
import socket
import httplib
import random
import string
import time
import os.path

defaultTestIp = "c1:8111"

# This class is for testing, bypasses any remote API calls to nodes 
# and emulates them instead

class TestRequests():
    nodes = [defaultTestIp, "c2:8111", "c3:8111", "c4:8111", "c5:8111"]

    # Return a list of nodes connected to id 
    def getNodes(self, ipport):
        # Find the next index to create a circular network
        try:
            nextIndex = (self.nodes.index(ipport) + 1) % len(self.nodes)
        except: 
            print "Unable to find {0} in list of nodes".format(ipport)
        retval = []
        retval.append(self.nodes[ nextIndex ])
        return retval
        
    # Get the current leader from ip
    def getLeader(self, ipport):
        # random (changes every second)
        return self.nodes[random.randint(0, len(self.nodes) - 1)]
        
class RemoteRequest(TestRequests):
    
    # Return a list of nodes connected to id 
    def getNodes(self, ipport):
        ip, port = ipport.split(":")
        try:
            conn = httplib.HTTPConnection(ip, port)
            conn.request("GET", "/getNodes")
            response = conn.getresponse()
            if response.status != 200:
                print response.reason
                return False
                
            retrievedValue = response.read()
        except:
            print "Unable to send GET request"
            exit(1)
            
        #return nodes as list of 
        return retrievedValue.splitlines()
        
    # Get the current leader from ip
    def getLeader(self, ipport):
        ip, port = ipport.split(":")
        try:
            conn = httplib.HTTPConnection(ip, port)
            conn.request("GET", "/getCurrentLeader")
            response = conn.getresponse()
            if response.status != 200:
                print response.reason
                return False
                
            retrievedValue = response.read()
        except:
            print "Unable to send GET request"
            exit(1)
            
        #return nodes as list of 
        return retrievedValue
        
class Benchmark():

    nodes = []
    testsToRun = 10
    
    # Runs a series of tests against the system (given an initial ip address)
    def run(self, ipport = defaultTestIp, requestHandler = TestRequests()):
        self.addNode(ipport)
        
        # Run tests forever
        for testId in range(self.testsToRun):
            time.sleep(1)
            print "Running test iteration {0}/{1}".format(testId + 1,self.testsToRun)
            
            # Check all available nodes
            for node in self.nodes:
                connected = requestHandler.getNodes(node)
                print "Node {0} is connected to: {1}".format(node, connected)
                
                # Add any new nodes
                for edge in connected:
                    self.addNode(edge)
            
            node = self.nodes[random.randint(0, len(self.nodes) - 1)]
            leader = requestHandler.getLeader(node)
            
            print "Current leader is {0} according to node {1}".format(leader, node)
            
            
    # Adds a new node to the list of nodes if not allready present
    def addNode(self, ipport):
        if not ipport in self.nodes:
            self.nodes.append(ipport)

if __name__ == '__main__':
    
    node_ip = node_port = None

    try:
        optlist, args = getopt.getopt(sys.argv[1:], 'x', ['port=', 'ip='])
    except getopt.GetoptError:
        print sys.argv[0] + ' [--ip ipaddress] [--port portnumber]'
        sys.exit(2)
    
    for opt, arg in optlist:
        if opt in ("-ip", "--ip"):
            node_ip = arg
        elif opt in ("-port", "--port"):
            node_port = int(arg)
    
    if node_ip is None and node_port is None:
        print "Running dummy test, usage:"
        print sys.argv[0] + ' [--ip ipaddress] [--port portnumber]'
        bench = Benchmark()
        bench.run()
    else:
        bench = Benchmark()
        bench.run("{0}:{1}".format(node_ip, node_port), RemoteRequest())
    