#!/usr/bin/env python

# ------------------------------------------------------------------------------
# Distributed Systems (TDDD25)
# ------------------------------------------------------------------------------
# Author: Sergiu Rafiliu (sergiu.rafiliu@liu.se)
# Modified: 01 December 2012
#
# Copyright 2012 Linkoping University
# ------------------------------------------------------------------------------

""" This is an implementation of a server that reads and writes data
    to a database.

    This server is one in a group of servers that all replicate the same
    data, so they implement 'read any write all' protocol.
"""

import sys
import random
import socket
import threading
from optparse import OptionParser

sys.path.append("../modules")
from Common import orb
from Common.nameServiceLocation import name_service_address
from Common.objectType import object_type

from Server import database
from Server.peerList import PeerList
from Server.Lock.distributedLock import DistributedLock
from Server.Lock.distributedReadWriteLock import DistributedReadWriteLock

# ------------------------------------------------------------------------------
# Initialize and read the command line arguments
# ------------------------------------------------------------------------------

arg_parser = OptionParser()
arg_parser.add_option("-p", "--port", metavar = "PORT", dest = "port",
    help = "Set the port to listen to. Must be in the range 40001 .. 50000."
           "The default value is chosen at random.")
arg_parser.add_option("-t", "--type", metavar = "TYPE", dest = "type",
    help = "Set the client's type.")
arg_parser.add_option("-f", "--file", metavar = "FILE", dest = "file",
    help = "Set the database file.")
opts, args = arg_parser.parse_args()

if opts.port is None:
        rand = random.Random()
        rand.seed()
        local_port = rand.randint(1, 10000) + 40000
else:   local_port = opts.port

if opts.type is None:   server_type = object_type
else:                   server_type = opts.type
assert server_type != "object", "Change the object type to something unique!"

if opts.file is None:   db_file = "dbs/fortune.db"
else:                   db_file = opts.file

# ------------------------------------------------------------------------------
# Auxiliary classes
# ------------------------------------------------------------------------------

class Server(orb.Peer):
    """Distributed mutual exclusion client class."""

    def __init__(self, local_address, ns_address, server_type, db_file):
        """Initialize the client."""
        orb.Peer.__init__(self, local_address, ns_address, server_type)
        self.peer_list = PeerList(self)
        self.distributed_lock = DistributedLock(self, self.peer_list)
        self.drwlock = DistributedReadWriteLock(self.distributed_lock)
        self.db = database.Database(db_file)
        self.dispatched_calls = {
                "display_peers"     : self.peer_list.display_peers,

                "acquire"           : self.distributed_lock.acquire,
                "release"           : self.distributed_lock.release,
                "request_token"     : self.distributed_lock.request_token,
                "obtain_token"      : self.distributed_lock.obtain_token,
                "display_status"    : self.distributed_lock.display_status
            }
        orb.Peer.start(self)
        self.peer_list.initialize()
        self.distributed_lock.initialize()

    # Public methods

    def destroy(self):
        orb.Peer.destroy(self)
        self.distributed_lock.destroy()
        self.peer_list.destroy()

    def __getattr__(self, attr):
        """Forward calls are dispatched here."""
        if attr in self.dispatched_calls:
            return self.dispatched_calls[attr]
        else:
            raise AttributeError(
                "Client instance has no attribute '{0}'".format(attr))

    # Public methods

    def read(self):
        #
        # Your code here.
        #
        pass

    def write(self, fortune):
        #
        # Your code here.
        #
        pass

    def write_no_lock(self, fortune):
        self.db.write(fortune)

    def register_peer(self, pid, paddr):
        self.peer_list.register_peer(pid, paddr)
        self.distributed_lock.register_peer(pid)

    def unregister_peer(self, pid):
        self.peer_list.unregister_peer(pid)
        self.distributed_lock.unregister_peer(pid)

# ------------------------------------------------------------------------------
# The main program
# ------------------------------------------------------------------------------

# Initialize the client object.
local_address = (socket.gethostname(), local_port)
p = Server(local_address, name_service_address, server_type, db_file)

def menu():
    print (
        "Choose one of the following commands:\n"
        "    l  ::  list peers,\n"
        "    s  ::  display status,\n"
        "    h  ::  print this menu,\n"
        "    q  ::  exit.")

command = ""
cursor = "{0}({1}) > ".format(p.type, p.id)
menu()
while command != "q":
    try:
        sys.stdout.write(cursor)
        command = raw_input()
        if   command == "l":    p.display_peers()
        elif command == "s":    p.display_status()
        elif command == "h":    menu()
    except KeyboardInterrupt:
        break
    except Exception, e:
        # Catch all errors to keep on running in spite of all errors.
        print "An error has occurred: {0}.".format(e)

# Kill our peer object.
p.destroy()
