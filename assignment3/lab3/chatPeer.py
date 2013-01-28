#!/usr/bin/env python

# ------------------------------------------------------------------------------
# Distributed Systems (TDDD25)
# ------------------------------------------------------------------------------
# Author: Sergiu Rafiliu (sergiu.rafiliu@liu.se)
# Modified: 01 December 2012
#
# Copyright 2012 Linkoping University
# ------------------------------------------------------------------------------

""" A simple chat client

    This chat client retains a list of peers so that it can send messages
    to each of them.
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

from Server.peerList import PeerList

# ------------------------------------------------------------------------------
# Initialize and read the command line arguments
# ------------------------------------------------------------------------------

arg_parser = OptionParser()
arg_parser.add_option("-p", "--port", metavar = "PORT", dest = "port",
    help = "Set the port to listen to. Must be in the range 40001 .. 50000."
           "The default value is chosen at random.")
arg_parser.add_option("-t", "--type", metavar = "TYPE", dest = "type",
    help = "Set the client's type.")
opts, args = arg_parser.parse_args()

if opts.port is None:
        rand = random.Random()
        rand.seed()
        local_port = rand.randint(1, 10000) + 40000
else:   local_port = opts.port

if opts.type is None:   client_type = object_type
else:                   client_type = opts.type
assert client_type != "object", "Change the object type to something unique!"

# ------------------------------------------------------------------------------
# Auxiliary classes
# ------------------------------------------------------------------------------

class Client(orb.Peer):
    """Chat client class."""

    def __init__(self, local_address, ns_address, cient_type):
        """Initialize the client."""
        orb.Peer.__init__(self, local_address, ns_address, client_type)
        self.peer_list = PeerList(self)
        self.dispatched_calls = {
                "register_peer"     : self.peer_list.register_peer,
                "unregister_peer"   : self.peer_list.unregister_peer,
                "display_peers"     : self.peer_list.display_peers
            }
        orb.Peer.start(self)
        self.peer_list.initialize()

    # Public methods

    def destroy(self):
        orb.Peer.destroy(self)
        self.peer_list.destroy()

    def __getattr__(self, attr):
        """Forward calls are dispatched here."""
        if attr in self.dispatched_calls:
            return self.dispatched_calls[attr]
        else:
            raise AttributeError("Client instance has no attribute '{0}'".format(attr))

    def print_message(self, from_id, msg):
        print "Received a message from {0}: {1}".format(from_id, msg)

    def send_message(self, to_id, msg):
        try:
            self.peer_list.peer(to_id).print_message(self.id, msg)
        except Exception, e:
            print ("Cannot send messages to {0}. "
                  "Make sure it is in the list of peers.").format(to_id)

# ------------------------------------------------------------------------------
# The main program
# ------------------------------------------------------------------------------

# Initialize the client object.
local_address = (socket.gethostname(), local_port)
p = Client(local_address, name_service_address, client_type)

def menu():
    print (
        "Choose one of the following commands:\n"
        "    l                       ::  display the peer list,\n"
        "    <PEER_ID> : <MESSAGE>   ::  send <MESSAGE> to <PEER_ID>,\n"
        "    h                       ::  print this menu,\n"
        "    q                       ::  exit.")

command = ""
cursor = "{0}({1}) > ".format(p.type, p.id)
menu()
while command != "q":
    try:
        sys.stdout.write(cursor)
        command = raw_input()
        if command == "l":
            p.display_peers()
        elif command == "h":
            menu()
        else:
            pos = command.find(":")
            if pos > -1 and command[0:pos].strip().isdigit():
                # We have a command to send a message to someone.
                to_id = int(command[0:pos])
                msg = command[pos + 1:]
                p.send_message(to_id, msg)
    except KeyboardInterrupt:
        break

# Kill our peer object.
p.destroy()
