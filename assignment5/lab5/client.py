#!/usr/bin/env python

# ------------------------------------------------------------------------------
# Distributed Systems (TDDD25)
# ------------------------------------------------------------------------------
# Author: Sergiu Rafiliu (sergiu.rafiliu@liu.se)
# Modified: 01 December 2012
#
# Copyright 2012 Linkoping University
# ------------------------------------------------------------------------------

"""Client reader/writer for a fortune database."""

import sys
from optparse import OptionParser

sys.path.append("../modules")
from Common import orb
from Common.nameServiceLocation import name_service_address
from Common.objectType import object_type

# ------------------------------------------------------------------------------
# Initialize and read the command line arguments
# ------------------------------------------------------------------------------

description = """
    Client for a fortune database. It reads a random fortune from the database.
"""

arg_parser = OptionParser(description = description)
arg_parser.add_option("-w", "--write", metavar = "FORTUNE", dest = "fortune",
    help = "Write a new fortune to the database.")
arg_parser.add_option("-i", "--interactive",
    action = "store_true", dest = "interactive", default = False,
    help = "Interactive session with the fortune database.")
arg_parser.add_option("-t", "--type", metavar = "TYPE", dest = "type",
    help = "Set the client's type.")
arg_parser.add_option("-p", "--peer", metavar = "PEER_ID", dest = "peer_id",
    help = "The identifier of a particular server peer.")
opts, args = arg_parser.parse_args()

if opts.type is None:   server_type = object_type
else:                   server_type = opts.type
assert server_type != "object", "Change the object type to something unique!"

server_id = opts.peer_id

# ------------------------------------------------------------------------------
# The main program
# ------------------------------------------------------------------------------

# Connect to the name service to obtain the address of the server.
ns = orb.Stub(name_service_address)

if server_id is None:
    server_address = tuple(ns.require_any(server_type))
else:
    server_address = tuple(ns.require_object(server_type, int(server_id)))

print "Connecting to server: {0}".format(server_address)

# Create the database object.
db = orb.Stub(server_address)

if not opts.interactive:
    # Run in the normal mode.
    if opts.fortune is not None:
        print "Writing '{0}' to the fortune database.".format(opts.fortune)
        db.write(opts.fortune)
    else:
        print db.read()

else:
    # Run in the interactive mode.
    def menu():
        print (
            "Choose one of the following commands:\n"
            "    r            ::  read a random fortune from the database,\n"
            "    w <FORTUNE>  ::  write a new fortune into the database,\n"
            "    h            ::  print this menu,\n"
            "    q            ::  exit.")

    command = ""
    menu()
    while command != "q":
        sys.stdout.write("Command> ")
        command = raw_input()
        if command == "r":
            print db.read()
        elif len(command) > 1 and command[0] == "w" and command[1] in [" ", "\t"]:
            db.write(command[2:].strip())
        elif command == "h":
            menu()
