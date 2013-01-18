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

from optparse import OptionParser

import sys
sys.path.append("../modules")
from Server.database import Database

# ------------------------------------------------------------------------------
# Initialize and read the command line arguments
# ------------------------------------------------------------------------------

description = """
    Client for a fortune database. It reads a random fortune from the database.
"""
arg_parser = OptionParser(description = description)
arg_parser.add_option("-w", "--write", metavar = "FORTUNE", dest = "fortune",
    help =  "Write a new fortune to the database.")
arg_parser.add_option("-i", "--interactive",
    action = "store_true", dest = "interactive", default = False,
    help = "Interactive session with the fortune database.")
opts, args = arg_parser.parse_args()

# ------------------------------------------------------------------------------
# The main program
# ------------------------------------------------------------------------------

# Create the database object
db = Database("dbs/fortune.db")

if not opts.interactive:
    # Run in the normal mode
    if opts.fortune is not None:
        db.write(opts.fortune)
    else:
        print db.read()

else:
    # Run in the interactive mode
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
