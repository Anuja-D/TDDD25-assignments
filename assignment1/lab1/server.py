#!/usr/bin/env python

# ------------------------------------------------------------------------------
# Distributed Systems (TDDD25)
# ------------------------------------------------------------------------------
# Author: Sergiu Rafiliu (sergiu.rafiliu@liu.se)
# Modified: 01 December 2012
#
# Copyright 2012 Linkoping University
# ------------------------------------------------------------------------------

"""Server that serves clients trying to work with the database."""

import threading
import socket
import json
import random
from optparse import OptionParser

import sys
sys.path.append("../modules")
from Server.database import Database
from Server.Lock.readWriteLock import ReadWriteLock

# ------------------------------------------------------------------------------
# Initialize and read the command line arguments
# ------------------------------------------------------------------------------

description = """
    Server for a fortune database. It allows clients to access the database in 
parallel.
"""
arg_parser = OptionParser(description = description)
arg_parser.add_option("-p", "--port", metavar = "PORT", dest = "port",
    help = "Set the port to listen to. Must be in the range 40001 .. 50000."
           "The default value is chosen at random.")
arg_parser.add_option("-f", "--file", metavar = "FILE", dest = "file",
    help = "Set the database file.")
opts, args = arg_parser.parse_args()

# Initialize values for the port and database file name.
if opts.port is None:
        rand = random.Random()
        rand.seed()
        port = rand.randint(1, 10000) + 40000
else:   port = opts.port

if opts.file is None:   db_file = "dbs/fortune.db"
else:                   db_file = opts.file

server_address = ("", port)

# ------------------------------------------------------------------------------
# Auxiliary classes
# ------------------------------------------------------------------------------

class Server(object):
    """Class that provides synchronous access to the database."""

    def __init__(self, db_file):
        self.db = Database(db_file)
        self.rwlock = ReadWriteLock()

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

class Request(threading.Thread):
    """ Class for handling incoming requests.
        Each request is handled in a separate thread.
    """

    def __init__(self, db_server, conn, addr):
        threading.Thread.__init__(self)
        self.db_server = db_server
        self.conn = conn
        self.addr = addr
        self.daemon = True

    # Private methods

    def process_request(self, request):
        """ Process a JSON formated request, send it to the database, and
            return the result.

            The request format is:
                {   "method" : method_name,
                    "params" : parameter_list }

            The returned result is a JSON of the following format:
                -- in case of no error:
                    { "result" : result_of_the_action }
                -- in case of error:
                    { "error" : {   "name" : error_class_name,
                                    "args" : error_args         } }
        """
        #
        # Your code here.
        #
        pass

    def run(self):
        try:
            # Threat the socket as a file stream.
            worker = self.conn.makefile()

            # Read the request in a serialized form (JSON).
            request = worker.readline()

            # Process the request.
            result = self.process_request(request)

            # Send the result.
            worker.write(result + '\n')
            worker.flush()
        except Exception, e:
            # Catch all errors in order to prevent the object from crashing
            # due to bad connections coming from outside.
            print "The connection to the caller has died:"
            print "\t{0}".format(e)
        finally:
            self.conn.shutdown(socket.SHUT_RDWR)
            self.conn.close()

# ------------------------------------------------------------------------------
# The main program
# ------------------------------------------------------------------------------

print "Listening to: {0}:{1}".format(socket.gethostname(), port)
with open("srv_address.tmp", "w") as f:
    f.write("{0}:{1}\n".format(socket.gethostname(), port))

sync_db = Server(db_file)

server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.bind(server_address)
server.listen(1)

print "Press Ctrl-C to stop the server..."

try:
    while True:
        try:
            conn, addr = server.accept()
            req = Request(sync_db, conn, addr)
            print "Serving a request from {0}".format(addr)
            req.start()
        except socket.error:
            continue
except KeyboardInterrupt:
    pass
