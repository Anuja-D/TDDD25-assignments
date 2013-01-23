# ------------------------------------------------------------------------------
# Distributed Systems (TDDD25)
# ------------------------------------------------------------------------------
# Author: Sergiu Rafiliu (sergiu.rafiliu@liu.se)
# Modified: 01 December 2012
#
# Copyright 2012 Linkoping University
# ------------------------------------------------------------------------------

"""Implementation of a simple database class."""

import random
import string

class Database(object):
    """Class containing a database implementation."""

    def __init__(self, db_file):
        self.db_file = db_file
        self.rand = random.Random()
        self.rand.seed()
        self.file = open(self.db_file, "a+")
        self.file.seek(0)
        
        raw_db = self.file.read()
        self.db = raw_db.split("%\n")

    def read(self):
        """Read a random location in the database."""
        return self.db[self.rand.randint(0, len(self.db) - 1)]

    def write(self, fortune):
        """Write a new fortune to the database."""

        self.file.write(fortune)
        self.file.write('\n%\n')

        self.db.append(fortune)
