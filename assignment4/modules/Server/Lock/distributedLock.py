# ------------------------------------------------------------------------------
# Distributed Systems (TDDD25)
# ------------------------------------------------------------------------------
# Author: Sergiu Rafiliu (sergiu.rafiliu@liu.se)
# Modified: 01 December 2012
#
# Copyright 2012 Linkoping University
# ------------------------------------------------------------------------------

"""Class for implementing distributed mutual exclusion."""

NO_TOKEN = 0
TOKEN_PRESENT = 1
TOKEN_HELD = 2

class DistributedLock(object):
    """ Implementation of distributed mutual exclusion for a list of peers.

        For simplicity, in this lock implementation we do not deal with cases
        where the peer holding the token dies without passing the token to
        someone else first.
    """

    def __init__(self, owner, peer_list):
        self.peer_list = peer_list
        self.owner = owner
        self.time = 0
        self.token = None
        self.request = {}
        self.state = NO_TOKEN

    # Public methods

    def initialize(self):
        """ Initialize the state, request, and token vectors of the lock.

            Since the state of the distributed lock is linked with the
            number of peers among which the lock is distributed, we can
            utilize the lock of peer_list to protect the state of the
            distributed lock (strongly suggested).

            NOTE: peer_list must already be populated.
        """
        #
        # Your code here.
        #
        pass

    def destroy(self):
        """ The object is being destroyed. If we have the token, we must
            give it to someone else.
        """
        #
        # Your code here.
        #
        pass

    def register_peer(self, pid):
        """Called when a new peer joins the system."""
        #
        # Your code here.
        #
        pass

    def unregister_peer(self, pid):
        """Called when a peer leaves the system."""
        #
        # Your code here.
        #
        pass

    def acquire(self):
        """Called when this object tries to acquire the lock."""
        print "Trying to acquire the lock..."
        #
        # Your code here.
        #
        pass

    def release(self):
        """Called when this object releases the lock."""
        print "Releasing the lock..."
        #
        # Your code here.
        #
        pass

    def request_token(self, time, pid):
        """Called when some other object requests the token from us."""
        #
        # Your code here.
        #
        pass

    def obtain_token(self, token):
        """Called when some other object is giving us the token."""
        print "Receiving the token..."
        #
        # Your code here.
        #
        pass

    def display_status(self):
        self.peer_list.lock.acquire()
        try:
            nt = self.state == NO_TOKEN
            tp = self.state == TOKEN_PRESENT
            th = self.state == TOKEN_HELD
            print "State   :: no token      : {0}".format(nt)
            print "           token present : {0}".format(tp)
            print "           token held    : {0}".format(th)
            print "Request :: {0}".format(self.request)
            print "Token   :: {0}".format(self.token)
            print "Time    :: {0}".format(self.time)
        finally:
            self.peer_list.lock.release()
