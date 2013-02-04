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

        self.peer_list.lock.acquire()
        try:
            if len(self.peer_list.get_peers()) == 1:
                self.state = TOKEN_PRESENT
                self.token = {str(self.owner.id) : 0}
            
            pids = self.peer_list.peers.keys()
            pids.sort()
            for pid in pids:
                self.request[pid] = 0

        finally:
            self.peer_list.lock.release()
        return "hej"
        

    def destroy(self):
        """ The object is being destroyed. If we have the token, we must
            give it to someone else.
        """
        print "I'm dyyyyying"

        if self.state == TOKEN_HELD:
            self.state = TOKEN_PRESENT
            pids = self.peer_list.peers.keys()
            pids.sort()
            for pid in pids:
                if self.request[pid] > self.token[str(pid)] and pid != self.owner.id:
                    
                    self.peer_list.peer(pid).obtain_token(self.token)
                    break

        if self.state == TOKEN_PRESENT:
            pids = self.peer_list.peers.keys()
            for pid in pids:
                if pid != self.owner.id:
                    self.peer_list.peer(pid).obtain_token(self.token)
                    break
        return "hej"

    def register_peer(self, pid):
        """Called when a new peer joins the system."""
        self.request[pid] = 0
        if self.state != NO_TOKEN:
            self.token[str(pid)] = 0
        return "hej"

    def unregister_peer(self, pid):
        """Called when a peer leaves the system."""
        del self.request[pid]
        if self.state != NO_TOKEN:
            del self.token[str(pid)]
        return "hej"

    def acquire(self):
        """Called when this object tries to acquire the lock."""
        print "Trying to acquire the lock..."
        
        self.time += 1
        self.request[self.owner.id] = self.time
        if self.state == TOKEN_PRESENT:
            self.state = TOKEN_HELD

        if self.state == NO_TOKEN:
            for pid in self.peer_list.peers:
                if pid != self.owner.id:
                    self.peer_list.peer(pid).request_token(self.time, self.owner.id)
        return "hej"

    def release(self):
        """Called when this object releases the lock."""
        print "Releasing the lock..."
        if self.state == TOKEN_HELD:
            self.time += 1
            self.state = TOKEN_PRESENT
            pids = self.peer_list.peers.keys()
            pids.sort()
            for pid in pids:
                print "123"
                if self.request[pid] > self.token[str(pid)]:
                    print "asj"
                    print pid
                    self.state = NO_TOKEN
                    self.token[str(self.owner.id)] = self.time
                    self.peer_list.peer(pid).obtain_token(self.token)
                    break
        return "hej"

    def request_token(self, time, pid):
        """Called when some other object requests the token from us."""
        print "get a request token call"
        self.time += 1
        self.request[pid] = max(time, self.request[pid])        
        if self.state == TOKEN_PRESENT and self.request[pid] > self.token[str(pid)]:
            self.state = NO_TOKEN
            self.token[str(self.owner.id)] = self.time
            self.peer_list.peer(pid).obtain_token(self.token)
        return "hej"

    def obtain_token(self, token):
        """Called when some other object is giving us the token."""
        print "Receiving the token..."
        self.token = token

        if self.request[self.owner.id] > self.token[str(self.owner.id)]:
            self.state = TOKEN_HELD
        else:
            self.state = TOKEN_PRESENT
        
        return "hej" 

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
