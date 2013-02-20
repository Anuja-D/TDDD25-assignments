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
        self.wait = False

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
            pids = self.peer_list.peers.keys()
            pids.sort()
            for pid in pids:
                if pid >= self.owner.id:
                    self.state = TOKEN_PRESENT
                    self.token = {str(self.owner.id) : 0}
                break
            
            pids = self.peer_list.peers.keys()
            pids.sort()
            for pid in pids:
                self.request[pid] = 0

        finally:
            self.peer_list.lock.release()

    def destroy(self):
        """ The object is being destroyed. If we have the token, we must
            give it to someone else.
        """
        self.peer_list.lock.acquire()
        try:
            if self.state == TOKEN_HELD:
                self.state = TOKEN_PRESENT
                pids = self.peer_list.peers.keys()
                pids.sort()
                for pid in pids:
                    if self.request[pid] > self.token[str(pid)] and pid != self.owner.id:
                        try:
                            self.peer_list.peers[pid].obtain_token(self.token)
                            break
                        except Exception, e:
                            self.state = TOKEN_PRESENT
                            del self.peer_list.peers[pid]


            if self.state == TOKEN_PRESENT:
                pids = self.peer_list.peers.keys()
                for pid in pids:
                    if pid != self.owner.id:
                        try:
                            self.peer_list.peers[pid].obtain_token(self.token)
                            break
                        except Exception, e:
                            self.state = TOKEN_PRESENT
                            del self.peer_list.peers[pid]


        finally:
            self.peer_list.lock.release()

    def register_peer(self, pid):
        """Called when a new peer joins the system."""
        self.peer_list.lock.acquire()
        try:

            self.request[pid] = 0
            if self.state != NO_TOKEN:
                self.token[str(pid)] = 0

        finally:
            self.peer_list.lock.release()

    def unregister_peer(self, pid):
        """Called when a peer leaves the system."""
        self.peer_list.lock.acquire()
        try:

            del self.request[pid]
            if self.state != NO_TOKEN:
                del self.token[str(pid)]

        finally:
            self.peer_list.lock.release()

    def acquire(self):
        """Called when this object tries to acquire the lock."""
        print "Trying to acquire the lock..."
        self.peer_list.lock.acquire()
        try:

            self.time += 1
            self.request[self.owner.id] = self.time
            if self.state == TOKEN_PRESENT:
                self.wait = False
                self.state = TOKEN_HELD

            if self.state == NO_TOKEN:
                self.wait = True
                pids = self.peer_list.peers.keys()
                pids.sort()
                for pid in pids:
                    if pid != self.owner.id:
                        try:
                            self.peer_list.peers[pid].request_token(self.time, self.owner.id)
                        except Exception, e:
                            del self.peer_list.peers[pid]

        finally:
            self.peer_list.lock.release()

        while self.wait:
            pass


    def release(self):
        """Called when this object releases the lock."""
        print "Releasing the lock..."

        self.peer_list.lock.acquire()
        try:

            if self.state == TOKEN_HELD:
                self.time += 1
                self.state = TOKEN_PRESENT
                pids = self.peer_list.peers.keys()
                pids.sort()
                for pid in pids:
                    if self.request[pid] > self.token[str(pid)] and pid != self.owner.id:
                        self.state = NO_TOKEN
                        self.token[str(self.owner.id)] = self.time
                        try:
                            self.peer_list.peers[pid].obtain_token(self.token)
                            break
                        except Exception, e:
                            self.state = TOKEN_PRESENT
                            del self.peer_list.peers[pid]

        finally:
            self.peer_list.lock.release()

    def request_token(self, time, pid):
        """Called when some other object requests the token from us."""
        print "Get a request token call..."

        self.peer_list.lock.acquire()
        try:

            self.time += 1
            self.request[pid] = max(time, self.request[pid])        
            if self.state == TOKEN_PRESENT and self.request[pid] > self.token[str(pid)]:
                self.state = NO_TOKEN
                self.token[str(self.owner.id)] = self.time
                try:
                    self.peer_list.peers[pid].obtain_token(self.token)
                except Exception, e:
                    self.state = TOKEN_PRESENT
                    del self.peer_list.peers[pid]
                    

        finally:
            self.peer_list.lock.release()

    def obtain_token(self, token):
        """Called when some other object is giving us the token."""
        print "Receiving the token..."

        self.token = token

        if self.request[self.owner.id] > self.token[str(self.owner.id)]:
            self.state = TOKEN_HELD
        else:
            self.state = TOKEN_PRESENT

        self.wait = False         

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
