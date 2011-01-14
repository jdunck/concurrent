# msgauth.py

import hmac
import os

MESSAGE_LENGTH = 32

# Utility function that receives a specified amount of data 
def recv_all(sock,size):
    buf = bytearray()
    while size > 0:
        chunk = sock.recv(size)
        if not chunk:
            raise IOError("Incomplete message")
        buf.extend(chunk)
        size -= len(chunk)
    return buf

# Send an HMAC challenge message and get response
def send_challenge(sock,authkey):
    # Make a message of random bytes and send it
    msg = os.urandom(MESSAGE_LENGTH)
    sock.sendall(msg)
    digest = hmac.new(authkey, msg).digest()

    # Get a response back and check digest
    try:
        recv_digest = recv_all(sock,len(digest))
    except IOError:
        return False

    # Send a one-byte success code to the client
    if (recv_digest == digest):
        sock.sendall(b"\x01")
        return True
    else:
        sock.sendall(b"\x00")
        return False

# Get the challenge message, send response, and check result
def answer_challenge(sock,authkey):
    try:
        message = bytes(recv_all(sock,MESSAGE_LENGTH))
    except IOError:
        return False
    digest = hmac.new(authkey,message).digest()
    sock.sendall(digest)
    try:
        resp = recv_all(sock,1)
        return True if resp[0] == 1 else False
    except IOError:
        return False
