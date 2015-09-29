import logging.handlers
import socket
import sys
import struct
import time


class rloggerNativeHandler(logging.handlers.SocketHandler):
    HDR_VERSION = 1
    HDR_TYPE_PSH = 1
    HDR_PACK_FMT = '!BBHLL'
    SBUF_PACK_FMT = '!LL'

    def __init__(self,
                 tag,
                 host='', port=10382, socket_path=None,
                 chunk_size=8388608):
        logging.handlers.SocketHandler.__init__(self, host, port)
        if not isinstance(tag, basestring):
            raise TypeError('tag is not string')
        self.socket_path = socket_path
        self.tag = tag.encode(sys.getdefaultencoding())
        self.hdr_size = len(
            struct.pack(rloggerNativeHandler.HDR_PACK_FMT, 0, 0, 0, 0, 0))
        self.sbuf_size = chunk_size - (self.hdr_size + len(self.tag))

    def makeUnixSocket(self):
        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        s.connect(self.socket_path)
        return s

    def createSocket(self):
        if self.socket_path and len(self.socket_path) > 0:
            # same as SocketHandler.createSocket except makeUnixSocket
            now = time.time()
            if self.retryTime is None:
                attempt = 1
            else:
                attempt = (now >= self.retryTime)
            if attempt:
                try:
                    self.sock = self.makeUnixSocket()
                    self.retryTime = None
                except socket.error:
                    if self.retryTime is None:
                        self.retryPeriod = self.retryStart
                    else:
                        self.retryPeriod = self.retryPeriod * self.retryFactor
                        if self.retryPeriod > self.retryMax:
                                self.retryPeriod = self.retryMax
                    self.retryTime = now + self.retryPeriod
        else:
            logging.handlers.SocketHandler.createSocket(self)

    def sbuf_gen(self, t, s):
        b = buffer(s.encode(sys.getdefaultencoding()))
        sp = 0
        while True:
            ep = sp + self.sbuf_size
            v = b[sp:ep]
            n = len(v)
            if n == 0:
                break
            yield struct.pack(
                rloggerNativeHandler.SBUF_PACK_FMT + str(n) + 's',
                int(t), n, v
            )
            sp += n

    def makeSinglePacket(self, buf):
        s = struct.pack(
            rloggerNativeHandler.HDR_PACK_FMT,
            rloggerNativeHandler.HDR_VERSION,
            rloggerNativeHandler.HDR_TYPE_PSH,
            self.hdr_size + len(self.tag),
            0,
            self.hdr_size + len(self.tag) + len(buf)
        )
        s += self.tag + buf
        return s

    def packet_gen(self, r):
        t = time.time()
        b = ''
        for ln in self.format(r).splitlines():
            for s in self.sbuf_gen(t, ln):
                if self.sbuf_size < len(b) + len(s):
                    yield self.makeSinglePacket(b)
                    b = ''
                b += s
        if len(b) > 0:
            yield self.makeSinglePacket(b)

    def emit(self, record):
        try:
            for p in self.packet_gen(record):
                self.send(p)
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            self.handleError(record)

if __name__ == '__main__':
    def main():
        logger = logging.getLogger('')
        logger.setLevel(logging.DEBUG)
        rh = rloggerNativeHandler(
            'myproject.sample.myhost',
            socket_path='/var/run/rloggerd/myproject.sock'
        )
#        rh = rloggerNativeHandler(
#            'myproject.sample.myhost',
#            host='localhost', port=5555
#        )
        logger.addHandler(rh)
        logger.warning("python rlogger sample 1\npython rlogger sample 2\n")
        time.sleep(60)
        logger.warning("python rlogger sample 3\npython rlogger sample 4\n")
    main()
