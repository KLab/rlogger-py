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
 
  def __init__(self, \
    tag, \
    host='', port=10382, socket_path=None, \
    chunk_size=8388608):
    logging.handlers.SocketHandler.__init__(self, host, port)
    if not isinstance(tag, basestring):
      raise TypeError('tag is not string')
    self.socket_path = socket_path
    self.tag = tag.encode(sys.getdefaultencoding())
    self.hdr_size = len(struct.pack(rloggerNativeHandler.HDR_PACK_FMT, 0, 0, 0, 0, 0))
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
 
  def makeRLoggerPackets(self, record):
    a = []
    b = ''
    t = time.time()
    for ln in self.format(record).splitlines():
      v = ln.encode(sys.getdefaultencoding())
      if len(v) > 0:
        s = self.makeSBuf(t, v)
        if len(b) + len(s) > self.sbuf_size:
          a.append(self.makeSinglePacket(b))
          b = ''
        b += s
    if len(b) > 0:
      a.append(self.makeSinglePacket(b))
    return ''.join(a)
 
  def makeSBuf(self, timestamp, data):
    n = len(data)
    s = struct.pack(\
          rloggerNativeHandler.SBUF_PACK_FMT + str(n) + 's', int(timestamp), n, data)
    return s
 
  def makeSinglePacket(self, buf):
    s = struct.pack(\
          rloggerNativeHandler.HDR_PACK_FMT, \
          rloggerNativeHandler.HDR_VERSION, rloggerNativeHandler.HDR_TYPE_PSH, \
          self.hdr_size + len(self.tag), 0, self.hdr_size + len(self.tag) + len(buf))
    s += self.tag + buf
    return s
 
  def emit(self, record):
    try:
      s = self.makeRLoggerPackets(record)
      if len(s) > 0:
        self.send(s)
    except (KeyboardInterrupt, SystemExit):
      raise
    except:
      self.handleError(record)
 
if __name__ == '__main__':
  def main():
    logger = logging.getLogger('')
    logger.setLevel(logging.DEBUG)
    rh = rloggerNativeHandler('myproject.sample.myhost', socket_path='/var/run/rloggerd/myproject.sock')
    #rh = rloggerNativeHandler('myproject.sample.myhost', host='localhost', port=5555)
    logger.addHandler(rh)
    logger.warning("python rlogger sample 1\npython rlogger sample 2\n")
    time.sleep(60)
    logger.warning("python rlogger sample 3\npython rlogger sample 4\n")
  main()
