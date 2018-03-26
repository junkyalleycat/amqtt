#!/usr/bin/env python3

import asyncio
import struct
import logging
import functools

CONNECT=0b0001
CONNACK=0b0010
SUBSCRIBE=0b1000
SUBACK=0b1001
PUBLISH=0b0011
PUBACK=0b0100
PINGREQ=0b1100
PINGRESP=0b1101
DISCONNECT=0b1110
UNSUBSCRIBE=0b1010
UNSUBACK=0b1011

def writeLengthPrefixedString(writer, s):
    writer.write(struct.pack('!H', len(s)) + s.encode('utf-8'))

def decodeLengthPrefixedString(b):
    l, = struct.unpack('!H', b[0:2])
    return b[2:2+l].decode('utf-8')    

def writeRemaining(writer, l):
    while True:
        byte = l % 128
        l = l // 128
        if l > 0:
            byte |= 0x80
        writer.write(struct.pack('B', byte))
        if l == 0:
            return

async def decodeRemaining(reader):
    remaining = 0
    mult = 1
    while True:
        byte = await reader.readexactly(1)
        byte, = struct.unpack('!B', byte)
        remaining += (byte & 127) * mult 
        mult = mult * 128
        if byte & 128 == 0:
            return remaining

class ConnectMsg:

    def __init__(self):
        self.clientId = None

    def __str__(self):
        return 'Connect'

    def write(self, writer):
        writer.write(struct.pack('B', CONNECT << 4))

        if(self.clientId is None):
            clientId = ""
        else:
            clientId = self.clientId

        remaining = 6 # var.protocolName
        remaining += 1 # var.protocolLevel
        remaining += 1 # var.connectFlags
        remaining += 2 # var.keepAlive
        remaining += 2 + len(clientId) # pay.clientId
        writeRemaining(writer, remaining)

        writeLengthPrefixedString(writer, 'MQTT')
        writer.write(struct.pack('B', 0b100))
        writer.write(struct.pack('B', 0b10))
        writer.write(struct.pack('!H', 600))

        writeLengthPrefixedString(writer, clientId)

class ConnAckMsg:

    def __init__(self):
        self.sessionPresent = None
        self.returnCode = None

    def __str__(self):
        return 'ConnAck[sessionPresent=%s, returnCode=%s]' % (self.sessionPresent, self.returnCode)

    @staticmethod
    def decode(header, remaining, data):
        connAck = ConnAckMsg()
        (sessionPresent, returnCode) = struct.unpack('BB', data)
        connAck.sessionPresent = sessionPresent == 0x1
        connAck.returnCode = returnCode
        return connAck

class UnsubscribeMsg:

    def __init__(self):
        self.topicFilters = []
        self.packetId = None

    def write(self, writer):
        writer.write(struct.pack('B', (UNSUBSCRIBE << 4) | 0b10))

        remaining = 2 # var.packetId
        for topicFilter in self.topicFilters:
            remaining += 2 + len(topicFilter) # pay.topicFilter
        writeRemaining(writer, remaining)

        writer.write(struct.pack('!H', self.packetId))

        for topicFilter in self.topicFilters:
            writeLengthPrefixedString(writer, topicFilter)

    def __str__(self):
        return 'Unsubscribe[packetId=%s, topicFilters=%s]' % (self.packetId, self.subs)

class UnsubAckMsg:

    def __init__(self):
        self.packetId = None

    def __str__(self):
        return 'UnsubAck[packetId=%s]' % self.packetId

    @staticmethod
    def decode(header, remaining, data):
        unsubAck = UnsubAckMsg()
        
        unsubAck.packetId, = struct.unpack('!H', data[0:2])

        return unsubAck

class SubscribeMsg:

    def __init__(self):
        self.subs = []
        self.packetId = None

    def write(self, writer):
        writer.write(struct.pack('B', (SUBSCRIBE << 4) | 0b10))

        remaining = 2 # var.packetId
        for (topicFilter, qos) in self.subs:
            remaining += 2 + len(topicFilter) # pay.topicFilter
            remaining += 1 # pay.qos
        writeRemaining(writer, remaining)

        writer.write(struct.pack('!H', self.packetId))

        for (topicFilter, qos) in self.subs:
            writeLengthPrefixedString(writer, topicFilter)
            writer.write(struct.pack('B', qos))

    def __str__(self):
        return 'Subscribe[packetId=%s, subs=%s]' % (self.packetId, self.subs)

class SubAckMsg:

    def __init__(self):
        self.packetId = None
        self.returnCodes = []

    def __str__(self):
        return 'SubAck[packetId=%s, returnCodes=%s]' % (self.packetId, self.returnCodes)

    @staticmethod
    def decode(header, remaining, data):
        subAck = SubAckMsg()

        subAck.packetId, = struct.unpack('!H', data[0:2])
        for x in range(remaining - 2):
            subAck.returnCodes.append(data[x + 2])

        return subAck

class PublishMsg:

    def __init__(self):
        self.packetId = None
        self.topic = None
        self.payload = None
        self.qos = 0
        self.dup = False
        self.retain = False

    def write(self, writer):
        dup = 0b1000 if self.dup else 0b0
        retain = 0b1 if self.retain else 0b0
        qos = self.qos << 1
        writer.write(struct.pack('B', (PUBLISH << 4) | dup | qos | retain))

        remaining = 2 + len(self.topic) # var.topic
        if qos > 0:
            remaining += 2 # var.packetId
        remaining += len(self.payload) # pay.payload
        writeRemaining(writer, remaining)

        writeLengthPrefixedString(writer, self.topic)
        if qos > 0:
            writer.write(struct.pack('!H', self.packetId))

        writer.write(self.payload)

    def __str__(self):
        return 'Publish[packetId=%s, topic=%s, qos=%s, dup=%s, retain=%s]' % (self.packetId, self.topic, self.qos, self.dup, self.retain)

    @staticmethod
    def decode(header, remaining, data):
        pub = PublishMsg()

        pub.dup = (header & 0b1000) == 0b1000
        pub.retain = (header & 0b1) == 0b1
        pub.qos = (header & 0b110) >> 1
        
        pub.topic = decodeLengthPrefixedString(data)
        i = len(pub.topic) + 2

        if pub.qos > 0:
            pub.packetId, = struct.unpack('!H', data[i:i+2])
            i = i + 2

        pub.payload = data[i:]

        return pub

class PubAckMsg:

    def __init__(self):
        self.packetId = None

    def __str__(self):
        return 'PubAck[packetId=%s]' % self.packetId

    @staticmethod
    def decode(header, remaining, data):
        pubAck = PubAckMsg()

        pubAck.packetId, = struct.unpack('!H', data)

        return pubAck

class PingReqMsg:

    def __str__(self):
        return 'PingReq'

    def write(self, writer):
        writer.write(struct.pack('BB', PINGREQ << 4, 0))

class PingRespMsg:

    def __str__(self):
        return 'PingResp'

    @staticmethod
    def decode(header, remaining, data):
        pingResp = PingRespMsg()
        return pingResp

class DisconnectMsg:

    def __str__(self):
        return 'Disconnect'

    def write(self, writer):
        writer.write(struct.pack('BB', DISCONNECT << 4, 0))

class Timers:

    def __init__(self, loop=None):
        if(loop is None):
            self.loop = asyncio.get_event_loop()
        else:
            self.loop = loop
        self.timers = {}

    def create(self, _id, delay, callback):
        if(_id in self.timers):
            raise Exception("Timer already defined: %s" % _id)

        def fired(_id, callback):
            try:
                del self.timers[_id]
                callback()
            except Exception as e:
                logging.error(e)
                raise

        timer = self.loop.call_later(delay, fired, _id, callback)
        self.timers[_id] = timer

    def cancel(self, _id):
        if(not _id in self.timers):
            raise Exception("Unknown timer: %s" % _id)
        timer = self.timers[_id]
        del self.timers[_id]
        timer.cancel() 

    def reset(self):
        ids = list(self.timers.keys())
        for _id in ids:
            self.cancel(_id)

class Acker:

    def __init__(self, loop=None):
        if(loop is None):
            self.loop = asyncio.get_event_loop()
        else:
            self.loop = loop
        self.acks = {}

    def create(self, callback):
        for x in range(1000):
            if(not x in self.acks):
                ack = self.loop.create_future()
                ack.id = x
                ack.add_done_callback(callback)
                self.acks[x] = ack
                return ack
        raise Exception("Ack exhaustion")

    def complete(self, _id, msg, ignoreMissing=False):
        if(not _id in self.acks):
            if(ignoreMissing):
                return
            raise Exception("Unknown ack: %s" % _id)
        ack = self.acks[_id]
        del self.acks[_id]
        try:
            ack.set_result(msg)
        except Exception as e:
            logging.error(e)
            raise

    def cancel(self, _id):
        if(not _id in self.acks):
            raise Exception("Unknown ack: %s" % _id)
        ack = self.acks[_id]
        del self.acks[_id]
        ack.cancel()

    def reset(self):
        ackIds = list(self.acks.keys())
        for _id in ackIds:
            self.cancel(_id)

# attempt is 0 based, attempt 0 is first time we are sending it
class SimpleRetryPolicy:

    def __init__(self, delay):
        self.delay = delay

    def getAckTimeout(self, attempt):
        return self.delay

    def shouldRetry(self, attempt):
        return True

class SimpleConnectPolicy:

    def __init__(self, delay):
        self.delay = delay

    def getAckTimeout(self):
        return self.delay

    def getRestTime(self):
        return self.delay

# need defined outstanding qos1 message limit
# need defined outstanding subscribe message limit
# need defined outstanding unsubscribe message limit
class MqttSession:

    def __init__(self, host, port, sslcontext=None, clientId=None, handler=None, loop=None):
        self.host = host
        self.port = port
        self.sslcontext = sslcontext
        self.clientId = clientId
        self.handler = handler
        if(loop is None):
            self.loop = asyncio.get_event_loop()
        else:
            self.loop = loop

        self.decoders = 0x10 * [None]
        self.decoders[CONNACK] = ConnAckMsg.decode
        self.decoders[SUBACK] = SubAckMsg.decode
        self.decoders[PUBACK] = PubAckMsg.decode
        self.decoders[PUBLISH] = PublishMsg.decode
        self.decoders[PINGRESP] = PingRespMsg.decode
        self.decoders[UNSUBACK] = UnsubAckMsg.decode

        retryPolicy = SimpleRetryPolicy(5)
        self.publishRetryPolicy = retryPolicy
        self.subscribeRetryPolicy = retryPolicy 
        self.unsubscribeRetryPolicy = retryPolicy

        self.connectPolicy = SimpleConnectPolicy(5)

        self.pingPeriod = 600

        self.timers = Timers()
        self.acker = Acker()

    def start(self):
        self.sessionLoop = self.loop.create_task(self.__start())
        return self.sessionLoop

    async def __start(self):
        while not self.loop.is_closed():
            try:
                (self.reader, self.writer) = await asyncio.open_connection(self.host, self.port, ssl=self.sslcontext)
                self.__connect()
                await self.__loop()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logging.error(e)
                await asyncio.sleep(self.connectPolicy.getRestTime())
            finally:
                self.timers.reset()
                self.acker.reset()

    def __connect(self):
        logging.info("Connecting")
        connect = ConnectMsg()
        connect.clientId = self.clientId

        def timedOut():
            logging.error("Connect timed out")
            self.__close()

        self.timers.create("connect", self.connectPolicy.getAckTimeout(), timedOut)

        self.__write(connect)

    def __close(self):
        self.writer.close()

    def __disconnect(self):
        logging.info("Disconnecting")
        disconnect = DisconnectMsg()
        self.__write(disconnect)

    async def __loop(self):
        pingerTask = None
        try:
            while True:
                header = await self.reader.readexactly(1)
                header, = struct.unpack('B', header)
                remaining = await decodeRemaining(self.reader)
                data = await self.reader.readexactly(remaining)
                msgType = header >> 4
                msg = self.decoders[msgType](header, remaining, data)
                if(isinstance(msg, ConnAckMsg)):
                    self.timers.cancel("connect")
                    pingerTask = self.loop.create_task(self.__pinger())
                    self.handler.connected(msg)
                elif(isinstance(msg, SubAckMsg)):
                    self.acker.complete(msg.packetId, msg, ignoreMissing=True)
                elif(isinstance(msg, UnsubAckMsg)):
                    self.acker.complete(msg.packetId, msg, ignoreMissing=True)
                elif(isinstance(msg, PubAckMsg)):
                    self.acker.complete(msg.packetId, msg, ignoreMissing=True)
                elif(isinstance(msg, PublishMsg)):
                    self.handler.received(msg)
        finally:
            if(not pingerTask is None):
                pingerTask.cancel()

    async def __pinger(self):
        try:
            while True:
                self.__write(PingReqMsg())
                await asyncio.sleep(self.pingPeriod)
        except Exception as e:
            logging.error(e)
            raise

    def __write(self, msg):
        msg.write(self.writer) 

    def stop(self):
        try:
            self.__disconnect()
        except Exception as e:
            logging.error(e)
        self.sessionLoop.cancel()

    def __ackable(self, msg, retryPolicy, callback=None):

        def acked(callback, ack):
            self.timers.cancel(ack.id)
            if(not callback is None):
                callback(self)

        def timedOut(retryPolicy, msg, packetId, attempt):
            if(not retryPolicy.shouldRetry(attempt)):
                self.acker.cancel(packetId)
                self.__disconnect()
            self.timers.create(packetId, retryPolicy.getAckTimeout(attempt), functools.partial(timedOut, retryPolicy, msg, packetId, attempt + 1))
            self.__write(msg)

        ack = self.acker.create(functools.partial(acked, callback))
        msg.packetId = ack.id
        self.timers.create(ack.id, retryPolicy.getAckTimeout(0), functools.partial(timedOut, retryPolicy, msg, ack.id, 1))

        self.__write(msg)

        return ack

    def publish(self, topic, qos, payload, callback=None):
        publish = PublishMsg()
        publish.topic = topic
        publish.qos = qos
        publish.payload = payload

        if(publish.qos == 0):
            self.__write(publish)
        else:
            return self.__ackable(publish, self.publishRetryPolicy, callback=callback)

    def pubAck(self, packetId):
        pubAck = PubAckMsg()
        pubAck.packetId = packetId

        self.__write(pubAck)

    def subscribe(self, subs, callback=None):
        subscribe = SubscribeMsg()
        subscribe.subs = subs

        return self.__ackable(subscribe, self.subscribeRetryPolicy, callback=callback)

    def unsubscribe(self, topicFilters, callback=None):
        unsubscribe = UnsubscribeMsg()
        unsubscribe.topicFilters = topicFilters

        return self.__ackable(unsubscribe, self.unsubscribeRetryPolicy, callback=callbak)
