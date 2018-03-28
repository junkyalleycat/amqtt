#!/usr/bin/env python3

import asyncio
import struct
import logging
import functools
import math

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

def writeLengthPrefixedBytes(writer, data):
    writer.write(struct.pack('!H', len(data)))
    writer.write(data)

def decodeLengthPrefixedBytes(data, i):
    l, = struct.unpack_from('!H', data, offset=i)
    return data[2:2+l]

def writeRemaining(writer, l):
    while True:
        byte = l % 128
        l = l // 128
        if(l > 0):
            byte |= 0x80
        writer.write(struct.pack('B', byte))
        if(l == 0):
            return

async def decodeRemaining(reader):
    remaining = 0
    mult = 1
    while True:
        byte = await reader.readexactly(1)
        byte, = struct.unpack('!B', byte)
        remaining += (byte & 127) * mult 
        mult = mult * 128
        if((byte & 128) == 0):
            return remaining

class ConnectMsg:

    def __init__(self):
        self.clientId = None
        self.username = None
        self.password = None
        self.will = None
        self.willTopic = None
        self.willQos = 0
        self.willRetain = False
        self.clean = False
        self.protocolName = 'MQTT'
        self.protocolLevel = 4
        self.keepAlive = 600

    def __str__(self):
        return 'Connect[clientId=%s, username=%s, willTopic=%s, willQos=%s, willRetain=%s, clean=%s, protocolName=%s, protocolLevel=%s, keepAlive=%s' % (
            self.clientId, self.username, self.willTopic, self.willQos, self.willRetain, self.clean, self.protocolName, self.protocolLevel, self.keepAlive)

    def write(self, writer):
        if((self.username is None) and (not self.password is None)):
            raise Exception("Password requires username")

        if((self.will is None) and (not self.willTopic is None)):
            raise Exception("willTopic requires will")

        if((self.willTopic is None) and (not self.will is None)):
            raise Exception("will requires willTopic")

        clientId = ("" if self.clientId is None else self.clientId).encode('utf-8')
        protocolName = self.protocolName.encode('utf-8')
        will = None if self.will is None else self.will.encode('utf-8')
        willTopic = None if self.willTopic is None else self.willTopic.encode('utf-8')
        username = None if self.username is None else self.username.encode('utf-8')

        connectFlags = 0
        remaining = 2 + len(protocolName) # var.protocolName
        remaining += 1 # var.protocolLevel
        remaining += 1 # var.connectFlags
        remaining += 2 # var.keepAlive
        remaining += 2 + len(clientId) # pay.clientId
        if(not will is None):
            connectFlags |= 0b100
            connectFlags |= 0b100000 if self.willRetain else 0b0
            connectFlags |= self.willQos << 3
            remaining += 2 + len(willTopic) # pay.willTopic
            remaining += 2 + len(will) # pay.will
        if(not username is None):
            connectFlags |= 0b10000000
            remaining += 2 + len(username) # pay.username
        if(not self.password is None):
            connectFlags |= 0b1000000
            remaining += 2 + len(self.password) # pay.password
        if(self.clean):
            connectFlags |= 0b10

        # header
        writer.write(struct.pack('B', CONNECT << 4))
        writeRemaining(writer, remaining)

        # variable
        writeLengthPrefixedBytes(writer, protocolName)
        writer.write(struct.pack('B', self.protocolLevel))
        writer.write(struct.pack('B', connectFlags))
        writer.write(struct.pack('!H', self.keepAlive))

        # payload
        writeLengthPrefixedBytes(writer, clientId)
        if(not will is None):
            writeLengthPrefixedBytes(writer, willTopic)
            writeLengthPrefixedBytes(writer, will)
        if(not username is None):
            writeLengthPrefixedBytes(writer, username)
        if(not self.password is None):
            writeLengthPrefixedBytes(writer, self.password)

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

        remaining = 2 # var.packetId
        topicFilters = []
        for rawTopicFilter in self.topicFilters:
            topicFilter = rawTopicFilter.encode('utf-8')
            topicFilters.append(topicFilter)
            remaining += 2 + len(topicFilter) # pay.topicFilter

        # header
        writer.write(struct.pack('B', (UNSUBSCRIBE << 4) | 0b10))
        writeRemaining(writer, remaining)

        # variable
        writer.write(struct.pack('!H', self.packetId))

        # payload
        for topicFilter in topicFilters:
            writeLengthPrefixedBytes(writer, topicFilter)

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
        
        unsubAck.packetId, = struct.unpack('!H', data)

        return unsubAck

class SubscribeMsg:

    def __init__(self):
        self.subs = []
        self.packetId = None

    def write(self, writer):

        remaining = 2 # var.packetId
        subs = []
        for (rawTopicFilter, qos) in self.subs:
            topicFilter = rawTopicFilter.encode('utf-8')
            subs.append((topicFilter, qos,))
            remaining += 2 + len(topicFilter) # pay.topicFilter
            remaining += 1 # pay.qos

        # header
        writer.write(struct.pack('B', (SUBSCRIBE << 4) | 0b10))
        writeRemaining(writer, remaining)

        # variable
        writer.write(struct.pack('!H', self.packetId))

        # payload
        for (topicFilter, qos) in subs:
            writeLengthPrefixedBytes(writer, topicFilter)
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

        i = 0

        subAck.packetId, = struct.unpack_from('!H', data, offset=i)
        i += 2

        for x in range(i, remaining):
            subAck.returnCodes.append(data[x])

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
        topic = self.topic.encode('utf-8')

        remaining = 2 + len(topic) # var.topic
        if(qos > 0):
            remaining += 2 # var.packetId
        remaining += len(self.payload) # pay.payload

        # header
        writer.write(struct.pack('B', (PUBLISH << 4) | dup | qos | retain))
        writeRemaining(writer, remaining)

        # variable
        writeLengthPrefixedBytes(writer, topic)
        if(qos > 0):
            writer.write(struct.pack('!H', self.packetId))

        # payload
        writer.write(self.payload)

    def __str__(self):
        return 'Publish[packetId=%s, topic=%s, qos=%s, dup=%s, retain=%s]' % (self.packetId, self.topic, self.qos, self.dup, self.retain)

    @staticmethod
    def decode(header, remaining, data):
        pub = PublishMsg()

        pub.dup = (header & 0b1000) == 0b1000
        pub.retain = (header & 0b1) == 0b1
        pub.qos = (header & 0b110) >> 1
        
        i = 0
        pub.topic = decodeLengthPrefixedBytes(data, i).decode('utf-8')
        i += len(pub.topic) + 2

        if(pub.qos > 0):
            pub.packetId, = struct.unpack_from('!H', data, offset=i)
            i += 2

        pub.payload = data[i:]

        return pub

class PubAckMsg:

    def __init__(self):
        self.packetId = None

    def __str__(self):
        return 'PubAck[packetId=%s]' % self.packetId

    def write(self, writer):
        remaining = 2 # var.packetId

        # header
        writer.write(struct.pack('B', PUBACK << 4))
        writeRemaining(writer, remaining)

        # variable
        writer.write(struct.pack('!H', self.packetId))

    @staticmethod
    def decode(header, remaining, data):
        pubAck = PubAckMsg()

        pubAck.packetId, = struct.unpack('!H', data)

        return pubAck

class PingReqMsg:

    def __str__(self):
        return 'PingReq'

    def write(self, writer):
        # header
        writer.write(struct.pack('BB', PINGREQ << 4, 0))

class PingRespMsg:

    def __str__(self):
        return 'PingResp'

    @staticmethod
    def decode(header, remaining, data):
        return PingRespMsg()

class DisconnectMsg:

    def __str__(self):
        return 'Disconnect'

    def write(self, writer):
        # header
        writer.write(struct.pack('BB', DISCONNECT << 4, 0))

decoders = 0x10 * [None]
decoders[CONNACK] = ConnAckMsg.decode
decoders[SUBACK] = SubAckMsg.decode
decoders[PUBACK] = PubAckMsg.decode
decoders[PUBLISH] = PublishMsg.decode
decoders[PINGRESP] = PingRespMsg.decode
decoders[UNSUBACK] = UnsubAckMsg.decode

class Timers:

    def __init__(self, loop):
        self._loop = loop
        self._timers = {}

    def create(self, _id, delay, callback):
        if(_id in self._timers):
            raise Exception("Timer already defined: %s" % _id)

        def fired():
            try:
                del self._timers[_id]
                callback()
            except Exception as e:
                logging.exception(e)
                raise

        timer = self._loop.call_later(delay, fired)
        self._timers[_id] = timer

    def cancel(self, _id, ignoreMissing=False):
        if(not _id in self._timers):
            if(ignoreMissing):
                return
            raise Exception("Unknown timer: %s" % _id)
        timer = self._timers[_id]
        del self._timers[_id]
        timer.cancel() 

    def cancelAll(self):
        for _id in list(self._timers.keys()):
            self.cancel(_id)

class Acker:

    def __init__(self, limits, loop):
        self._loop = loop
        self._limits = limits
        self._acks = {}
        self._used = bytearray(math.ceil(limits.ackLimit / 8))

        # next id to try after previously found one
        self._packetId = 0

    def create(self, callback):
        packetId = self._claimPacketId()
        ack = self._loop.create_future()
        ack.id = packetId
        ack.add_done_callback(callback)
        self._acks[packetId] = ack
        return ack

    def _claimPacketId(self):
        attempt = 0
        while(True):
            if(self._packetId >= self._limits.ackLimit):
                self._packetId = 0

            i = math.floor(self._packetId / 8)
            mask = 1 << (self._packetId % 8)

            if((self._used[i] & mask) == 0):
                self._used[i] |= mask
                break

            self._packetId += 1

            attempt += 1
            if(attempt >= self._limits.ackLimit):
                raise Exception("Ack exchaustion")

        packetId = self._packetId
        self._packetId += 1
        return packetId

    def _freePacketId(self, packetId):
        i = math.floor(packetId / 8)
        mask = (1 << (packetId % 8)) ^ 0xFF
        self._used[i] &= mask

    def complete(self, _id, result=None, exception=None, ignoreMissing=False):
        if(not _id in self._acks):
            if(ignoreMissing):
                return
            raise Exception("Unknown ack: %s" % _id)
        ack = self._acks[_id]
        del self._acks[_id]
        self._freePacketId(ack.id)

        if(not result is None):
            ack.set_result(result)
        if(not exception is None):
            ack.set_exception(exception)

    def cancel(self, _id):
        if(not _id in self._acks):
            raise Exception("Unknown ack: %s" % _id)
        ack = self._acks[_id]
        del self._acks[_id]
        self._freePacketId(ack.id)
        ack.cancel()

    def cancelAll(self):
        for _id in list(self._acks.keys()):
            self.cancel(_id)

# attempt is 0 based, attempt 0 is first time we are sending it
class SimpleRetryPolicy:

    def __init__(self, delay):
        self._delay = delay

    def getAckTimeout(self, attempt):
        return self._delay

    def shouldRetry(self, attempt):
        return True

class SimpleConnectPolicy:

    def __init__(self, delay):
        self._delay = delay

    def getAckTimeout(self):
        return self._delay

    def getRestTime(self):
        return self._delay

class SimpleLimits:

    def __init__(self, ackLimit):
        self.ackLimit = ackLimit

class AckTimeoutException:

    pass

# need defined outstanding qos1 message limit
# need defined outstanding subscribe message limit
# need defined outstanding unsubscribe message limit
class MqttSession:

    def __init__(self, connector, handler, loop=None):
        self._connector = connector
        self._handler = handler
        self._loop = asyncio.get_event_loop() if loop is None else loop

        self.defaultRetryPolicy = SimpleRetryPolicy(5)
        self.publishRetryPolicy = self.defaultRetryPolicy
        self.subscribeRetryPolicy = self.defaultRetryPolicy 
        self.unsubscribeRetryPolicy = self.defaultRetryPolicy

        self.connectPolicy = SimpleConnectPolicy(5)

        self.pingPeriod = 600

        self.limits = SimpleLimits(100)

        self._timers = Timers(loop)
        self._acker = Acker(self.limits, loop)

    def connect(self, connect):
        self.sessionLoop = self._loop.create_task(self.__start(connect))
        return self.sessionLoop

    async def __start(self, connect):
        while not self._loop.is_closed():
            self.writer = None
            try:
                (self.reader, self.writer) = await self._connector()
                await self.__loop(connect)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logging.exception(e)
            finally:
                if(not self.writer is None):
                    self.writer.close()
        
                # cancelling all acks will also cancel all ack timers
                # so to prevent unknown timer exceptions cancel them first
                self._acker.cancelAll()
                self._timers.cancelAll()

            await asyncio.sleep(self.connectPolicy.getRestTime())

    def __close(self):
        self.writer.close()

    def __disconnect(self):
        disconnect = DisconnectMsg()
        self.__write(disconnect)

    async def stop(self):
        try:
            self.__disconnect()
            await self.writer.drain()
        except Exception as e:
            logging.exception(e)
        self.sessionLoop.cancel()

    async def __loop(self, connect):
        def connectTimedOut():
            logging.error("Connect timed out")
            self.__close()

        self._timers.create("connect", self.connectPolicy.getAckTimeout(), connectTimedOut)
        self.__write(connect)

        pingerTask = None
        try:
            while True:
                header = await self.reader.readexactly(1)
                header, = struct.unpack('B', header)
                remaining = await decodeRemaining(self.reader)
                data = await self.reader.readexactly(remaining)
                msgType = header >> 4
                msg = decoders[msgType](header, remaining, data)
                if(isinstance(msg, ConnAckMsg)):
                    self._timers.cancel("connect")
                    pingerTask = self._loop.create_task(self.__pinger())
                    self._handler.connected(msg)
                elif(isinstance(msg, SubAckMsg)):
                    self._acker.complete(msg.packetId, result=msg, ignoreMissing=True)
                elif(isinstance(msg, UnsubAckMsg)):
                    self._acker.complete(msg.packetId, result=msg, ignoreMissing=True)
                elif(isinstance(msg, PubAckMsg)):
                    self._acker.complete(msg.packetId, result=msg, ignoreMissing=True)
                elif(isinstance(msg, PublishMsg)):
                    self._handler.received(msg)
        finally:
            if(not pingerTask is None):
                pingerTask.cancel()

    async def __pinger(self):
        try:
            while True:
                self.__write(PingReqMsg())
                await asyncio.sleep(self.pingPeriod)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logging.exception(e)

    def __write(self, msg):
        msg.write(self.writer) 

    def __ackable(self, msg, retryPolicy, callback=None):

        def acked(ack):
            self._timers.cancel(ack.id, ignoreMissing=True)
            if(not callback is None):
                callback(self, ack)

        def timedOut(packetId, attempt):
            if(retryPolicy.shouldRetry(attempt)):
                self._timers.create(packetId, retryPolicy.getAckTimeout(attempt), functools.partial(timedOut, packetId, attempt + 1))
                self.__write(msg)
            else:
                self._acker.complete(packetId, exception=AckTimeoutException())

        ack = self._acker.create(acked)
        msg.packetId = ack.id
        self._timers.create(ack.id, retryPolicy.getAckTimeout(0), functools.partial(timedOut, ack.id, 1))

        self.__write(msg)

        return ack

    def publish(self, topic, qos, payload, dup=False, retain=False, callback=None):
        publish = PublishMsg()
        publish.topic = topic
        publish.qos = qos
        publish.payload = payload
        publish.dup = dup
        publish.retain = retain

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
