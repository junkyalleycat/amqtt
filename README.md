# amqtt
another python asyncio mqtt impl

I wanted to learn asyncio, so I made this.  The objective of this project is to provide a true asyncio mqtt library for python
that provides most of the useful features of mqtt, but does not stive to implement the entire protocol.

mqtt 3.1 and lower are not supported, this is currently hardcoded to use 3.1.1
Qos2 is not supported, it's a myth

This is not a streaming api, so while large messages are not prohibited, it's something to keep in mind.

Usage:

mqtt = MqttSession(handler)
mqtt.loop() => asyncio.task
mqtt.publish(topic, qos, callback=None) => ack (if qos1)
mqtt.puback(packetId)
mqtt.subscribe(list((topicFilter, qos,)), callback=None) => ack
mqtt.unsubscribe(list(topicFilter), callback=None) => ack

Examples:

class Handler:

  def received(self, mqtt, publish):
    pass
  
  def connected(self, mqtt, publish):
    mqtt.subscribe
    
mqtt = MqttSession()
