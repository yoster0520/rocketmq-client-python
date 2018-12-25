# -*- coding: utf-8 -*-
from librocketmqclientpython import *

class Message(object):
    
    def __init__(self, topic, body, properties = None, tag = None):
        print "create msg for topic %s" % topic
        self.topic = topic
        self.msg = CreateMessage(topic)
        SetMessageBody(self.msg, body)

        if tag is not None:
            SetMessageTags(self.msg, tag)

        if properties is not None:
            for key in properties:
                SetMessageProperty(self.msg, key, properties[key])

    def __enter__(self):
        return self.msg

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.msg is not None:
            print "destory msg for topic %s" % self.topic
            DestroyMessage(self.msg)