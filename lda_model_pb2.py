# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: lda_model.proto
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x0flda_model.proto\"4\n\x0bSingleTopic\x12\x10\n\x08keywords\x18\x01 \x03(\t\x12\x13\n\x0b\x63orrelation\x18\x02 \x01(\x02\")\n\tAllTopics\x12\x1c\n\x06topics\x18\x01 \x03(\x0b\x32\x0c.SingleTopic\"0\n\x13PredictTopicRequest\x12\x19\n\x11query_description\x18\x01 \x01(\t\">\n\x14PredictTopicResponse\x12&\n\x12topic_distribution\x18\x01 \x01(\x0b\x32\n.AllTopics2D\n\nPredictLda\x12\x36\n\x07predict\x12\x14.PredictTopicRequest\x1a\x15.PredictTopicResponseb\x06proto3')



_SINGLETOPIC = DESCRIPTOR.message_types_by_name['SingleTopic']
_ALLTOPICS = DESCRIPTOR.message_types_by_name['AllTopics']
_PREDICTTOPICREQUEST = DESCRIPTOR.message_types_by_name['PredictTopicRequest']
_PREDICTTOPICRESPONSE = DESCRIPTOR.message_types_by_name['PredictTopicResponse']
SingleTopic = _reflection.GeneratedProtocolMessageType('SingleTopic', (_message.Message,), {
  'DESCRIPTOR' : _SINGLETOPIC,
  '__module__' : 'lda_model_pb2'
  # @@protoc_insertion_point(class_scope:SingleTopic)
  })
_sym_db.RegisterMessage(SingleTopic)

AllTopics = _reflection.GeneratedProtocolMessageType('AllTopics', (_message.Message,), {
  'DESCRIPTOR' : _ALLTOPICS,
  '__module__' : 'lda_model_pb2'
  # @@protoc_insertion_point(class_scope:AllTopics)
  })
_sym_db.RegisterMessage(AllTopics)

PredictTopicRequest = _reflection.GeneratedProtocolMessageType('PredictTopicRequest', (_message.Message,), {
  'DESCRIPTOR' : _PREDICTTOPICREQUEST,
  '__module__' : 'lda_model_pb2'
  # @@protoc_insertion_point(class_scope:PredictTopicRequest)
  })
_sym_db.RegisterMessage(PredictTopicRequest)

PredictTopicResponse = _reflection.GeneratedProtocolMessageType('PredictTopicResponse', (_message.Message,), {
  'DESCRIPTOR' : _PREDICTTOPICRESPONSE,
  '__module__' : 'lda_model_pb2'
  # @@protoc_insertion_point(class_scope:PredictTopicResponse)
  })
_sym_db.RegisterMessage(PredictTopicResponse)

_PREDICTLDA = DESCRIPTOR.services_by_name['PredictLda']
if _descriptor._USE_C_DESCRIPTORS == False:

  DESCRIPTOR._options = None
  _SINGLETOPIC._serialized_start=19
  _SINGLETOPIC._serialized_end=71
  _ALLTOPICS._serialized_start=73
  _ALLTOPICS._serialized_end=114
  _PREDICTTOPICREQUEST._serialized_start=116
  _PREDICTTOPICREQUEST._serialized_end=164
  _PREDICTTOPICRESPONSE._serialized_start=166
  _PREDICTTOPICRESPONSE._serialized_end=228
  _PREDICTLDA._serialized_start=230
  _PREDICTLDA._serialized_end=298
# @@protoc_insertion_point(module_scope)
