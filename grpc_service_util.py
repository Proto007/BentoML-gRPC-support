from io import BytesIO
import numpy as np
import io_descriptors_pb2
from google.protobuf.timestamp_pb2 import Timestamp
from google.protobuf.duration_pb2 import Duration

supported_datatypes={
    np.dtype('i4'): "i4",
    np.dtype('i8'): "i8",
    np.dtype('u4'): "u4",
    np.dtype('u8'): "u8",
    np.dtype('f4'): "f4",
    np.dtype('f8'): "f8",
    np.dtype('?'): "bool_",
    np.unicode_ : "string_",
    np.string_: "bytes_",
    np.datetime64: "timestamp_",
    np.timedelta64: "duration_",
    np.ndarray: "array_",
    list: "array_",
    np.str_:"string_"
    #@TODO : complex types, bytestring, signed int, lower byte integers(8,16)
}

def is_supported(datatype):
    if not datatype:
        return ""
    found_dtype=""
    for key in supported_datatypes.keys():
        if(key==np.dtype(datatype)):
            found_dtype=supported_datatypes[key]
            break
    return found_dtype