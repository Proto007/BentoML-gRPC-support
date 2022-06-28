from io import BytesIO
import numpy as np
import io_descriptors_pb2
from google.protobuf.timestamp_pb2 import Timestamp
from google.protobuf.duration_pb2 import Duration
import datetime

supported_datatypes={
    np.dtype('i4'): "sint32_",
    np.dtype('i8'): "sint64_",
    np.dtype('u4'): "uint32_",
    np.dtype('u8'): "uint64_",
    np.dtype('f4'): "float_",
    np.dtype('f8'): "double_",
    np.dtype('?'): "bool_",
    np.unicode_ : "string_",
    np.string_: "bytes_",
    datetime.datetime: "timestamp_",
    datetime.date:"timestamp_",
    np.timedelta64: "duration_",
    np.ndarray: "array_",
    list: "array_",
    np.str_:"string_"
    #@TODO : complex types, bytestring, signed int, lower byte integers(8,16)
}

def is_supported(datatype:type)->str:
    """
    Checks if the given type is within `supported_datatypes` dictionary
    """
    if not datatype:
        return ""
    found_dtype=""
    for key in supported_datatypes.keys():
        if(key==np.dtype(datatype)):
            found_dtype=supported_datatypes[key]
            break
    return found_dtype

def create_tuple_proto(tuple)->io_descriptors_pb2.Tuple:
    """
    Convert given tuple list or tuple array to protobuf
    """
    if len(tuple)==0:
        raise ValueError("Provided tuple is either empty or invalid.")
    tuple_arr=[]
    for item in tuple:
        dtype=is_supported(type(item))
        if(not dtype):
            raise ValueError(f'Invalid datatype "{type(item).__name__}" within tuple.')
        elif(dtype=="timestamp_" and type(item)!=list and type(item)!=datetime.timedelta):
            if(type(item)==datetime.date):
                item=datetime.datetime(item.year, item.month, item.day)
            t=Timestamp()
            t.FromDatetime(item)
            tuple_arr.append(io_descriptors_pb2.Value(**{"timestamp_":t}))
        elif(dtype=="duration_" or type(item)==datetime.timedelta):
            d=Duration()
            d.FromTimedelta(item)
            tuple_arr.append(io_descriptors_pb2.Value(**{"duration_":d}))
        elif(dtype=="array_" or type(item)==list):
            if(not all(isinstance(x,type(item[0]))for x in item)):
                val=create_tuple_proto(item)
                tuple_arr.append(io_descriptors_pb2.Value(tuple_=val))
            else:
                val=arr_to_proto(item)
                tuple_arr.append(io_descriptors_pb2.Value(array_=val))
        else:
            tuple_arr.append(io_descriptors_pb2.Value(**{f"{dtype}":item}))
        
    return io_descriptors_pb2.Tuple(value_=tuple_arr)

def arr_to_proto(arr)->io_descriptors_pb2.NumpyNdarray:
    """
    Convert given array or list to protobuf
    """
    if len(arr)==0:
        raise ValueError("Provided array is either empty or invalid.")
    if(not all(isinstance(x,type(arr[0]))for x in arr)):
        raise ValueError("Entered tuple, expecting array.")
    
    dtype=is_supported(type(arr[0]))
    if not dtype:
        raise ValueError("Dtype is not supported.")
    if(dtype=="timestamp_" and type(arr[0])!=list):
        timestamp_arr=[]
        for dt in arr:
            if(type(dt)==datetime.date):
                dt=datetime.datetime(dt.year, dt.month, dt.day)
            t=Timestamp()
            t.FromDatetime(dt)
            timestamp_arr.append(t)
        return io_descriptors_pb2.NumpyNdarray(dtype="timestamp_",timestamp_=timestamp_arr)
    elif(dtype=="duration_"):
        duration_arr=[]
        for td in arr:
            d=Duration()
            d.FromTimedelta(td)
            duration_arr.append(d)
        return io_descriptors_pb2.NumpyNdarray(dtype="duration_",duration_=timestamp_arr)
    elif(dtype!="array_" and type(arr[0])!=list):
        return io_descriptors_pb2.NumpyNdarray(**{"dtype":dtype,f"{dtype}":arr})
        
    return_arr=[]
    is_tuple=False
    for i in range(len(arr)):
        print(arr[i])
        if(not all(isinstance(x,type(arr[i][0]))for x in arr[i])):
            is_tuple=True
            val=create_tuple_proto(arr[i])
        else:
            val=arr_to_proto(arr[i])
        return_arr.append(val)
    try:
        if(is_tuple):
            return_arr=io_descriptors_pb2.NumpyNdarray(dtype="tuple_",tuple_=return_arr)
        else:
            return_arr=io_descriptors_pb2.NumpyNdarray(dtype="array_",array_=return_arr)
    except:
        raise ValueError("Entered invalid array of inconsistent shape.")
    
    return return_arr

def handle_tuple(proto_tuple:io_descriptors_pb2.Tuple):
    """
    Convert given protobuf tuple to a tuple list
    """
    tuple_arr=[]
    [tuple_arr.append(i) for i in getattr(proto_tuple,"value_")]
    
    if(not tuple_arr):
        raise ValueError("Provided tuple is either empty or invalid.")
    
    return_arr=[]

    for i in range(len(tuple_arr)):
        val=getattr(tuple_arr[i],tuple_arr[i].WhichOneof("dtype"))
        
        if(tuple_arr[i].WhichOneof("dtype")=="timestamp_"):
            val=Timestamp.ToDatetime(val)
        elif(tuple_arr[i].WhichOneof("dtype")=="duration_"):
            val=Duration.ToTimedelta(val)

        if(type(val)==io_descriptors_pb2.NumpyNdarray):
            val=proto_to_arr(val)
        elif(type(val)==io_descriptors_pb2.Tuple):
            val=handle_tuple(val)
        return_arr.append(val)
    
    return return_arr

def proto_to_arr(proto_arr:io_descriptors_pb2.NumpyNdarray):
    """
    Convert given protobuf array to python list
    """
    return_arr=[]
    [return_arr.append(i) for i in getattr(proto_arr,proto_arr.dtype)]
    
    if(proto_arr.dtype=="timestamp_"):
        return_arr=[Timestamp.ToDatetime(dt) for dt in return_arr]
    elif(proto_arr.dtype=="duration_"):
        return_arr=[Duration.ToTimedelta(td) for td in return_arr]
    
    if(not return_arr):
        raise ValueError("Provided array is either empty or invalid")

    for i in range(len(return_arr)):
        if(type(return_arr[i])==io_descriptors_pb2.NumpyNdarray):
            return_arr[i]=proto_to_arr(return_arr[i])
        elif(type(return_arr[i])==io_descriptors_pb2.Tuple):
            return_arr[i]=handle_tuple(return_arr[i])
    return return_arr