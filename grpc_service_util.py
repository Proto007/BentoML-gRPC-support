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

# TODO: Add support for datetime and timedelta
def create_tuple_proto(tuple):
    if len(tuple)==0:
        raise ValueError("Provided tuple is either empty or invalid.")
    tuple_arr=[]
    for item in tuple:
        dtype=is_supported(type(item))
        if(dtype and dtype!="array_"):
            tuple_arr.append(io_descriptors_pb2.Value(**{f"{dtype}":item}))
        elif(dtype=="array_"):
            if(not all(isinstance(x,type(item[0]))for x in item)):
                val=create_tuple_proto(item)
                tuple_arr.append(io_descriptors_pb2.Value(tuple_=val))
            else:
                val=arr_to_proto(item)
                tuple_arr.append(io_descriptors_pb2.Value(array_=val))
        else:
            raise ValueError(f'Invalid datatype "{type(item).__name__}" within tuple.')
        
    return io_descriptors_pb2.Tuple(value_=tuple_arr)

def arr_to_proto(arr):
    if len(arr)==0:
        raise ValueError("Provided array is either empty or invalid.")
    if(not all(isinstance(x,type(arr[0]))for x in arr)):
        raise ValueError("Entered tuple, expecting array.")
    
    dtype=is_supported(type(arr[0]))
    if(dtype and dtype!="array_"):
        return io_descriptors_pb2.Array(**{"dtype":dtype,f"{dtype}":arr})

    return_arr=[]
    is_tuple=False
    for i in range(len(arr)):
        if(not all(isinstance(x,type(arr[i][0]))for x in arr[i])):
            is_tuple=True
            val=create_tuple_proto(arr[i])
        else:
            val=arr_to_proto(arr[i])
        return_arr.append(val)
    try:
        if(is_tuple):
            return_arr=io_descriptors_pb2.Array(dtype="tuple_",tuple_=return_arr)
        else:
            return_arr=io_descriptors_pb2.Array(dtype="array_",array_=return_arr)
    except:
        raise ValueError("Entered invalid array of inconsistent shape.")
    
    return return_arr

def handle_tuple(proto_tuple):
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

        if(type(val)==io_descriptors_pb2.Array):
            val=proto_to_arr(val)
        elif(type(val)==io_descriptors_pb2.Tuple):
            val=handle_tuple(val)
        return_arr.append(val)
    
    return return_arr

def proto_to_arr(proto_arr):
    return_arr=[]
    [return_arr.append(i) for i in getattr(proto_arr,proto_arr.dtype)]
    
    if(proto_arr.dtype=="timestamp_"):
        return_arr=[Timestamp.ToDatetime(dt) for dt in return_arr]
    elif(proto_arr.dtype=="duration_"):
        return_arr=[Duration.ToTimedelta(td) for td in return_arr]
    
    if(not return_arr):
        raise ValueError("Provided array is either empty or invalid")

    for i in range(len(return_arr)):
        if(type(return_arr[i])==io_descriptors_pb2.Array):
            return_arr[i]=proto_to_arr(return_arr[i])
        elif(type(return_arr[i])==io_descriptors_pb2.Tuple):
            return_arr[i]=handle_tuple(return_arr[i])
    return return_arr

def array_to_bytes(arr):
    to_bytes=BytesIO()
    np.save(to_bytes,arr,allow_pickle=True)
    return to_bytes.getvalue()

def bytes_to_array(bytes_arr):
    load_bytes=BytesIO(bytes_arr)
    loaded_arr=np.load(load_bytes,allow_pickle=True)
    return loaded_arr