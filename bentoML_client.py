# Import necessary modules
import grpc
from google.protobuf.json_format import MessageToDict
import numpy as np
from io import BytesIO

# Import generated files
import bentoML_pb2
import bentoML_pb2_grpc

# # Define a channel with the backend server address
channel=grpc.insecure_channel('localhost:8000')
stub=bentoML_pb2_grpc.BentoMLStub(channel)

# """
#     Testing String
# """
# string_data="badaS ma I ,iH"

# data=bentoML_pb2.Input(text_Input=string_data)

# response=stub.api_func(data)

# print(response.text_Output)

"""
    Testing Numpy
"""
def array_to_bytes(arr):
    to_bytes=BytesIO()
    np.save(to_bytes,arr,allow_pickle=True)
    return to_bytes.getvalue()

def bytes_to_array(bytes_arr):
    load_bytes=BytesIO(bytes_arr)
    loaded_arr=np.load(load_bytes,allow_pickle=True)
    return loaded_arr

arr=np.array([[[1,2,3,4],
              [5,6,7,8],
              [9,10,11,12]],
            [[13,14,15,16],
             [17,18,19,20],
             [21,22,23,24]]],dtype=np.int16
             )

arr1=np.array([[[1,2],"a"],[[2,3],"b"],[[3,4],"c"]],dtype=object)

data=bentoML_pb2.Input(numpyArr_Input=array_to_bytes(arr))

# # Call the predict function
response=stub.api_func(data)

print(bytes_to_array(response.numpyArr_Output))

