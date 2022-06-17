import bentoml
import os
import sys
import json
from bentoml.io import NumpyNdarray,Text
import numpy as np

"""
    Load the service
    Verify user entered correct number of cli arguments and valid service name
"""
if(len(sys.argv)!=2):
    print("Invalid number of arguments. Please provide a Service.")
    quit()
try:
    svc=bentoml.load(sys.argv[1])
except:
    raise ValueError("Service is invalid. Try again with a valid Service.")

"""
    Functions to generate protobuf content for specific datatypes
"""
def get_proto_text(input_type):
    return f"message {input_type}{{\n\tstring text_{input_type} = 1;\n}}\n\n",f'text_{input_type}'

def get_proto_numpyArr(input_type):
    return f'bytes numpyArr_{input_type} = 1;', f"numpyArr_{input_type}"

func_dict={
    Text: get_proto_text,
    NumpyNdarray: get_proto_numpyArr
}


def generate_protobuf(input_type,output_type):
    """
    Generates protobuf based on given input_type, output_type and function name
    """
    # Verify input_type and output_type are correct
    if(type(input_type) not in func_dict) or (type(output_type) not in func_dict):
        if(type(input_type) not in func_dict):
            print("Input is not a valid type")
        if(type(output_type) not in func_dict):
            print("Output is not a valid type")
        raise ValueError("Invalid input types.")

    # Map the input and output to the appropriate functions
    input_proto, input_name=func_dict[type(input_type)]("Input")
    output_proto, output_name=func_dict[type(output_type)]("Output")

    #Create the protobuf file
    with open('./protos/bentoML.proto','w') as f:
        f.write(
            'syntax = "proto3";\n\n'
            f"{input_proto}"
            f"{output_proto}"
            "service BentoML{\n"
                f"\trpc api_func(Input) returns (Output);\n"
            "}"
        )
    return input_name, output_name

for name,api in svc.apis.items():    
    # Create a server and add the lda_model to the server
    i,o=generate_protobuf(api.input,api.output)
    svc_func=api.func

def create_server(server_name:str):
    if server_name[-3:]!=".py":
        raise ValueError("File must be a .py file.")
    with open(server_name,"w") as f:
        f.write(
f"""
import grpc
import bentoML_pb2
import bentoML_pb2_grpc
import numpy as np
import json
import bentoml
from concurrent import futures
from io import BytesIO

svc=bentoml.load("{sys.argv[1]}")

for name,api in svc.apis.items():    
    # Create a server and add the lda_model to the server
    svc_func=api.func

def array_to_bytes(arr):
    to_bytes=BytesIO()
    np.save(to_bytes,arr,allow_pickle=True)
    return to_bytes.getvalue()

def bytes_to_array(bytes_arr):
    load_bytes=BytesIO(bytes_arr)
    loaded_arr=np.load(load_bytes,allow_pickle=True)
    return loaded_arr

class BentoMLServicer(bentoML_pb2_grpc.BentoMLServicer):
    def api_func(self, request, context):
        input= request.{i}
        if "numpyArr" in "{i}":
            input=bytes_to_array(input)
        output=svc_func(input)
        if "numpyArr" in "{o}":
            output=array_to_bytes(output)
        return bentoML_pb2.Output({o}=output)

# Create a server and add the lda_model to the server
server=grpc.server(futures.ThreadPoolExecutor(max_workers=10))
bentoML_pb2_grpc.add_BentoMLServicer_to_server(BentoMLServicer(),server)
server.add_insecure_port('[::]:8000')

# Start server and keep the server running until terminated\n
server.start()
print("Starting server...")
print("Listening on port 8000...")
server.wait_for_termination()
"""
        )


os.system("python -m grpc_tools.protoc -I./protos --python_out=. --grpc_python_out=. protos/bentoML.proto")
create_server("numpy_server.py")
os.system("python numpy_server.py")   
