import os
import sys

import bentoml

"""
    Load the service
    Verify user entered correct number of cli arguments and valid service name
"""
if len(sys.argv) != 2:
    print("Invalid number of arguments. Please provide a Service.")
    quit()
try:
    svc = bentoml.load(sys.argv[1])
except:
    raise ValueError("Service is invalid. Try again with a valid Service.")

for _, api in svc.apis.items():
    api_func = api.func


def create_service_proto():
    """
    Generate service proto file with the name of the api function
    """

    # Create the protobuf file
    with open("./protos/bentoML.proto", "w") as f:
        f.write(
            f"""
syntax = "proto3";

import "io_descriptors.proto";

message BentoServiceMessage {{
	oneof io_descriptor{{
		string text=1;
        NumpyNdarray array=2;
	}}
}}

message BentoServiceInput {{
	BentoServiceMessage input = 1;
	//string api_name=2;
}}

message BentoServiceOutput {{
	BentoServiceMessage output= 1;
	//status
	//error
}}

service BentoML{{
	rpc {api_func.__name__}(BentoServiceInput) returns (BentoServiceOutput);
}}
"""
        )
    return api_func


def create_server(server_name: str):
    """
    Write grpc server code in a new python file with the specified server_name
    """
    if server_name[-3:] != ".py":
        raise ValueError("File must be a .py file.")
    with open(server_name, "w") as f:
        f.write(
            f"""# Code to create a server using grpc
import grpc
import bentoML_pb2
import bentoML_pb2_grpc
import bentoml
from concurrent import futures
from bentoml.io import NumpyNdarray,Text
import numpy as np

from grpc_service_util import arr_to_proto,proto_to_arr

svc=bentoml.load("{sys.argv[1]}")

for name,api in svc.apis.items():    
    # Create a server and add the lda_model to the server
    svc_func=api.func
    input_type=api.input
    output_type=api.output

type_dict={{
    "text" : Text ,
    "array" : NumpyNdarray 
}}

class BentoMLServicer(bentoML_pb2_grpc.BentoMLServicer):
    def {api_func.__name__}(self, request, context):
        input= request.input

        io_type=input.WhichOneof("io_descriptor")

        if(type_dict[io_type]!=type(input_type)):
            raise ValueError(f"Please provide a {{type(input_type).__name__}}.")
    
        if(io_type=="text"):
            input=input.text
        elif(io_type=="array"):
            input=input.array
            input=np.array(proto_to_arr(input))
        else:
            raise ValueError("Invalid input type.")

        output=svc_func(input)

        if(type(output_type)==Text):
            output=bentoML_pb2.BentoServiceMessage(text=output)
        elif(type(output_type)==NumpyNdarray):
            out=arr_to_proto(output)
            output=bentoML_pb2.BentoServiceMessage(array=out)
        return bentoML_pb2.BentoServiceOutput(output=output)

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


create_service_proto()
os.system(
    "python -m grpc_tools.protoc -I./protos --python_out=. protos/io_descriptors.proto"
)
os.system(
    "python -m grpc_tools.protoc -I./protos --python_out=. --grpc_python_out=. protos/bentoML.proto"
)
create_server("server.py")
os.system("python server.py")
