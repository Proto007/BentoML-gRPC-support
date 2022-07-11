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
    with open("./protos/bentoml_service.proto", "w") as f:
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
	//TODO string api_name=2;
}}

message BentoServiceOutput {{
	BentoServiceMessage output= 1;
	//TODO status, error
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
import bentoml_service_pb2
import bentoml_service_pb2_grpc
import bentoml
from concurrent import futures
from bentoml.io import NumpyNdarray,Text
import numpy as np
import logging
import asyncio

from grpc_service_util import arr_to_proto, proto_to_arr

svc=bentoml.load("{sys.argv[1]}")

for runner in svc.runners:
    runner.init_local()

for name,api in svc.apis.items():    
    # Create a server and add the lda_model to the server
    svc_func=api.func
    input_type=api.input
    output_type=api.output

type_dict={{
    "text" : Text ,
    "array" : NumpyNdarray 
}}

class BentoMLServicer(bentoml_service_pb2_grpc.BentoMLServicer):
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
            output=bentoml_service_pb2.BentoServiceMessage(text=output)
        elif(type(output_type)==NumpyNdarray):
            out=arr_to_proto(output)
            output=bentoml_service_pb2.BentoServiceMessage(array=out)
        return bentoml_service_pb2.BentoServiceOutput(output=output)

_cleanup_coroutines = []
async def serve() -> None:
    server = grpc.aio.server()
    bentoml_service_pb2_grpc.add_BentoMLServicer_to_server(BentoMLServicer(),server)
    listen_addr = '[::]:50051'
    server.add_insecure_port(listen_addr)
    logging.info("Starting server on %s", listen_addr)
    await server.start()
    async def server_graceful_shutdown():
        logging.info("Starting graceful shutdown...")
        # Shuts down the server with 5 seconds of grace period. During the
        # grace period, the server won't accept new connections and allow
        # existing RPCs to continue within the grace period.
        await server.stop(5)
    _cleanup_coroutines.append(server_graceful_shutdown())
    await server.wait_for_termination()

logging.basicConfig(level=logging.INFO)
loop = asyncio.get_event_loop()
try:
    loop.run_until_complete(serve())
finally:
    loop.run_until_complete(*_cleanup_coroutines)
    loop.close()
"""
        )

create_service_proto()

# import pkg_resources
# from grpc_tools import protoc
# proto_include = pkg_resources.resource_filename('grpc_tools', '_proto')
# curr_dir = os.getcwd()
# protoc.main([
#     'grpc_tools.protoc',
#     f'-I{proto_include}',
#     '--proto_path=./protos',
#     '--python_out=./',
#     'protos/io_descriptors.proto'
# ])

# protoc.main([
#     'grpc_tools.protoc',
#     f'-I{proto_include}',
#     '--proto_path=./protos',
#     '--python_out=./',
#     '--grpc_python_out=./',
#     'protos/bentoml_service.proto'
# ])

import subprocess

subprocess.run(
    "python -m grpc_tools.protoc -I./protos --python_out=. protos/io_descriptors.proto",
    shell = True
)

subprocess.run(
    "python -m grpc_tools.protoc -I./protos --python_out=. --grpc_python_out=. protos/bentoml_service.proto",
    shell = True
)

create_server("server.py")

subprocess.run(
    "python server.py",
    shell=True
)
