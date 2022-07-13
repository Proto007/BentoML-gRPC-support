# Code to create a server using grpc
import sys
import grpc
import bentoml_service_pb2
import bentoml_service_pb2_grpc
import bentoml
from bentoml.io import NumpyNdarray,Text
import numpy as np
import logging
import asyncio

from grpc_service_util import arr_to_proto, proto_to_arr

svc=bentoml.load(sys.argv[1])

for runner in svc.runners:
    runner.init_local()

type_dict={
    "text" : Text ,
    "array" : NumpyNdarray 
}

class BentoMLServicer(bentoml_service_pb2_grpc.BentoMLServicer):
    
    def call(self, request, context):
        try:
            api=svc.apis[request.api_name]
        except KeyError:
            raise ValueError(f'Provided api_name "{request.api_name}" is not defined in service."')
        input= request.input

        io_type=input.WhichOneof("io_descriptor")

        if(type_dict[io_type] != type(api.input)):
            raise ValueError(f"Please provide a {type(api.input).__name__}.")
    
        if(io_type == "text"):
            input = input.text
        elif(io_type == "array"):
            input=input.array
            input = np.array(proto_to_arr(input))
        else:
            raise ValueError("Invalid input type.")

        output = api.func(input)

        if(type(api.output) == Text):
            output=bentoml_service_pb2.BentoServiceMessage(text=output)
        elif(type(api.output) == NumpyNdarray):
            out=arr_to_proto(output)
            output=bentoml_service_pb2.BentoServiceMessage(array = out)
        return bentoml_service_pb2.BentoServiceOutput(output = output)

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
