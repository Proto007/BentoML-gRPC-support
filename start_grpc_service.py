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
import bentoml
import json
from concurrent import futures
from io import BytesIO
from bentoml.io import NumpyNdarray,Text

svc=bentoml.load("{sys.argv[1]}")

for name,api in svc.apis.items():    
    # Create a server and add the lda_model to the server
    svc_func=api.func
    input_type=api.input
    output_type=api.output

def array_to_bytes(arr):
    to_bytes=BytesIO()
    np.save(to_bytes,arr,allow_pickle=True)
    return to_bytes.getvalue()

def bytes_to_array(bytes_arr):
    load_bytes=BytesIO(bytes_arr)
    loaded_arr=np.load(load_bytes,allow_pickle=True)
    return loaded_arr

supported_datatypes={{
    np.double:"double",
    np.float32: "float",
    np.int32: "int32",
    np.int64: "int64",
    np.uint32: "uint32",
    np.uint64: "uint64",
    np.int_:"int32",
    np.bool_:"bool",
    np.byte:"bytes",
    np.str_:"string",
    np.string_:"string"
}}

class BentoMLServicer(bentoML_pb2_grpc.BentoMLServicer):
    def api_func(self, request, context):
        input= request.input

        #@TODO: Code that deals with the shape of the array, structured types

        io_type=input.WhichOneof("io_descriptor")
        if(io_type=="text"):
            input=input.text
        elif(io_type=="numpy_ndarray"):
            input=input.numpy_ndarray
            input=getattr(input,str(input.array_type)+"_array")
        else:
            raise ValueError("Invalid input type.")

        output=svc_func(input)

        if(type(output_type)==Text):
            output=bentoML_pb2.BentoServiceMessage(text=output)
        elif(type(output_type)==NumpyNdarray):
            found_dtype=""
            for key in supported_datatypes.keys():
                if(key==output.dtype):
                    found_dtype=key
                    break
            if(not found_dtype):
                out=[json.dumps(item.tolist()) for item in output]
                output=bentoML_pb2.NumpyNdArray(array_type="string_array",string_array=out)
                output=bentoML_pb2.BentoServiceMessage(numpy_ndarray=output)
            else:
                out=bentoML_pb2.NumpyNdArray(**{{"array_type":str(output.dtype),str(output.dtype)+"_array":output}})
                #get
                #setattr(out,str(out.array_type)+"_array",output)
                #out.str(out.array_type)+"_array".extend(output)
                output=bentoML_pb2.BentoServiceMessage(numpy_ndarray=out)

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


os.system("python -m grpc_tools.protoc -I./protos --python_out=. --grpc_python_out=. protos/bentoML.proto")
create_server("lda_server.py")
os.system("python lda_server.py")   
