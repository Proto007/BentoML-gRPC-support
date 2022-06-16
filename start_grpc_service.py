import bentoml
import os
import sys
import json

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
    return f'string text_{input_type} = 1;', f'string text_{input_type}'

def get_proto_numpyArr(input_type):
    return f'string numpyArr_{input_type} = 1;', f"numpyArr_{input_type}"

func_dict={
    "Text()": get_proto_text,
    "NumpyNdarray()": get_proto_numpyArr
}


def generate_protobuf(input_type,output_type):
    """
    Generates protobuf based on given input_type, output_type and function name
    """
    # Verify input_type and output_type are correct
    if(input_type not in func_dict) or (output_type not in func_dict):
        if(input_type not in func_dict):
            print(f"${input_type} is not a valid type")
        if(output_type not in func_dict):
            print(f"${output_type} is not a valid type")
        raise ValueError("Invalid input types.")

    # Map the input and output to the appropriate functions
    input_proto, input_name=func_dict[input_type]("Input")
    output_proto, output_name=func_dict[output_type]("Output")

    #Create the protobuf file
    with open('./protos/bentoML.proto','w') as f:
        f.write(
            'syntax = "proto3";\n\n'
            
            "message Input{\n"
                f"\t{input_proto}\n"
            "}\n\n"
            "message Output{\n"
                f"\t{output_proto}\n"
            "}\n\n"
            "service BentoML{\n"
                f"\trpc api_func(Input) returns (Output);\n"
            "}"
        )
    return input_name, output_name

for name,api in svc.apis.items():    
    # Create a server and add the lda_model to the server
    i,o=generate_protobuf(api.input._init_str,api.output._init_str)
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

svc=bentoml.load("{sys.argv[1]}")

for name,api in svc.apis.items():    
    # Create a server and add the lda_model to the server
    svc_func=api.func

class BentoMLServicer(bentoML_pb2_grpc.BentoMLServicer):
    def api_func(self, request, context):
        input_arr=request.{i}
        input_arr=eval(f"np.array({{input_arr}})")
        out=svc_func(input_arr)
        out_json=json.dumps(out.tolist())
        return bentoML_pb2.Output({o}=out_json)
        
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
"""
 Potential Libraries (numproto: depracated/not popular, numpy-protobuf(not finished))
 Using repeated Any, doesn't work with error "must be instance of same class"
 Using bytesIO
 Json Dump
"""
# dtype is provided and shape is provided