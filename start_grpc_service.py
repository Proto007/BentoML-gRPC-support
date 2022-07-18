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
    "python -m grpc_tools.protoc -I./protos --python_out=. protos/payload.proto",
    shell=True,
)

subprocess.run(
    "python -m grpc_tools.protoc -I./protos --python_out=. --grpc_python_out=. protos/service.proto",
    shell=True,
)

subprocess.run(f"python server.py {sys.argv[1]}", shell=True)
