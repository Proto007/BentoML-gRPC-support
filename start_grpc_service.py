from concurrent import futures
import bentoml
import grpc
import lda_model_pb2_grpc
import sys

if(len(sys.argv)!=2):
    print("Invalid number of arguments. Please provide a Service.")
    quit()
try:
    svc=bentoml.load(sys.argv[1])
except:
    raise ValueError("Service is invalid. Try again with a valid Service.")

class GrpcRunner:
    pass

for name,api in svc.apis.items():    
    # Create a server and add the lda_model to the server
    server=grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    def apiFunc(self,*args,**kwargs): 
        api.func(*args,**kwargs)
    setattr(GrpcRunner,name,apiFunc)
    
lda_model_pb2_grpc.add_PredictLdaServicer_to_server(GrpcRunner,server)
server.add_insecure_port('[::]:8000')
# Start server and keep the server running until terminated
server.start()
print("Starting server...")
print("Listening on port 8000...")
server.wait_for_termination()

# Generate a proto file based on the runners
# Generate code from the proto file
# Generate server and client code