"""
    Creates a service with NumpyNdArray input and output types
    This service is used to test grpc server
"""
import numpy as np
import bentoml
from bentoml.io import NumpyNdarray

# Create a class that can be used for runner
class ModelClass:
    def predict(self, inp:np.ndarray):
        return np.array(inp,dtype=inp.dtype)

# Create an instance of LdaModelClass
runner_model=ModelClass()

# Save the model as 'runnermodel'
tag=bentoml.picklable_model.save_model('numpyrunnermodel',runner_model,signatures={"predict":{"batchable":False}})

# Initialize a runner and create a service
numpy_model_runner = bentoml.picklable_model.get(tag).to_runner()
numpy_model_runner.init_local()
svc = bentoml.Service("numpy_model_classifier", runners=[numpy_model_runner])

# Specify api input as Text and output as Numpy Array
@svc.api(input=NumpyNdarray(), output=NumpyNdarray())
def predict(input_series) -> np.ndarray:
    return numpy_model_runner.predict.run(input_series)
     