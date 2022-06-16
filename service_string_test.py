"""
    Creates a service with Text() input and output types
    This service is used to test grpc server
"""
import numpy as np
import bentoml
from bentoml.io import Text

# Create a class that can be used for runner
class ModelClass:
    def predict(self, inp:str):
        return_str=inp[::-1]
        return return_str

# Create an instance of LdaModelClass
runner_model=ModelClass()

# Save the model as 'runnermodel'
tag=bentoml.picklable_model.save_model('textrunnermodel',runner_model,signatures={"predict":{"batchable":False}})

# Initialize a runner and create a service
text_model_runner = bentoml.picklable_model.get(tag).to_runner()
text_model_runner.init_local()
svc = bentoml.Service("text_model_classifier", runners=[text_model_runner])

# Specify api input as Text and output as Numpy Array
@svc.api(input=Text(), output=Text())
def predict(input_series) -> np.ndarray:
    return text_model_runner.predict.run(input_series)
     