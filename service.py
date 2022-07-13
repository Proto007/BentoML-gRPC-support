import bentoml
import numpy as np
from bentoml.io import NumpyNdarray, Text

try:
    lda_model=bentoml.models.get("runnermodel:latest")
except:
    raise ValueError("Can't find lda_model or vectorizer in storage. Try running `python trainModel.py` to save a model and vectorizer.")

# Initialize a runner and create a service
lda_model_runner = lda_model.to_runner()
svc = bentoml.Service("lda_model_classifier", runners=[lda_model_runner])

# Specify api input as Text and output as Numpy Array
@svc.api(input=Text(), output=NumpyNdarray())
def predict(input_series) -> np.ndarray:
    topicsArr = lda_model_runner.predict.run(str(input_series))
    return np.array(topicsArr, dtype=object)

# Specify api input as Numpy Array and output as Numpy Array
@svc.api(input=NumpyNdarray(), output=NumpyNdarray())
def numpy_test(input_series) -> np.ndarray:
    return np.array(input_series,dtype=object)