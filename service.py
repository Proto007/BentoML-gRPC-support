import string

import bentoml
import numpy as np
from bentoml.io import NumpyNdarray, Text

import ldaModel

# Load the data_vectorized, vectorizer and lda_model from storage. Throw an error with helpful message if loading fails.
try:
    vectorizer = bentoml.sklearn.load_model("vectorizer:latest")
    lda_model = bentoml.sklearn.load_model("ldamodel:latest")
    number_of_topics = lda_model.get_params()["n_components"]
except:
    raise ValueError(
        "Can't find lda_model or vectorizer in storage. Try running `python trainModel.py` to save a model and vectorizer."
    )

# Create a dataframe that is used for the prediction function
df_topic_keywords = ldaModel.show_topics(vectorizer, lda_model, number_of_topics)


# Create a class that can be used for runner
class LdaModelClass:
    def predict(self, query_description: string):
        return ldaModel.predict(
            str(query_description), vectorizer, lda_model, df_topic_keywords
        )


# Create an instance of LdaModelClass
runner_model = LdaModelClass()

# Save the model as 'runnermodel'
tag = bentoml.picklable_model.save_model(
    "runnermodel", runner_model, signatures={"predict": {"batchable": False}}
)

# Initialize a runner and create a service
lda_model_runner = bentoml.picklable_model.get(tag).to_runner()
lda_model_runner.init_local()
svc = bentoml.Service("lda_model_classifier", runners=[lda_model_runner])

# Specify api input as Text and output as Numpy Array
@svc.api(input=Text(), output=NumpyNdarray())
def predict(input_series) -> np.ndarray:
    topicsArr = lda_model_runner.predict.run(str(input_series))
    return np.array(topicsArr, dtype=object)
