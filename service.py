import string
import numpy as np
import bentoml
from requests import request
import ldaModel
from bentoml.io import NumpyNdarray, Text

import lda_model_pb2_grpc
import lda_model_pb2

# Load the data_vectorized, vectorizer and lda_model from storage. Throw an error with helpful message if loading fails.
try:
    vectorizer=bentoml.sklearn.load_model("vectorizer:latest")
    lda_model=bentoml.sklearn.load_model("ldamodel:latest")
    number_of_topics=lda_model.get_params()["n_components"]
except:
    raise ValueError("Can't find lda_model or vectorizer in storage. Try running `python trainModel.py` to save a model and vectorizer.")

# Create a dataframe that is used for the prediction function
df_topic_keywords = ldaModel.show_topics(vectorizer, lda_model,number_of_topics)

class PredictLdaServicer(lda_model_pb2_grpc.PredictLdaServicer):
    def predict(self, request, context):
        # Create a dataframe that is used for the prediction function
        df_topic_keywords = ldaModel.show_topics(vectorizer, lda_model,number_of_topics)
        
        # Call the predict method to get topic distribution for request query 
        lda_out=ldaModel.predict(request.query_description, vectorizer, lda_model, df_topic_keywords)
        
        # Create a list to append SingleTopic protobuf objects
        all_topics_list=[]
        
        # Create a SingleTopic protobuf object for all topic tuples in lda_out and append the SingleTopic to all_topics_list
        for topic in lda_out:
            single_topic=lda_model_pb2.SingleTopic(keywords=topic[0],correlation=topic[1])
            all_topics_list.append(single_topic)
        
        # Create AllTopics protobuf object that will be assigned to the response `topic_distribution`
        all_topics=lda_model_pb2.AllTopics(topics=all_topics_list)

        # Create and return a response using the AllTopics object created earlier
        response=lda_model_pb2.PredictTopicResponse(topic_distribution=all_topics)
        return response

# Create an instance of LdaModelClass
runner_model=PredictLdaServicer()
# Save the model as 'runnermodel'
tag=bentoml.picklable_model.save_model('runnermodel',runner_model,signatures={"predict":{"batchable":False}})

# Initialize a runner and create a service
lda_model_runner = bentoml.picklable_model.get(tag).to_runner()
lda_model_runner.init_local()
svc = bentoml.Service("lda_model_classifier", runners=[lda_model_runner])

# Specify api input as Text and output as Numpy Array
@svc.api(input=Text(), output=NumpyNdarray())
def predict(input_series) -> np.ndarray:
    proto_request=lda_model_pb2.PredictTopicRequest(query_description=input_series)
    topicsArr=lda_model_runner.predict.run(proto_request)
    return np.array(topicsArr)
