# Import necessary modules
import grpc
from concurrent import futures
# Import generated files
import lda_model_pb2
import lda_model_pb2_grpc
# Import LDA model modules
import ldaModel
import bentoml

class PredictLdaServicer(lda_model_pb2_grpc.PredictLdaServicer):
    def predict(self, request, context):
        # Load the models from storage. If not available, throw exception with a helpful message
        try:
            vectorizer=bentoml.sklearn.load_model("vectorizer:latest")
            lda_model=bentoml.sklearn.load_model("ldamodel:latest")
            number_of_topics=lda_model.get_params()["n_components"]
        except:
            raise ValueError("Can't find lda_model or vectorizer in storage. Try running `python trainModel.py` to save a model and vectorizer.")
        
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

# Create a server and add the lda_model to the server
server=grpc.server(futures.ThreadPoolExecutor(max_workers=10))
lda_model_pb2_grpc.add_PredictLdaServicer_to_server(PredictLdaServicer(),server)
server.add_insecure_port('[::]:8000')

# Start server and keep the server running until terminated
server.start()
print("Starting server...")
print("Listening on port 8000...")
server.wait_for_termination()