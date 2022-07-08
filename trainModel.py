"""
    Author: Sadab Hafiz
    Description: This file needs to be executed once to train and save the model locally.
"""
# Import necessary modules
import bentoml

import ldaModel as lda

"""
    Creating an LDA model
"""
data_vectorized, vectorizer = lda.get_data_vectorized("books.csv", 10, 0.8)
lda_model = lda.create_lda_model(data_vectorized, 15)
lda.visualize_lda_model(lda_model, data_vectorized, vectorizer)
df_topic_keywords = lda.show_topics(vectorizer, lda_model, 15)

"""
    Saving the data_vectorized, vectorizer and lda_model in disk for faster lodaing
"""
bentoml.sklearn.save_model("data_vectorized", data_vectorized)
bentoml.sklearn.save_model("vectorizer", vectorizer)
bentoml.sklearn.save_model("ldamodel", lda_model)
"""
    Example code showing how to predict the probability of given text using the LDA model
"""
# probability_scores=lda.predict("As a third-year Ph.D. candidate, Olive Smith doesn't believe in lasting romantic relationships--but her best friend does, and that's what got her into this situation. Convincing Anh that Olive is dating and well on her way to a happily ever after was always going to take more than hand-wavy Jedi mind tricks: Scientists require proof. So, like any self-respecting biologist, Olive panics and kisses the first man she sees. That man is none other than Adam Carlsen, a young hotshot professor--and well-known ass. Which is why Olive is positively floored when Stanford's reigning lab tyrant agrees to keep her charade a secret and be her fake boyfriend. But when a big science conference goes haywire, putting Olive's career on the Bunsen burner, Adam surprises her again with his unyielding support and even more unyielding... six-pack abs. Suddenly their little experiment feels dangerously close to combustion. And Olive discovers that the only thing more complicated than a hypothesis on love is putting her own heart under the microscope.",vectorizer,lda_model,df_topic_keywords)
# print(probability_scores)
