"""
    Author: Sadab Hafiz
    Description: This file contains functions to read and prepare the dataset for the LDA model
"""
#Imports for reading and filtering languages other than English
from turtle import pos
import pandas as pd
from langdetect import detect

#Imports for processing the data
import nltk
from nltk.stem import WordNetLemmatizer 
from nltk.corpus import wordnet
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
#Download Wordnet through NLTK in python console
nltk.download('wordnet')


"""
    Read given .csv dataset into a list of strings where each string serves as a book description
    The .csv file provided must have a column called 'description'
    @params:
        fname (string): name of the .csv file being read
    @return:
        descriptions (string): A list of strings
"""
def read_data(fname):
    if not fname:
        return []
    #Read csv into a dataframe
    book_df=pd.read_csv(fname)

    #Create a list of all descriptions from the dataframe
    descriptions=[]
    for each_description in book_df['description']:
        #Add descriptions greater than length 10
        if(len(str(each_description))>10):
            #Filter out languages other than English
            lang=detect(str(each_description))
            if(lang!='en'):
                continue
            descriptions.append(each_description.strip())
    
    return descriptions

"""
    Returns the pos tag of given word in a format that is recognized by wordnet lemmatizer
    @params:
        word (string): query word
    @return 
        pos_tag (string): pos tag that is recognizable by wordnet lemmatizer/ empty if the given word is empty or not one of the lemmatized pos
"""
def get_pos_tag(word):
    #Return empty string if query word is invalid
    if not word:
        return ""
    #Get the uppercase of first letter of the pos recognized by nltk's pos_tag function
    tag=nltk.pos_tag([word])[0][1][0].upper()

    #Return approprite wordnet pos tag based on the pag recognized by nltk's pos_tag function
    if(tag=="J"):
        return wordnet.ADJ
    elif(tag=="N"):
        return wordnet.NOUN
    elif(tag=="V"):
        return wordnet.VERB
    elif(tag=="R"):
        return wordnet.ADV
    #Return empty if the given word is not one of the above tags
    return ""

"""
    Return lemmatized form of given word if it is a noun, verb, adjective or adverb.
    @params: 
        word (string): query word
    @return 
        lemmatized_word (string): lemmatized word if it is one of the valid pos tags/ returns the word itself if it is empty or not a valid pos tag
"""
def lemmatize(word):
    #Return the word itself if its empty
    if not word:
        return word
    #Initialize the lemmatizer
    lemmatizer=WordNetLemmatizer()
    #Get the pos tag for the word
    pos_tag=get_pos_tag(word)
    #If the pos tag is not valid, return the word itself
    if not pos_tag:
        return word
    #Return the lemmatized form of the word
    return lemmatizer.lemmatize(word,pos_tag)

"""
    Updates passed list of strings into a list of preprocessed strings
    Preprocessing includes tokenizing(using nltk), removing stopwords, lemmatizing, casefolding
    @param 
        description (list of strings): list of descriptions from the dataset
    @result: param list is updated into a list of preprocessed strings
    @return: none
"""
def preprocess(descriptions):
    #Get the stopwords_list from nltk
    stopwords_list=stopwords.words('english')
    #Tokenize, lemmatize, remove stopwords, remove numeric words, remove endline
    for i in range(len(descriptions)):
        #Tokenize the words from each description using nltk's tokenizer
        descriptions[i]=word_tokenize(descriptions[i])
        #Each processed word in current description is appended to this string
        processed_description=""
        for word in descriptions[i]:
            #Remove endline chars and casefold words to lowercase
            word=word.lower().strip()
            #Lemmatize the words
            word=lemmatize(word)
            #Filter out the stopwords
            if word not in stopwords_list:
                #Filter out words that are not greater than length of 2 chars and numeric words
                if((len(word)>2) ):
                    #Add word to the processed description
                    processed_description+=str(word)+" "
        #Update the curr index of descriptions with cleaned list of words
        descriptions[i]=processed_description.strip()

"""
    Preprocess passed description by tokenizing, lemmatizing, stopwords removal, casefolding
    @param:
        description (string): description that will be preprocessed
    @result: passed description is updated into a list of preprocessed strings which are words after preprocessing
    @return: none
"""
def preprocess_single(description):
    #If the provided description is empty, return empty array
    if not description:
        return []
    #Get the stopwords_list from nltk
    stopwords_list=stopwords.words('english')
    #Tokenize the words from each description using nltk's tokenizer
    description=word_tokenize(description)
    #Each processed word in current description is appended to this string
    processed_description=""
    for word in description:
        #Remove endline chars and casefold words to lowercase
        word=word.lower().strip()
        #Lemmatize the word
        word=lemmatize(word)
        #Filter out the stopwords
        if word not in stopwords_list:
            #Filter out words that are not greater than length of 2 chars
            if((len(word)>2) ):
                #Add word to the processed description
                processed_description+=str(word)+" "
    #Update the description with cleaned list of words
    description=processed_description.strip()