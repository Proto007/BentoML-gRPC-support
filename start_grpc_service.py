import bentoml
import os
import sys
import json

import numpy as np

"""
    Load the service
    Verify user entered correct number of cli arguments and valid service name
"""
if(len(sys.argv)!=2):
    print("Invalid number of arguments. Please provide a Service.")
    quit()
try:
    svc=bentoml.load(sys.argv[1])
except:
    raise ValueError("Service is invalid. Try again with a valid Service.")

"""
    Functions to generate protobuf content for specific datatypes
"""
def get_proto_text(input_type):
    return f'string text_{input_type} = 1;', f'string text_{input_type}'

def get_proto_numpyArr(input_type):
    return f'string numpyArr_{input_type} = 1;', f"numpyArr{input_type}"

