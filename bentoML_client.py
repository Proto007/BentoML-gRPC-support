# Import necessary modules
import datetime

import grpc
import numpy as np

import service_pb2
# Import generated files
import service_pb2_grpc
from grpc_service_util import arr_to_proto, proto_to_arr
from service import text_test

# Define a channel with the backend server address
channel = grpc.insecure_channel("localhost:3000")
stub = service_pb2_grpc.BentoServiceStub(channel)

"""
    Testing LDA Model
"""
query_description = "If happy ever after did exist, I was to be holding you like this."


def test_lda(query_description=query_description):
    query_description = service_pb2.RouteCallRequest(
        api_name="predict", input=service_pb2.Payload(text=query_description)
    )
    response = stub.RouteCall(query_description)
    return response.output.array


print("#####################################LDA####################################")
print(test_lda())
print(proto_to_arr(test_lda()))

"""
    Testing Text
"""
string_data = "badaS ma I ,iH"


def test_text(query_data=string_data):
    query_data = service_pb2.RouteCallRequest(
        api_name="text_test", input=service_pb2.Payload(text=query_data)
    )
    response = stub.RouteCall(query_data)
    return response.output.text


print("#####################################TEXT####################################")
print(test_text())

"""
    Testing Numpy
"""
# Test arrays
arr1 = [1, 2, 3, 4, 5]
arr1_numpy = np.array(arr1)
a1 = [arr1, arr1_numpy]

arr2 = [[1, 2], [3, 4], [5, 6]]
arr2_numpy = np.array(arr2)
a2 = [arr2, arr2_numpy]

arr3 = [[1, "a"], [2, "b"]]
arr3_numpy = np.array([(1, "a"), (2, "b")], dtype=[("num", "i8"), ("char", "U10")])
a3 = [arr3, arr3_numpy]

arr4 = [[["a", "b"], 0.1], [["A", "B"], 0.2]]
arr4_numpy = np.array(arr4, dtype=object)
a4 = [arr4, arr4_numpy]

arr5 = [[[[1, 2], "b"], 0.1], [[[3, 4], "B"], 0.2]]
arr5_numpy = np.array(arr5, dtype=object)
a5 = [arr5, arr5_numpy]

arr6_numpy = np.arange("2005-02-01", "2005-03-02", dtype="datetime64[h]")
arr6 = arr6_numpy.tolist()
a6 = [arr6, arr6_numpy]

arr7 = [
    [[[datetime.datetime(2020, 5, 17), datetime.datetime(2020, 5, 18)], "b"], 0.1],
    [[[datetime.datetime(2020, 5, 19), datetime.datetime(2020, 5, 20)], "B"], 0.2],
]
arr7_numpy = np.array(arr7, dtype=object)
a7 = [arr7, arr7_numpy]

arr8 = [
    [
        [[datetime.datetime(2020, 5, 17), datetime.datetime(2020, 5, 18)], "A"],
        datetime.timedelta(days=3, minutes=10),
    ],
    [
        [[datetime.datetime(2020, 5, 19), datetime.datetime(2020, 5, 20)], "B"],
        datetime.timedelta(days=4, minutes=30),
    ],
]
arr8_numpy = np.array(arr8, dtype=object)
a8 = [arr8, arr8_numpy]

query_arr = a8


def test_numpy(arr=query_arr[0]):
    data = service_pb2.RouteCallRequest(
        api_name="numpy_test", input=service_pb2.Payload(array=arr_to_proto(arr))
    )
    response = stub.RouteCall(data)
    return response.output.array


print("#####################################ARRAY####################################")
print(test_numpy())
print(proto_to_arr(test_numpy()))

"""
Pandas
"""
import pandas as pd

from grpc_service_util import df_to_proto, proto_to_df

# def df_to_proto(df):
#     index = arr_to_proto([row for row in df.index])
#     columns = arr_to_proto([col for col in df.columns])
#     data = arr_to_proto(df.to_numpy(dtype=df.dtypes))
#     return io_descriptors_pb2.PandasDataframe(index=index,columns=columns,data=data)

# def proto_to_df(proto_df):
#     index = proto_to_arr(proto_df.index)
#     columns = proto_to_arr(proto_df.columns)
#     data = proto_to_arr(proto_df.data)
#     return pd.DataFrame(data, index=index, columns=columns)


# Test dataframes
data = [["tom", 10], ["nick", 15], ["juli", 14]]
df1 = pd.DataFrame(data, columns=[1.3, 1.4], index=[1.2, 1.3, 1.4])

data = [["tom", 10, 1.1], ["nick", 15, 1.1], ["juli", 14, 1.1]]
df2 = pd.DataFrame(data, columns=["a", "b", "c"])

data = [["tom", 10, 1.1], ["nick", 15, 1.1], ["juli", 14, 1.1]]
df3 = pd.DataFrame(data)

data = ["tom", 10, 1.1]
df4 = pd.DataFrame(data)

query_df = df3
# proto_df = df_to_proto(query_df)
# new_df = proto_to_df(proto_df)

df5 = {
    "a": {0: "tom", 1: "nick", 2: "juli"},
    "b": {0: 10, 4: 15, 5: 14},
    "c": {6: 1.1, 7: 1.1, 8: 1.1},
}
