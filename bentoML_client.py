# Import necessary modules
import datetime
from turtle import numinput

import grpc
import numpy as np
from google.protobuf.duration_pb2 import Duration
from google.protobuf.json_format import MessageToDict
from google.protobuf.timestamp_pb2 import Timestamp
from requests import request

# Import generated files
import bentoml_service_pb2
import bentoml_service_pb2_grpc
import io_descriptors_pb2
from grpc_service_util import arr_to_proto, proto_to_arr

# # Define a channel with the backend server address
channel = grpc.insecure_channel("localhost:50051")
stub = bentoml_service_pb2_grpc.BentoMLStub(channel)

"""
    Testing LDA Model
"""
query_description = "If happy ever after did exist, I was to be holding you like this."


def test_lda(query_description=query_description):
    query_description = bentoml_service_pb2.BentoServiceInput(
        input=bentoml_service_pb2.BentoServiceMessage(text=query_description)
    )
    response = stub.predict(query_description)
    return response.output.array


print(test_lda())
print(proto_to_arr(test_lda()))
"""
    Testing String
"""
# string_data="badaS ma I ,iH"

# data=input_data=bentoml_service_pb2.BentoServiceInput(input=bentoml_service_pb2.BentoServiceMessage(text=string_data))

# response=stub.predict(data)

# print(response.output.text)

"""
    Testing Numpy
"""
# Test arrays
arr1 = [1, 2, 3, 4, 5]
arr1_numpy = np.array(arr1)
arr1_proto = io_descriptors_pb2.NumpyNdarray(dtype="sint32_", sint32_=arr1_numpy)
a1 = [arr1, arr1_numpy, arr1_proto]

arr2 = [[1, 2], [3, 4], [5, 6]]
arr2_numpy = np.array(arr2)
subarr_list_proto = []
for i in arr2_numpy:
    subarr_list_proto.append(
        io_descriptors_pb2.NumpyNdarray(dtype="sint32_", sint32_=i)
    )
arr2_proto = io_descriptors_pb2.NumpyNdarray(dtype="array_", array_=subarr_list_proto)
a2 = [arr2, arr2_numpy, arr2_proto]

arr3 = [[1, "a"], [2, "b"]]
arr3_numpy = np.array([(1, "a"), (2, "b")], dtype=[("num", "i8"), ("char", "U10")])
tup_list = []
for i in arr3:
    item1 = io_descriptors_pb2.Value(sint64_=i[0])
    item2 = io_descriptors_pb2.Value(string_=i[1])
    tup_ = io_descriptors_pb2.Tuple(value_=[item1, item2])
    tup_list.append(tup_)
arr3_proto = io_descriptors_pb2.NumpyNdarray(dtype="tuple_", tuple_=tup_list)
a3 = [arr3, arr3_numpy, arr3_proto]

arr4 = [[["a", "b"], 0.1], [["A", "B"], 0.2]]
arr4_numpy = np.array(arr4, dtype=object)
arr4_proto = []
for i in arr4:
    item1_arr = io_descriptors_pb2.NumpyNdarray(dtype="string_", string_=i[0])
    item1 = io_descriptors_pb2.Value(array_=item1_arr)
    item2 = io_descriptors_pb2.Value(double_=i[1])
    proto_tuple = io_descriptors_pb2.Tuple(value_=[item1, item2])
    arr4_proto.append(proto_tuple)
arr4_proto = io_descriptors_pb2.NumpyNdarray(dtype="tuple_", tuple_=arr4_proto)
a4 = [arr4, arr4_numpy, arr4_proto]

arr5 = [[[[1, 2], "b"], 0.1], [[[3, 4], "B"], 0.2]]
arr5_numpy = np.array(arr5, dtype=object)
arr5_proto = []
for i in arr5_numpy:
    val1_arr = io_descriptors_pb2.NumpyNdarray(dtype="sint64_", sint64_=i[0][0])
    val1 = io_descriptors_pb2.Value(array_=val1_arr)
    val2 = io_descriptors_pb2.Value(string_=i[0][1])
    tup_ = io_descriptors_pb2.Tuple(value_=[val1, val2])
    item1 = io_descriptors_pb2.Value(tuple_=tup_)
    item2 = io_descriptors_pb2.Value(double_=i[1])
    proto_tuple = io_descriptors_pb2.Tuple(value_=[item1, item2])
    arr5_proto.append(proto_tuple)
arr5_proto = io_descriptors_pb2.NumpyNdarray(dtype="tuple_", tuple_=arr5_proto)
a5 = [arr5, arr5_numpy, arr5_proto]

arr6_numpy = np.arange("2005-02-01", "2005-03-02", dtype="datetime64[h]")
arr6 = arr6_numpy.tolist()
arr6_proto = []
for i in arr6:
    if type(i) == datetime.date:
        i = datetime.datetime(i.year, i.month, i.day)
    t = Timestamp()
    t.FromDatetime(i)
    arr6_proto.append(t)
arr6_proto = io_descriptors_pb2.NumpyNdarray(dtype="timestamp_", timestamp_=arr6_proto)
a6 = [arr6, arr6_numpy, arr6_proto]

arr7 = [
    [[[datetime.datetime(2020, 5, 17), datetime.datetime(2020, 5, 18)], "b"], 0.1],
    [[[datetime.datetime(2020, 5, 19), datetime.datetime(2020, 5, 20)], "B"], 0.2],
]
arr7_numpy = np.array(arr7, dtype=object)
arr7_proto = []
for i in arr7_numpy:
    t_arr = []
    for x in i[0][0]:
        t = Timestamp()
        t.FromDatetime(x)
        t_arr.append(t)
    val1_arr = io_descriptors_pb2.NumpyNdarray(dtype="timestamp_", timestamp_=t_arr)
    val1 = io_descriptors_pb2.Value(array_=val1_arr)
    val2 = io_descriptors_pb2.Value(string_=i[0][1])
    tup_ = io_descriptors_pb2.Tuple(value_=[val1, val2])
    item1 = io_descriptors_pb2.Value(tuple_=tup_)
    item2 = io_descriptors_pb2.Value(double_=i[1])
    proto_tuple = io_descriptors_pb2.Tuple(value_=[item1, item2])
    arr7_proto.append(proto_tuple)
arr7_proto = io_descriptors_pb2.NumpyNdarray(dtype="tuple_", tuple_=arr7_proto)
a7 = [arr7, arr7_numpy, arr7_proto]

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
arr8_proto = []
for i in arr8_numpy:
    t_arr = []
    for x in i[0][0]:
        t = Timestamp()
        t.FromDatetime(x)
        t_arr.append(t)
    val1_arr = io_descriptors_pb2.NumpyNdarray(dtype="timestamp_", timestamp_=t_arr)
    val1 = io_descriptors_pb2.Value(array_=val1_arr)
    val2 = io_descriptors_pb2.Value(string_=i[0][1])
    tup_ = io_descriptors_pb2.Tuple(value_=[val1, val2])
    item1 = io_descriptors_pb2.Value(tuple_=tup_)
    d = Duration()
    d.FromTimedelta(i[1])
    item2 = io_descriptors_pb2.Value(duration_=d)
    proto_tuple = io_descriptors_pb2.Tuple(value_=[item1, item2])
    arr8_proto.append(proto_tuple)
arr8_proto = io_descriptors_pb2.NumpyNdarray(dtype="tuple_", tuple_=arr8_proto)
a8 = [arr8, arr8_numpy, arr8_proto]

query_arr = a8

def test_numpy(arr=query_arr[1]):
    data = bentoml_service_pb2.BentoServiceInput(
        input=bentoml_service_pb2.BentoServiceMessage(array=arr_to_proto(arr))
    )
    response = stub.predict(data)
    return response.output.array

# print(test_numpy())
# print(proto_to_arr(test_numpy()))
"""
Pandas
"""
# import pandas as pd

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

# # Test dataframes
# data = [['tom', 10], ['nick', 15], ['juli', 14]]
# df1 = pd.DataFrame(data, columns=[1.3,1.4],index=[1.2,1.3,1.4])

# data = [['tom', 10, 1.1], ['nick', 15, 1.1], ['juli', 14, 1.1]]
# df2 = pd.DataFrame(data, columns=["a", "b", "c"])

# data = [['tom', 10, 1.1], ['nick', 15, 1.1], ['juli', 14, 1.1]]
# df3 = pd.DataFrame(data)

# data = ['tom', 10, 1.1]
# df4 = pd.DataFrame(data)

# query_df = df2
# proto_df = df_to_proto(query_df)
# new_df = proto_to_df(proto_df)
# print(proto_df)
# print(query_df)
# print(new_df)

# print(df2.to_dict())