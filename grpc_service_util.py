def is_supported(data):
    """
    Checks if the given type is within `supported_datatypes` dictionary
    """
    import datetime

    import numpy as np

    supported_datatypes = {
        np.int32: "sint32_",
        np.int64: "sint64_",
        np.uint32: "uint32_",
        np.uint64: "uint64_",
        np.float32: "float_",
        np.float64: "double_",
        np.bool_: "bool_",
        np.bytes_: "bytes_",
        np.str_: "string_",
        np.ndarray: "array_",
        np.datetime64: "timestamp_",
        np.timedelta64: "duration_"
        # TODO : complex types, lower byte integers(8,16)
    }


    found_dtype = ""
    for key in supported_datatypes:
        if np.dtype(type(data)) == key:
            found_dtype = supported_datatypes[key]
            if found_dtype == "array_":
                if isinstance(data, datetime.datetime) or isinstance(
                    data, datetime.date
                ):
                    found_dtype = "timestamp_"
                elif isinstance(data, datetime.timedelta):
                    found_dtype = "duration_"
            break

    return found_dtype

def create_tuple_proto(tuple):
    """
    Convert given tuple list or tuple array to protobuf
    """
    import datetime

    import numpy as np
    import io_descriptors_pb2
    from google.protobuf.duration_pb2 import Duration
    from google.protobuf.timestamp_pb2 import Timestamp

    if len(tuple) == 0:
        raise ValueError("Provided tuple is either empty or invalid.")

    tuple_arr = []
    for item in tuple:
        dtype = is_supported(item)

        if not dtype:
            raise ValueError(
                f'Invalid datatype "{type(item).__name__}" within tuple.'
            )
        elif dtype == "timestamp_":
            if isinstance(item, np.datetime64):
                item = item.astype(datetime.datetime)
            if isinstance(item, datetime.date):
                item = datetime.datetime(item.year, item.month, item.day)
            t = Timestamp()
            t.FromDatetime(item)
            tuple_arr.append(io_descriptors_pb2.Value(**{"timestamp_": t}))
        elif dtype == "duration_":
            if isinstance(item, np.timedelta64):
                item = item.astype(datetime.timedelta)
            d = Duration()
            d.FromTimedelta(item)
            tuple_arr.append(io_descriptors_pb2.Value(**{"duration_": d}))
        elif dtype == "array_":
            if not all(isinstance(x, type(item[0])) for x in item):
                val = create_tuple_proto(item)
                tuple_arr.append(io_descriptors_pb2.Value(tuple_=val))
            else:
                val = arr_to_proto(item)
                tuple_arr.append(io_descriptors_pb2.Value(array_=val))
        else:
            tuple_arr.append(io_descriptors_pb2.Value(**{f"{dtype}": item}))

    return io_descriptors_pb2.Tuple(value_=tuple_arr)

def arr_to_proto(arr):
    """
    Convert given list or array to protobuf
    """
    import datetime

    import numpy as np
    import io_descriptors_pb2
    from google.protobuf.duration_pb2 import Duration
    from google.protobuf.timestamp_pb2 import Timestamp

    if len(arr) == 0:
        raise ValueError("Provided array is either empty or invalid.")
    if not all(isinstance(x, type(arr[0])) for x in arr):
        raise ValueError("Entered tuple, expecting array.")

    dtype = is_supported(arr[0])
    if not dtype:
        raise ValueError("Dtype is not supported.")

    if dtype == "timestamp_":
        timestamp_arr = []
        for dt in arr:
            if isinstance(dt, np.datetime64):
                dt = dt.astype(datetime.datetime)
            if isinstance(dt, datetime.date):
                dt = datetime.datetime(dt.year, dt.month, dt.day)
            t = Timestamp()
            t.FromDatetime(dt)
            timestamp_arr.append(t)
        return io_descriptors_pb2.NumpyNdarray(
            dtype="timestamp_", timestamp_=timestamp_arr
        )
    elif dtype == "duration_":
        duration_arr = []
        for td in arr:
            if isinstance(td, np.timedelta64):
                td = td.astype(datetime.timedelta)
            d = Duration()
            d.FromTimedelta(td)
            duration_arr.append(d)
        return io_descriptors_pb2.NumpyNdarray(
            dtype="duration_", duration_=duration_arr
        )
    elif dtype != "array_":
        return io_descriptors_pb2.NumpyNdarray(**{"dtype": dtype, f"{dtype}": arr})

    return_arr = []
    is_tuple = False
    for i in range(len(arr)):
        if not all(isinstance(x, type(arr[i][0])) for x in arr[i]):
            is_tuple = True
            val = create_tuple_proto(arr[i])
        else:
            val = arr_to_proto(arr[i])
        return_arr.append(val)

    if is_tuple:
        return_arr = io_descriptors_pb2.NumpyNdarray(
            dtype="tuple_", tuple_=return_arr
        )
    else:
        return_arr = io_descriptors_pb2.NumpyNdarray(
            dtype="array_", array_=return_arr
        )

    return return_arr


def handle_tuple(proto_tuple):
    """
    Convert given protobuf tuple to a tuple list
    """
    from google.protobuf.duration_pb2 import Duration
    from google.protobuf.timestamp_pb2 import Timestamp

    import io_descriptors_pb2

    tuple_arr = [i for i in getattr(proto_tuple, "value_")]

    if not tuple_arr:
        raise ValueError("Provided tuple is either empty or invalid.")

    return_arr = []

    for i in range(len(tuple_arr)):
        val = getattr(tuple_arr[i], tuple_arr[i].WhichOneof("dtype"))

        if not val:
            raise ValueError("Provided protobuf tuple doesn't have a value.")

        if tuple_arr[i].WhichOneof("dtype") == "timestamp_":
            val = Timestamp.ToDatetime(val)
        elif tuple_arr[i].WhichOneof("dtype") == "duration_":
            val = Duration.ToTimedelta(val)

        if isinstance(val, io_descriptors_pb2.NumpyNdarray):
            val = proto_to_arr(val)
        elif isinstance(val, io_descriptors_pb2.Tuple):
            val = handle_tuple(val)
        return_arr.append(val)

    return return_arr


def proto_to_arr(proto_arr):
    """
    Convert given protobuf array to python list
    """
    from google.protobuf.duration_pb2 import Duration
    from google.protobuf.timestamp_pb2 import Timestamp

    import io_descriptors_pb2

    return_arr = [i for i in getattr(proto_arr, proto_arr.dtype)]

    if proto_arr.dtype == "timestamp_":
        return_arr = [Timestamp.ToDatetime(dt) for dt in return_arr]
    elif proto_arr.dtype == "duration_":
        return_arr = [Duration.ToTimedelta(td) for td in return_arr]

    if not return_arr:
        raise ValueError("Provided array is either empty or invalid")

    for i in range(len(return_arr)):
        if isinstance(return_arr[i], io_descriptors_pb2.NumpyNdarray):
            return_arr[i] = proto_to_arr(return_arr[i])
        elif isinstance(return_arr[i], io_descriptors_pb2.Tuple):
            return_arr[i] = handle_tuple(return_arr[i])

    return return_arr
