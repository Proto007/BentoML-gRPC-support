import datetime


def is_supported(data):
    """
    Checks if the given type is within `supported_datatypes` dictionary
    """
    import numpy as np

    supported_datatypes = {
        np.float32: "float_value",
        np.float64: "double_value",
        np.bytes_: "bytes_value",
        np.bool_: "bool_value",
        np.str_: "string_value",
        np.uint32: "uint32_value",
        np.int32: "sint32_value",
        np.datetime64: "timestamp_value",
        np.timedelta64: "duration_value",
        np.ndarray: "array_value",
        np.uint64: "uint64_value",
        np.int64: "sint64_value"
        # TODO : complex types, lower byte integers(8,16)
    }

    found_dtype = ""
    for key in supported_datatypes:
        if np.dtype(type(data)) == key:
            found_dtype = supported_datatypes[key]
            if found_dtype == "array_value":
                if isinstance(data, datetime.datetime) or isinstance(
                    data, datetime.date
                ):
                    found_dtype = "timestamp_value"
                elif isinstance(data, datetime.timedelta):
                    found_dtype = "duration_value"
            break

    return found_dtype


def create_tuple_proto(tuple):
    """
    Convert given tuple list or tuple array to protobuf
    """
    import numpy as np
    from google.protobuf.duration_pb2 import Duration
    from google.protobuf.timestamp_pb2 import Timestamp

    import payload_pb2

    if len(tuple) == 0:
        raise ValueError("Provided tuple is either empty or invalid.")

    tuple_arr = []
    for item in tuple:
        dtype = is_supported(item)

        if not dtype:
            raise ValueError(f'Invalid datatype "{type(item).__name__}" within tuple.')
        elif dtype == "timestamp_value":
            if isinstance(item, np.datetime64):
                item = item.astype(datetime.datetime)
            if isinstance(item, datetime.date):
                item = datetime.datetime(item.year, item.month, item.day)
            t = Timestamp()
            t.FromDatetime(item)
            tuple_arr.append(payload_pb2.Value(**{"timestamp_value": t}))
        elif dtype == "duration_value":
            if isinstance(item, np.timedelta64):
                item = item.astype(datetime.timedelta)
            d = Duration()
            d.FromTimedelta(item)
            tuple_arr.append(payload_pb2.Value(**{"duration_value": d}))
        elif dtype == "array_value":
            if not all(isinstance(x, type(item[0])) for x in item):
                val = create_tuple_proto(item)
                tuple_arr.append(payload_pb2.Value(tuple_value=val))
            else:
                val = arr_to_proto(item)
                tuple_arr.append(payload_pb2.Value(array_value=val))
        else:
            tuple_arr.append(payload_pb2.Value(**{f"{dtype}": item}))

    return payload_pb2.Tuple(value=tuple_arr)


def arr_to_proto(arr):
    """
    Convert given list or array to protobuf
    """
    import numpy as np
    from google.protobuf.duration_pb2 import Duration
    from google.protobuf.timestamp_pb2 import Timestamp

    import payload_pb2

    if len(arr) == 0:
        raise ValueError("Provided array is either empty or invalid.")
    if not all(isinstance(x, type(arr[0])) for x in arr):
        raise ValueError("Entered tuple, expecting array.")

    dtype = is_supported(arr[0])
    if not dtype:
        raise ValueError("Dtype is not supported.")

    if dtype == "timestamp_value":
        timestamp_arr = []
        for dt in arr:
            if isinstance(dt, np.datetime64):
                dt = dt.astype(datetime.datetime)
            if isinstance(dt, datetime.date):
                dt = datetime.datetime(dt.year, dt.month, dt.day)
            t = Timestamp()
            t.FromDatetime(dt)
            timestamp_arr.append(t)
        return payload_pb2.Array(timestamp_value=timestamp_arr)
    elif dtype == "duration_value":
        duration_arr = []
        for td in arr:
            if isinstance(td, np.timedelta64):
                td = td.astype(datetime.timedelta)
            d = Duration()
            d.FromTimedelta(td)
            duration_arr.append(t)
        return payload_pb2.Array(duration_value=duration_arr)
    elif dtype != "array_value":
        return payload_pb2.Array(**{f"{dtype}": arr})

    return_arr = []
    is_tuple = False
    for item in arr:
        if not all(isinstance(x, type(item[0])) for x in item):
            is_tuple = True
            val = create_tuple_proto(item)
        else:
            val = arr_to_proto(item)
        return_arr.append(val)

    if is_tuple:
        return_arr = payload_pb2.Array(tuple_value=return_arr)
    else:
        return_arr = payload_pb2.Array(array_value=return_arr)

    return return_arr


def handle_tuple(proto_tuple):
    """
    Convert given protobuf tuple to a tuple list
    """
    from google.protobuf.duration_pb2 import Duration
    from google.protobuf.timestamp_pb2 import Timestamp

    import payload_pb2

    tuple_arr = [i for i in getattr(proto_tuple, "value")]

    if not tuple_arr:
        raise ValueError("Provided tuple is either empty or invalid.")

    return_arr = []

    for item in tuple_arr:
        val = getattr(item, item.WhichOneof("value"))

        if not val:
            raise ValueError("Provided protobuf tuple is missing a value.")

        if item.WhichOneof("value") == "timestamp_value":
            val = Timestamp.ToDatetime(val)
        elif item.WhichOneof("value") == "duration_value":
            val = Duration.ToTimedelta(val)

        if isinstance(val, payload_pb2.Array):
            val = proto_to_arr(val)
        elif isinstance(val, payload_pb2.Tuple):
            val = handle_tuple(val)
        return_arr.append(val)

    return return_arr


def WhichArray(proto_arr):
    import payload_pb2

    if not proto_arr:
        return ""

    arr_types = [field.name for field in payload_pb2.Array.DESCRIPTOR.fields]

    return_type = ""
    for arr_type in arr_types:
        if len(getattr(proto_arr, arr_type)) != 0:
            if not return_type:
                return_type = arr_type
            else:
                raise ValueError("More than one repeated Array fields contain data.")

    return return_type


def proto_to_arr(proto_arr):
    """
    Convert given protobuf array to python list
    """
    from google.protobuf.duration_pb2 import Duration
    from google.protobuf.timestamp_pb2 import Timestamp

    import payload_pb2

    array_type = WhichArray(proto_arr)
    if not array_type:
        raise ValueError("Provided array is either empty or invalid.")

    return_arr = [i for i in getattr(proto_arr, array_type)]

    if array_type == "timestamp_value":
        return_arr = [Timestamp.ToDatetime(dt) for dt in return_arr]
    elif array_type == "duration_value":
        return_arr = [Duration.ToTimedelta(td) for td in return_arr]

    for i, item in enumerate(return_arr):
        if isinstance(item, payload_pb2.Array):
            return_arr[i] = proto_to_arr(item)
        elif isinstance(item, payload_pb2.Tuple):
            return_arr[i] = handle_tuple(item)

    return return_arr


def is_key_supported(data):
    import numpy as np

    supported_keytypes = {
        np.int32: "sint32_",
        np.int64: "sint64_",
        np.uint32: "uint32_",
        np.uint64: "uint64_",
        np.bool_: "bool_",
        np.str_: "string_",
    }

    found_keytype = ""
    for key in supported_keytypes:
        if np.dtype(type(data)) == key:
            found_keytype = supported_keytypes[key]
            break

    return found_keytype


def create_series_proto(pd_series):
    """{0: 'tom', 1: 'nick', 2: 'juli'}"""
    import numpy as np
    from google.protobuf.duration_pb2 import Duration
    from google.protobuf.timestamp_pb2 import Timestamp

    import payload_pb2

    index_type = ""
    return_dict = {}
    for index in pd_series:
        if not is_key_supported(index):
            raise ValueError("Invalid datatype for index.")
        if not index_type:
            index_type = is_key_supported(index)
        elif is_key_supported(index) != index_type:
            raise ValueError("Mixed datatype indexes are not supported.")
        value_type = is_supported(pd_series[index])
        if not value_type:
            raise ValueError(
                f'Invalid datatype "{type(pd_series[index]).__name__}" within series.'
            )
        elif value_type == "timestamp_":
            if isinstance(item, np.datetime64):
                item = item.astype(datetime.datetime)
            if isinstance(item, datetime.date):
                item = datetime.datetime(item.year, item.month, item.day)
            t = Timestamp()
            t.FromDatetime(item)
            return_dict[index] = payload_pb2.Value(**{"timestamp_": t})
        elif value_type == "duration_":
            if isinstance(item, np.timedelta64):
                item = item.astype(datetime.timedelta)
            d = Duration()
            d.FromTimedelta(item)
            return_dict[index] = payload_pb2.Value(**{"duration_": d})
        elif value_type == "array_":
            if not all(isinstance(x, type(item[0])) for x in item):
                val = create_tuple_proto(item)
                return_dict[index] = payload_pb2.Value(tuple_=val)
            else:
                val = arr_to_proto(item)
                return_dict[index] = payload_pb2.Value(array_=val)
        else:
            return_dict[index] = payload_pb2.Value(
                **{f"{value_type}": pd_series[index]}
            )

    return payload_pb2.Series(**{"keytype": index_type, f"{index_type}": return_dict})
    ...


def df_to_proto(df):
    import payload_pb2

    dict_df = df.to_dict()
    return_dict = {}
    column_type = ""
    for key in dict_df:
        if not is_key_supported(key):
            raise ValueError("Invalid datatype for column.")
        if not column_type:
            column_type = is_key_supported(key)
        elif is_key_supported(key) != column_type:
            raise ValueError("Mixed datatype columns are not supported.")
        series_proto = create_series_proto(dict_df[key])
        return_dict[key] = series_proto

    return payload_pb2.Dataframe(
        **{"keytype": column_type, f"{column_type}": return_dict}
    )


def handle_series(proto_series):
    from google.protobuf.duration_pb2 import Duration
    from google.protobuf.timestamp_pb2 import Timestamp

    import payload_pb2

    series_data = getattr(proto_series, f"{proto_series.keytype}")
    series_dict = {}

    for index in series_data:
        val = getattr(series_data[index], series_data[index].WhichOneof("dtype"))

        if not val:
            raise ValueError("Provided protobuf tuple doesn't have a value.")

        if series_data[index].WhichOneof("dtype") == "timestamp_":
            val = Timestamp.ToDatetime(val)
        elif series_data[index].WhichOneof("dtype") == "duration_":
            val = Duration.ToTimedelta(val)

        if isinstance(val, payload_pb2.NumpyNdarray):
            val = proto_to_arr(val)
        elif isinstance(val, payload_pb2.Tuple):
            val = handle_tuple(val)
        series_dict[index] = val

    return series_dict


def proto_to_df(proto_df):
    import pandas as pd

    proto_df = getattr(proto_df, f"{proto_df.keytype}")
    df_dict = {}
    for key in proto_df:
        series_dict = handle_series(proto_df[key])
        df_dict[key] = series_dict
    return pd.DataFrame(df_dict)
