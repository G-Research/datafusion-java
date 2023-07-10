from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq


def main():
    resource_dir = Path("src/test/resources")
    write_dictionary_data(resource_dir / "dictionary_data.parquet")
    write_zstd_ipc_data(resource_dir / "zstd_compressed.arrow")


def write_dictionary_data(path):
    num_rows = 100

    dict_array_x = pa.DictionaryArray.from_arrays(
        pa.array([i % 3 for i in range(num_rows)]),
        pa.array(["one", "two", "three"])
    )

    dict_array_y = pa.DictionaryArray.from_arrays(
        pa.array([i % 3 for i in range(num_rows)]),
        pa.array(["four", "five", "six"])
    )

    table = pa.Table.from_arrays([dict_array_x, dict_array_y], ["x", "y"])
    pq.write_table(table, path)


def write_zstd_ipc_data(path):
    schema = pa.schema([pa.field("x", pa.int64())])

    options = pa.ipc.IpcWriteOptions(compression='zstd')
    with pa.OSFile(str(path), 'wb') as sink:
        with pa.ipc.new_file(sink, schema, options=options) as writer:
            for _ in range(2):
                xs = pa.array([i % 10 for i in range(500)], type=pa.int64())
                batch = pa.record_batch([xs], schema)
                writer.write(batch)


if __name__ == '__main__':
    main()
