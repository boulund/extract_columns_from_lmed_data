#!/usr/bin/env python3

from sys import argv, exit
import os
import pandas as pd


def extract_columns_from_sas(filename, columns, output_csv, chunksize=100000):
    """Iterate through SAS file in chunks,
    extract columns from chunks into separate subdataframe,
    convert contents to string (instead of bytestring, because of bug in pandas csv-writer),
    write (append) extracted headers from chunk to CSV file.
    """

    reader = pd.read_sas(filename, chunksize=chunksize, encoding='latin1')
    print(filename, "contains", reader.row_count, "rows")

    for count, chunk in enumerate(reader, start=1):
        percent_read = count*chunksize / reader.row_count * 100
        print("\r{:3.4f} % read".format(percent_read), end="")
        if count == 1:
            chunk.to_csv(output_csv, columns=columns, index=False, mode='a')
        else:
            chunk.to_csv(output_csv, columns=columns, index=False, mode='a', header=False)



if __name__ == "__main__":
    if len(argv) < 2:
        print("Usage: script.py file.sas7bdat")
        exit(1)
    else:
        lmed_file = argv[1]
        if os.path.isfile(lmed_file+".csv"):
            print("WARNING:", lmed_file+".csv already exists!")
            exit(2)

    extract_columns_from_sas(lmed_file, columns=["lpnr", "KON", "atc", "EDATUM"], output_csv=lmed_file+".csv")
