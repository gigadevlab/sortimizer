import pandas as pd
import pyorc
import csv
import os


def csv_to_orc(
        csv_filepath,
        rows,
        compression=1,
        compression_strategy=1,
        compression_block_size=65536,
        stripe_size=67108864,
        batch_size=1024
):
    col_list = ['IP', 'Time', 'URL', 'Status']
    df = pd.DataFrame(pd.read_csv(csv_filepath, usecols=col_list, low_memory=False, nrows=rows))

    df.to_csv(f'weblog{rows}.csv')

    output = open(f'weblog{rows}.orc', 'wb')
    writer = pyorc.Writer(
        output,
        "struct<col0:string,col1:string,col2:string,col3:string>",
        compression=compression,
        compression_strategy=compression_strategy,
        compression_block_size=compression_block_size,
        stripe_size=stripe_size,
        batch_size=batch_size,
    )

    for row in zip(df['Status'], df['URL'], df['IP'], df['Time']):
        writer.write(row)

    writer.close()

    df = df.sort_values(by=['Status', 'URL', 'IP', 'Time'], ascending=False)

    output = open(f'weblog{rows}_s.orc', 'wb')
    writer = pyorc.Writer(
        output,
        "struct<col0:string,col1:string,col2:string,col3:string>",
        compression=compression,
        compression_strategy=compression_strategy,
        compression_block_size=compression_block_size,
        stripe_size=stripe_size,
        batch_size=batch_size,
    )

    for row in zip(df['Status'], df['URL'], df['IP'], df['Time']):
        writer.write(row)

    writer.close()

    with open('report.csv', 'a') as f:
        writer = csv.writer(f)
        writer.writerow([
            rows,
            os.path.getsize(f'weblog{rows}.csv'),
            os.path.getsize(f'weblog{rows}.orc'),
            os.path.getsize(f'weblog{rows}_s.orc'),
            compression,
            compression_strategy,
            compression_block_size,
            stripe_size,
            batch_size
        ])


if __name__ == '__main__':
    rows_list = [5000, 50000, 500000, 5000000, 50000000]
    compressions = [0, 1, 5]
    compression_strategies = [0, 1]
    compression_block_sizes = [16384, 32768, 65536, 131072, 262144]
    stripe_sizes = [16777216, 33554432, 67108864, 134217728, 268435456]
    batch_sizes = [256, 512, 1024, 2048, 4096]

    for rows in rows_list:
        for compression in compressions:
            for compression_strategy in compression_strategies:
                for compression_block_size in compression_block_sizes:
                    for stripe_size in stripe_sizes:
                        for batch_size in batch_sizes:
                            csv_to_orc(
                                csv_filepath='weblog.csv',
                                rows=rows,
                                compression=compression,
                                compression_strategy=compression_strategy,
                                compression_block_size=compression_block_size,
                                stripe_size=stripe_size,
                                batch_size=batch_size
                            )
