import pandas as pd
import pyorc
import csv
import os
import argparse

parser = argparse.ArgumentParser(description='Find optimized sort order for .orc file.')
parser.add_argument(
    '-f',
    '--csv_filepath',
    type=str,
    required=True,
    help='csv filepath'
)

parser.add_argument(
    '-c',
    '--col_list',
    nargs='+',
    required=True,
    help='columns header list of the csv file'
)

parser.add_argument(
    '-s',
    '--sort_col_list',
    nargs='+',
    required=True,
    help='sort columns ordered list'
)

parser.add_argument(
    '-r',
    '--row_num_list',
    nargs='+',
    default=[5000, 50000, 500000, 5000000, 50000000],
    help='row numbers list to test by iteration'
)

parser.add_argument(
    '--compressions',
    nargs='+',
    default=[0, 1, 5]
)

parser.add_argument(
    '--compression_strategies',
    nargs='+',
    default=[0, 1]
)

parser.add_argument(
    '--compression_block_sizes',
    nargs='+',
    default=[16384, 32768, 65536, 131072, 262144]
)

parser.add_argument(
    '--stripe_sizes',
    nargs='+',
    default=[16777216, 33554432, 67108864, 134217728, 268435456]
)

parser.add_argument(
    '--batch_sizes',
    nargs='+',
    default=[256, 512, 1024, 2048, 4096]
)

args = parser.parse_args()


def write_to_orc(schema, df, filepath, sort_col_list):
    output = open(filepath, 'wb')
    writer = pyorc.Writer(
        output,
        schema=schema,
        compression=compression,
        compression_strategy=compression_strategy,
        compression_block_size=compression_block_size,
        stripe_size=stripe_size,
        batch_size=batch_size,
    )

    for row in zip(*[df.get(col) for col in sort_col_list]):
        writer.write(row)

    writer.close()


def csv_to_orc(
        csv_filepath,
        col_list,
        sort_col_list,
        row_num,
        compression=1,
        compression_strategy=1,
        compression_block_size=65536,
        stripe_size=67108864,
        batch_size=1024
):
    df = pd.DataFrame(pd.read_csv(csv_filepath, usecols=col_list, low_memory=False, nrows=row_num))

    filename = os.path.splitext(csv_filepath)[0]

    if not os.path.isfile(f'{filename}{row_num}.csv'):
        df.to_csv(f'{filename}{row_num}.csv')

    schema = 'struct<'
    for i, col in enumerate(col_list):
        schema += f'col{i}:string,'
    schema = schema[0:-1] + '>'

    write_to_orc(schema, df, f'{filename}{row_num}.orc', sort_col_list)

    df = df.sort_values(by=sort_col_list, ascending=False)

    write_to_orc(schema, df, f'{filename}{row_num}_sorted.orc', sort_col_list)

    with open(f'{filename}_report.csv', 'a') as f:
        writer = csv.writer(f)
        writer.writerow([
            row_num,
            os.path.getsize(f'{filename}{row_num}.csv'),
            os.path.getsize(f'{filename}{row_num}.orc'),
            os.path.getsize(f'{filename}{row_num}_sorted.orc'),
            compression,
            compression_strategy,
            compression_block_size,
            stripe_size,
            batch_size
        ])


if __name__ == '__main__':

    for row_num in args.row_num_list:
        for compression in args.compressions:
            for compression_strategy in args.compression_strategies:
                for compression_block_size in args.compression_block_sizes:
                    for stripe_size in args.stripe_sizes:
                        for batch_size in args.batch_sizes:
                            csv_to_orc(
                                csv_filepath=args.csv_filepath,
                                col_list=args.col_list,
                                sort_col_list=args.sort_col_list,
                                row_num=row_num,
                                compression=compression,
                                compression_strategy=compression_strategy,
                                compression_block_size=compression_block_size,
                                stripe_size=stripe_size,
                                batch_size=batch_size
                            )
