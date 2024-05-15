"""BRC challenge with polars in python"""

from time import perf_counter

import polars as pl

pl.Config.set_streaming_chunk_size(1024 ** 2)


def brc(file: str) -> None:
    """Run the brc challenge with polars"""
    output = (
        pl.scan_csv(
            file, separator=';', schema={'name': pl.String, 'temp': pl.Float32},
            has_header=False, encoding='utf8-lossy', eol_char='\n'
        )
        .group_by('name')
        .agg(
            pl.col('temp').min().alias('min'),
            pl.col('temp').mean().alias('mean'),
            pl.col('temp').max().alias('max')
        )
        .sort('name')
        .collect(streaming=True)
    )

    print(
        '{',
        ', '.join(
            f'{row["name"]}={row["min"]:.1f}/{row["mean"]:.1f}/{row["max"]:.1f}'
            for row in output.iter_rows(named=True)
        ),
        '}',
        sep=''
    )


if __name__ == '__main__':
    input_file = r"C:\Users\Max\Downloads\1brc-main\data\measurements.txt"
    start = perf_counter()
    brc(input_file)
    end = perf_counter()
    print(f'Run Time: {(end - start):.2f} sec')
