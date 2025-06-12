from glob import glob
from functools import partial
import pandas as pd
from os import path, makedirs
import pdfplumber
import click
from multiprocessing import Pool


def run(outd, f):
    print(f"Converting {f}")
    file_base = path.basename(f).split(".")[0]
    pdf = pdfplumber.open(f)
    all_tables = []
    for page in pdf.pages:
        list_of_rows = page.extract_table()
        print(list_of_rows)
        # columns = list_of_rows[0]
        # columns = [col.replace('\n', '') for col in columns]
        data = []
        add_desc = False
        # for row in list_of_rows[1:]:
        # print(row)
        # if row[0] in ['New Events', 'Ongoing Events', 'Closed Events']:
        # continue
        # if row.count('') / len(row) > 0.5:
        # continue
        # if not row.count(None):
        # data.append(row)
        # else:
        # add_desc = True

        # core_row = ''.join(['' if v is None else v for v in row])
        # print(data)
        # print(data[-1])
        # print(core_row)
        # data[-1].append(core_row)
        # if add_desc:
        # columns.append('Description')
        # all_tables.append(pd.DataFrame(data=data, columns=columns))
    # all_tables = pd.concat(all_tables, ignore_index=True).fillna('')
    # all_tables = all_tables.applymap(lambda x: x.replace('\n', ''))
    # all_tables.to_csv(f"{outd}/{file_base}.csv", index=False)


@click.command()
@click.option(
    "-i",
    "--ind",
    "ind",
    help="Input directory containing pdf files",
    type=str,
    default=None,
    show_default=True,
)
@click.option(
    "-o",
    "--outd",
    "outd",
    help="Output directory",
    type=str,
    default=None,
    show_default=True,
)
@click.option(
    "-p",
    "--processes",
    "processes",
    help="Number of processes to use",
    type=int,
    default=1,
    show_default=True,
)
def main(ind, outd, processes):
    """
    Convert pdf to csv
    """
    input_files = glob(f"{ind}/*.pdf")
    makedirs(outd, exist_ok=True)
    runfunc = partial(run, outd)
    with Pool(processes) as p:
        p.map(runfunc, input_files)


if __name__ == "__main__":
    main()
