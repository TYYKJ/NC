import os

import pandas as pd


def merge_csv_file(root_path):
    # content = pd.read_csv(file_path, index_col=0)
    # df = pd.concat([df, content])
    # df.to_csv(new_file_path)
    df = pd.DataFrame()
    subdirs = os.listdir(root_path)
    subdirs = subdirs[:-2]
    for i in range(1, 13):
        ff = str(i)
        p = os.path.join(root_path, ff)
        files = os.listdir(p)
        for f in files:
            f_p = os.path.join(p, f)
            con = pd.read_csv(f_p, index_col=0)
            con.to_csv(f, mode='a+', index='time')


def distinct(file):
    df = pd.read_csv(file, header=None)
    datalist = df.drop_duplicates()
    datalist.to_csv(file + '.csv', index='time', header=False)
    print('完成去重')


distinct(r'E:\PycharmProjects\ncDataProcess\processed\merge_files\120.5-35.0.csv')
