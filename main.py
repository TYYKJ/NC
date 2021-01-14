import multiprocessing
import os
import shutil
import threading
import time
from functools import wraps
from multiprocessing import Process

import pandas as pd
import yaml
from tqdm import tqdm

import nc_data_extract


def timethis(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time.perf_counter()
        r = func(*args, **kwargs)
        end = time.perf_counter()
        print('{}.{} : {}'.format(func.__module__, func.__name__, end - start))
        return r

    return wrapper


class OperateFiles:
    def __init__(self):
        self._nc = nc_data_extract.NC()
        self._config = load_config()
        self.root_path = self._config['root_path']
        self._lon = self._config['lon']
        self._lat = self._config['lat']
        self.make_dir()
        self.get_data()

    def make_dir(self):
        try:
            os.mkdir('processed')
            nc_file_path = self.get_files()
            keys = nc_file_path.keys()
            for key in keys:
                dir_name = key.split('\\')[-1]
                os.mkdir(f'processed/{dir_name}')
        except FileExistsError:
            shutil.rmtree('processed')
            os.mkdir('processed')
            nc_file_path = self.get_files()
            keys = nc_file_path.keys()
            for key in keys:
                dir_name = key.split('\\')[-1]
                os.mkdir(f'processed/{dir_name}')

    def get_files(self):
        """
        获取各子文件夹中的nc文件
        eg: {'F:\\2014\\01': ['ww3.201401_dir.nc',...]}

        :return: dict
        """
        nc_file_path = dict()
        for folder, subdir, files in os.walk(self.root_path):
            if not subdir:
                # 有文件的话将整个文件列表加到字典里
                if files:
                    nc_file_path[folder.split('.')[-1]] = files

        keys = nc_file_path.keys()
        for key in keys:
            cache = []
            for file in nc_file_path.get(key):
                full_path = os.path.join(key, file)
                cache.append(full_path)
                nc_file_path[key] = cache

        return nc_file_path

    # @timethis
    def get_data(self):
        files_dict = self.get_files()
        keys = files_dict.keys()
        for files in tqdm(keys, desc='nc列表加载'):
            # 每个进程负责一个月的数据处理
            # p = multiprocessing.Process(target=self.multiprocess_accelerate, args=(files_dict, files))
            p = threading.Thread(target=self.multiprocess_accelerate, args=(files_dict, files))
            p.start()
            p.join()

    def multiprocess_accelerate(self, files_dict, files):
        """
        负责一个月数据提取的进程加速

        :param files_dict:
        :param files:
        :return:
        """
        for file in files_dict[files]:
            dataset = self._nc.load_nc_data(file)
            lon, lat = self._nc.get_part_lon_lat(dataset, self._lon, self._lat)

            _time = self._nc.get_time(dataset)
            for single_lon in lon[0]:
                for single_lat in lat[0]:
                    data = self._nc.get_part_data(dataset, single_lon, single_lat)
                    attributes = data.keys()
                    for k in tqdm(attributes,
                                  desc=f'正在处理{single_lon}-{single_lat}位置数据'):
                        csv_name = f"{single_lon}-{single_lat}.csv"

                        self.append2csv(_time, data[k], k, files, csv_name)

    @staticmethod
    def append2csv(data_time, data, column_name, subdir, csv_name):
        """
        追加数据到csv文件

        :param data_time: 时间列
        :param data: 数据列
        :param column_name: 列名
        :param subdir: 保存的子文件夹名
        :param csv_name: csv文件名
        :return:
        """

        subdir = subdir.split('\\')[-1]

        processed_path = 'processed'
        csv_saved_path = os.path.join(os.path.join(processed_path, subdir), csv_name)

        try:
            try:
                df = pd.read_csv(csv_saved_path, index_col=0, engine='python')
                if column_name not in df.columns:
                    df[column_name] = data
                    df.to_csv(csv_saved_path)
            except pd.errors.EmptyDataError:
                pass

        except FileNotFoundError:

            data_dict = {'time': data_time, column_name: data}
            df = pd.DataFrame(data_dict)
            df.to_csv(csv_saved_path)


def load_config():
    """
    加载配置

    :return: dict
    """
    with open('config.yml', 'r') as f:
        data = f.read()

    content = yaml.load(data, Loader=yaml.FullLoader)
    return content


if __name__ == '__main__':
    import time

    start = time.time()
    OperateFiles().get_data()
    end = time.time()

    print(f'消耗时间{end - start}s')
