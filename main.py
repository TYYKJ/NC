import os
import platform
import shutil
import threading

import pandas as pd
import yaml
from tqdm import tqdm

import nc_data_extract


class OperateFiles:
    def __init__(self):
        self._nc = nc_data_extract.NC()
        self._config = load_config()
        self.root_path = self._config['root_path']
        self._lon = self._config['lon']
        self._lat = self._config['lat']
        self.now_os = platform.platform()
        self.make_dir()

    def make_dir(self):
        try:
            os.mkdir('processed')
            os.mkdir('merge_data')
            nc_file_path = self.get_files()
            keys = nc_file_path.keys()
            for key in keys:
                if 'Windows' in self.now_os:
                    dir_name = key.split('\\')[-1]
                    os.mkdir(f'processed/{dir_name}')
                else:
                    dir_name = key.split('/')[-1]
                    os.mkdir(f'processed/{dir_name}')
        except FileExistsError:
            shutil.rmtree('processed')
            os.mkdir('processed')
            nc_file_path = self.get_files()
            keys = nc_file_path.keys()
            for key in keys:
                now_os = platform.platform()
                if 'Windows' in now_os:
                    dir_name = key.split('\\')[-1]
                    os.mkdir(f'processed/{dir_name}')
                else:
                    dir_name = key.split('/')[-1]
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

    def get_data(self):
        files_dict = self.get_files()
        keys = files_dict.keys()
        for files in tqdm(keys, desc='nc列表加载'):
            # 每个进程负责一个月的数据处理
            # p = multiprocessing.Process(target=self.multiprocess_accelerate, args=(files_dict, files))
            p = threading.Thread(target=self.multiprocess_accelerate, args=(files_dict, files))
            p.start()
            # p.join()

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
                                  desc=f'线程{threading.currentThread().name} 正在处理{single_lon}-{single_lat}位置数据'):
                        csv_name = f"{single_lon}-{single_lat}.csv"

                        self.append2csv(_time, data[k], k, files, csv_name)

    def append2csv(self, data_time, data, column_name, subdir, csv_name):
        """
        追加数据到csv文件

        :param data_time: 时间列
        :param data: 数据列
        :param column_name: 列名
        :param subdir: 保存的子文件夹名
        :param csv_name: csv文件名
        :return:
        """
        if self.now_os == 'Windows':
            subdir = subdir.split('\\')[-1]
        else:
            subdir = subdir.split('/')[-1]

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

    def merge_csv_file(self):
        """
        合并数据

        :return:
        """

        nc_file_path = self.get_files()
        keys = nc_file_path.keys()
        dir_name = []
        for key in keys:
            dir_name.append(key.split('\\')[-1])

        for month in dir_name:
            p = os.path.join('processed', month)
            files = os.listdir(p)
            for f in files:
                f_p = os.path.join(p, f)
                con = pd.read_csv(f_p, index_col=0)
                con.to_csv(f'merge_data/{f}', mode='a+', index='time')
        shutil.rmtree('processed')


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
    from multiprocessing import Process

    start = time.time()
    op = OperateFiles()
    p1 = Process(target=op.get_data)
    p1.start()
    p1.join()

    op.merge_csv_file()
    end = time.time()

    print(f'消耗时间{end - start:.2f}s')
