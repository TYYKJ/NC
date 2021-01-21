import numpy as np
import xarray as xr


class NC:

    @staticmethod
    def load_nc_data(path: str):
        """
        加载nc数据

        :param path: file path
        :return: ds: dataset
        """
        assert path.endswith('nc'), '文件格式必须以 .nc 结尾'
        ds = xr.open_dataset(path)
        return ds

    @staticmethod
    def get_nc_attributes(dataset):
        """
        获取数据集中的属性

        :param dataset: 数据集
        :return:
        """
        attr = list()
        for i in dataset.data_vars:
            attr.append(i)
        return attr

    @staticmethod
    def get_all_lon_lat(dataset):
        """
        获取全球的经纬度数据

        :param dataset: 数据集
        :return: lon, lat: 经度, 纬度
        """
        lon = dataset.longitude.values
        lat = dataset.latitude.values
        return lon, lat

    def get_part_lon_lat(self, dataset, lon_min_max: tuple, lat_min_max: tuple):
        """
        截取部分地区经纬度

        :param dataset: 数据集
        :param lon_min_max: 经度最大最小元组 eg: (120.5, 123.5)
        :param lat_min_max: 纬度最大最小元组 eg: (34.75, 35.5)
        :return: lat_need[list], lon_need[list]
        """
        lon, lat = self.get_all_lon_lat(dataset)
        lon_index = np.where((lon <= lon_min_max[1]) & (lon >= lon_min_max[0]))
        lat_index = np.where((lat <= lat_min_max[1]) & (lat >= lat_min_max[0]))

        lat_need = list()
        lon_need = list()
        for lo_index, la_index in zip(lon_index, lat_index):
            lat_need.append(lat[la_index])
            lon_need.append(lon[lo_index])
        assert lat_need != [] and lon_need != [], '获取到经纬度为空'
        return lon_need, lat_need

    @staticmethod
    def get_time(dataset):
        """
        获取时间信息

        :return:
        """
        return dataset.time.values

    def get_part_data(self, dataset, lon, lat):
        """
        获取部分经纬度下的数据

        :param dataset: 数据集
        :param lon: 单点经度
        :param lat: 单点纬度
        :return:
        """
        # 获取一个文件内所有的变量 (有些文件可能会有多个 比如风 u\v)
        attr = self.get_nc_attributes(dataset)
        try:
            # 移除MAPSTA
            attr.remove('MAPSTA')
            # 获取固定经纬度下的数据
            d = dataset.sel(longitude=lon, latitude=lat, method='nearest')
            # 因为可能有很多属性,所以用字典保存,key是属性,value是值
            need_data = dict()
            for item in attr:
                need_data[item] = d[item].values
        except ValueError:
            # 获取固定经纬度下的数据
            d = dataset.sel(longitude=lon, latitude=lat, method='nearest')
            # 因为可能有很多属性,所以用字典保存,key是属性,value是值
            need_data = dict()
            for item in attr:
                need_data[item] = d[item].values

        return need_data
