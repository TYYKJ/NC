#!/usr/bin/env python
# -*- coding: UTF-8 -*-

"""

@Project ：pycharm
@File ：generateNC.py
@Date ：2021-06-16 17:54

"""
import numpy as np
import xarray as xr
import time


class NC:

    @staticmethod
    def load_nc_data(path: str):
        """
        加载nc数据

        :param path: file path
        :return: ds: dataset
        """
        assert path.endswith('nc'), 'file format must be .nc'
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
    def get_time(dataset):
        """
        获取时间信息

        :return:
        """
        return dataset.time.values

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


def getOceanArea(mask):
    """获取所有属于海洋的经纬度索引列表

    Args:
        mask ([ndarray]): 海洋标识符，数值为1是海洋

    Returns:
        latSum[list]: 纬度索引列表
        lonSum[list]: 经度索引列表
    """
    # 获取海洋的所有索引
    index = np.where(mask == 1)
    latSum = list(set(index[0]))
    lonSum = list(set(index[1]))
    # 列表排序
    latSum.sort()
    lonSum.sort()
    return latSum, lonSum


def getLonLat(lat, lon, latSum, lonSum):
    """获取所有的海洋经纬度

    Args:
        lat ([ndarray]): 所有纬度
        lon ([ndarray]): 所有经度
        latSum ([list]): 纬度索引
        lonSum ([list]): 经度索引

    Returns:
        lat[ndarray]: 海洋纬度
        lon[ndarray]: 海洋经度
    """
    lon = lon[lonSum]
    lat = lat[latSum]
    return lat, lon


def getValue(value):
    """获取所有的在海洋经纬度的数据

    Args:
        value ([dic]): 所有数据生成的字典

    Returns:
        valueUpdate[dic]: 海洋领域内的数据字典
    """
    valueUpdate = {}
    for key in value.keys():
        uwindArrayFirst = value[key][:, latSum]
        uwindArray = uwindArrayFirst[:, :, lonSum]

        uwindArray[np.isnan(uwindArray)] = 0
        valueUpdate[key] = uwindArray
    return valueUpdate


if __name__ == '__main__':
    originalNCPath = r'H:\D资料\workspace\python\
        pycharm\NC-master\ncTriangle\ww3.200901_wnd.nc'
    newNCPath = r'H:\D资料\workspace\python\pycharm\NC-master\ncTriangle\wnd.nc'
    start = time.time()
    print(time.time())
    nc = NC()
    dataSet = nc.load_nc_data(originalNCPath)
    lon, lat = nc.get_all_lon_lat(dataSet)
    timeValue = nc.get_time(dataSet)
    print(nc.get_nc_attributes(dataSet))
    print(time.time() - start)
    value = {}
    for i in range(1, len(nc.get_nc_attributes(dataSet))):
        key = nc.get_nc_attributes(dataSet)[i]
        value[key] = dataSet.data_vars[key].values

    mask = dataSet.data_vars[nc.get_nc_attributes(dataSet)[0]].values
    latSum, lonSum = getOceanArea(mask)
    lat, lon = getLonLat(lat, lon, latSum, lonSum)
    print(time.time() - start)
    maskValueFirst = mask[latSum]
    maskValue = maskValueFirst[:,  lonSum]
    print(time.time() - start)
    valueArray = getValue(value)
    print(time.time() - start)
    ds = xr.Dataset(data_vars=dict(MASPATA=(['latitude', 'longitude'],
                                            maskValue),
                                   uwnd=(['time', 'latitude', 'longitude'],
                                         valueArray['uwnd']),
                                   vwnd=(['time', 'latitude', 'longitude'],
                                         valueArray['vwnd'])),
                    coords=dict(longitude=lon,
                                latitude=lat,
                                time=timeValue), )
    ds.to_netcdf(newNCPath)
