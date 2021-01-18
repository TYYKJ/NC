import csv
import time
import pandas as pd
import os


def getCsv(title, values, path):
    filepath = path.replace('.csv', '(1).csv')
    dataFrame = pd.DataFrame(columns=title)
    for i in range(len(values)):
        # 采用.loc的方法进行
        dataFrame.loc[i] = values[i]  # 其中loc[]中需要加入的是插入地方dataframe的索引，默认是整数型
        # 也可采用诸如df.loc['a'] = ['123',30]的形式
    dataFrame.to_csv(filepath, index=False)


# 匹配在后的时间
def getTimeString2(datetime):
    # 将其转换为时间数组
    timeArray = time.strptime(datetime, "%Y-%m-%d %H:%M")
    # 转换为时间戳
    timeStamp = int(time.mktime(timeArray))
    timeInt = timeStamp + 10 * 60

    timeArrayLocal = time.localtime(timeInt)
    timeString = time.strftime("%Y-%m-%d %H:%M", timeArrayLocal)
    return timeString


# 匹配在前的时间
def getTimeString(datetime):
    # 将其转换为时间数组
    timeArray = time.strptime(datetime, "%Y-%m-%d %H:%M")
    # 转换为时间戳
    timeStamp = int(time.mktime(timeArray))

    timeArrayLocal = time.localtime(timeStamp)
    timeString = time.strftime("%Y-%m-%d %H:%M", timeArrayLocal)
    return timeString


# 路径，可修改
path1 = r'E:\Desktop桌面\未命名文件夹\模式-在前'
path2 = r'E:\Desktop桌面\未命名文件夹\实测-在后'

# 路径匹配字典
filePathPair = {}
filespath1 = os.listdir(path1)
filespath2 = os.listdir(path2)
for file1 in filespath1:
    filepath_1 = os.path.join(path1, file1)
    for file2 in filespath2:
        if file1[0:4] in file2:
            filePathPair[filepath_1] = os.path.join(path2, file2)
print(filePathPair)

# 循环所有字典
for key in filePathPair.keys():
    # 在前的路径
    filePath1 = key
    # 在后的路径
    filePath2 = filePathPair[key]
    print(filePath1 + ':' + filePath2)

    rows1 = []
    rows2 = []
    with open(filePath1, encoding="utf-8") as f:
        reader = csv.reader(f)
        rows1 = [row for row in reader]

    with open(filePath2, encoding="utf-8") as f:
        reader = csv.reader(f)
        rows2 = [row for row in reader]

    valuelist = []
    titleList = rows1[0] + rows2[0][5:]
    for i in range(1, len(rows1)):
        value = []
        for j in range(1, len(rows2)):
            timeCurrent = rows2[j][0] + '-' + rows2[j][1] + '-' + rows2[j][2] + ' ' + rows2[j][3] + ':' + rows2[j][4]
            string1 = getTimeString(rows1[i][1])
            string2 = getTimeString2(timeCurrent)
            # 时间匹配
            if string1 == string2:
                value = rows1[i] + rows2[j][5:]
                # print(value)
                valuelist.append(value)
                break
            # 没有匹配的时间
            if j == len(rows2) - 1:
                value = rows1[i]
                for k in range(0, len(rows2[0]) - 5):
                    value = value + ['']
                print(value)
                valuelist.append(value)
    getCsv(titleList, valuelist, filePath1)
