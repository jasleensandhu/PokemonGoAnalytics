#******************************************************************************************
#       1.	Sagar Chhadia (Computer Science) sagarmahesh.chhadia@mavs.uta.edu
#       2.	Nikita Dhawle (Information Systems) nikita.dhawle@mavs.uta.edu
#       3.	Jasleen Kaur Sandhu (Computer Science) jasleenkaur.sandhu@mavs.uta.edu
#******************************************************************************************

from bs4 import BeautifulSoup
import codecs
import fnmatch
import os
import json
import pickle
from pymongo import MongoClient
import logging
import pandas as pd
import datetime
import matplotlib.pyplot as plt
from pandas.tools.plotting import scatter_matrix
import seaborn as sns
from sklearn.linear_model import LinearRegression
import numpy as np
from sklearn.model_selection import train_test_split

logging.basicConfig(filename='PokemonGo.log', filemode='a', format='%(asctime)s, %(name)s %(levelname)s %(message)s',
                    datefmt='%d/%m/%Y %H:%M:%S', level=logging.DEBUG)


filePath = "D:\pokemon_5378"
iosDict = dict()
androidDict = dict()
resultDict = dict()
myDict = dict()


# DataBase Connection
class MongoDB():
    def __init__(self):
        client = MongoClient('mongodb://ds151070.mlab.com:51070')
        self.db = client['pokemongodb']
        self.db.authenticate('sagar', 'passw0rd')

    def get_collection(self,collection_name):
        return self.db[collection_name]
logging.info("Database connected.")


class Datetime(object):
    year = 0
    month = 0
    date = 0
    hour = 0
    minute = 0
    second = 0

    def __init__(self, year, month, date, hour, minute, second=0):
        self.year = year
        self.month = month
        self.date = date
        self.hour = hour
        self.minute = minute
        self.second = second

    def __repr__(self):
        return "datetime({},{},{},{},{},{})".format(self.year, self.month, self.date, self.hour, self.minute, self.second)

    def __str__(self):
        return "datetime({},{},{},{},{},{})".format(self.year, self.month, self.date, self.hour, self.minute, self.second)

    def __eq__(self, other):
        if not isinstance(other, Datetime):
            return False
        if (self.year == other.year and self.month == other.month and self.date == other.date and self.hour == other.hour \
                    and self.minute == other.minute and self.second == other.second):
            return True
        return False

    def __hash__(self):
        dateTimeKey = int("{}{}{}{}{}{}".format(self.year, self.month, self.date, self.hour, self.minute, self.second))
        return dateTimeKey


def merge_ios_andriod(iosVal, androidVal, dateTime):
    tempDict = dict()
    for iosSubKey, iosSubVal in iosVal.items():
        tempDict[iosSubKey] = iosSubVal
    for androidSubKey, androidSubVal in androidVal.items():
        tempDict[androidSubKey] = androidSubVal
    # resultDict[dateTime] = tempDict        # To store data in Pickle formate
    resultDict[str(dateTime)] = tempDict     # To store data in Json formate


def get_ios_android_key(jdata_ios, jdata_android):
    for androidKey, androidVal in jdata_android.items():
        yr = androidKey[0:4]
        mon = androidKey[5:7]
        dt = androidKey[8:10]
        hr = androidKey[11:13]
        min = androidKey[14:16]
        datetime = Datetime(yr, mon, dt, hr, min)
        myDict[datetime] = androidVal

    for iosKey, iosVal in jdata_ios.items():
        yr = iosKey[0:4]
        mon = iosKey[5:7]
        dt = iosKey[8:10]
        hr = iosKey[11:13]
        min = iosKey[14:16]
        datetime = Datetime(yr, mon, dt, hr, min)

        if datetime in myDict:
            #print ("Matched -> {0} ".format(iosKey))
            if isinstance(iosVal, dict) and isinstance(myDict[datetime], dict):
                merge_ios_andriod(iosVal, myDict[datetime], datetime)


def get_merged_file():
    json_data_android = open('PokemonGo_android.json', 'r+')
    jdata_android = json.loads(json_data_android.read().decode("utf-8"))

    json_data_ios = open('PokemonGo_ios.json', 'r+')
    jdata_ios = json.loads(json_data_ios.read().decode("utf-8"))

    get_ios_android_key(jdata_ios, jdata_android)
    store_json(resultDict, fileExt="data")
    logging.info("Merged (ios + android) json file created.")
    # store_pickle(resultDict, fileExt="data")
    # logging.info("Merged (ios + android) pickle file created.")


def get_data_frame(resultDict):
    pokemonGoDataFrame = pd.DataFrame(resultDict)
    dfTranspose = pokemonGoDataFrame.transpose()

    # export DataFrame to csv
    dfTranspose.to_csv('PokemonGoDataFrame.csv')
    logging.info("PokemonGoDataFrame.csv file created.")

    # export DataFrame to json
    dfTranspose.reset_index().to_json("PokemonGoDataFrame.json", orient='records', lines=True)
    logging.info("PokemonGoDataFrame.json file created.")

    # export DataFrame to excel
    dfTranspose.to_excel("PokemonGoDataFrame.xlsx", sheet_name='PokemonGoDataFrame')
    logging.info("PokemonGoDataFrame.xlsx file created.")

    # describe() method to find the count/mean/std/min/25%/50%75%/max values for each 11 variables
    print dfTranspose.describe()

    # scatter_matrix() method to find pairs of variables with high correlations (either positive or negative)
    snsPlot = sns.pairplot(dfTranspose)
    snsPlot.savefig('ScatterMatrix.png')

    corcoefList = []
    colValList = []
    dfArray = np.asarray(dfTranspose)

    for i in range(1, len(dfArray[0])):
        col1Name = dfTranspose.columns[i]
        for j in range(i+1, len(dfArray[0])):
            col2Name = dfTranspose.columns[j]
            corcoefVal = np.corrcoef(dfArray[:, i].astype('float64'), dfArray[:, j].astype('float64'))[0][1]
            if corcoefVal > 0.5 or corcoefVal < -0.5:
                corcoefList.append(corcoefVal)
            colValList.append("{} {}".format(col1Name, col2Name))
    print corcoefList
    return dfTranspose


def to_integer(dt_time):
    return 10000*dt_time.year + 1000*dt_time.month + 100*dt_time.day + 10*dt_time.hour + dt_time.minute


def create_model(dataFrame, Y, flag):
    dataFrameCopy = dataFrame.copy()
    srtDateTime = dataFrameCopy.index

    # Convert DateTime fromstring to int/float to fit into the model
    xList = []
    for item in srtDateTime:
        x = item.split(',')
        year = (x[0][-4:])
        month = x[1]
        day = x[2]
        hour = x[3]
        minute = x[4]
        xList.append(to_integer(datetime.datetime(int(year), int(month), int(day), int(hour), int(minute))))
    dataFrameCopy.insert(loc=0, column='DateTime', value=xList)

    # Creating training/testing dataset using sklearn cross validation
    X = dataFrameCopy['DateTime']
    X_train, X_test, y_train, y_test = train_test_split(np.asarray(X).reshape(-1, 1), np.asarray(Y), test_size=0.2)

    # Linear Regression Model
    model = LinearRegression()
    model.fit(X_train.reshape(-1,1), y_train)
    print "{} model score : {}".format(flag, model.score(X_test.reshape(-1,1), y_test))
    print "Predicted value of {} for 2016/11/01 11:50 PM is : {}\n\n".format(flag, model.predict(to_integer(datetime.datetime(2016,11,01,23,50))))


def get_num(x):
    return int(''.join(ele for ele in x if ele.isdigit()))


def upload_data(postData, colName):
    client = MongoDB()
    if colName == 'ios':
        getCollection = client.get_collection('ios')
    elif colName == 'android':
        getCollection = client.get_collection('android')
    try:
        insertDataQuery = getCollection.insert(postData)
    except Exception, e:
        print e


def store_json(json_data, fileExt):
    with open('PokemonGo_{}.json'.format(fileExt), 'w') as f:
        json.dump(json_data, f, indent=4)


def store_pickle(pickle_data, fileExt):
    with open('PokemonGo_{}.pickle'.format(fileExt), 'w') as f:
        pickle.dump(pickle_data, f)


def get_ios_html_data(HtmlFile, fileDate, filename):
    try:
        f = codecs.open(HtmlFile, 'r', 'utf-8')
        sourceCode = BeautifulSoup(f.read(), "lxml")

        outputFile1 = fileDate + '-' + filename
        # Remaning .html with _html becasue in dict key doesn't contains '.'
        outputFile = outputFile1.replace('.', '_')

        # Find iOS Current Ratings and All Ratings
        divRattingTag = sourceCode.find("div", {"class": "extra-list customer-ratings"})
        try:
            divCurrentVersion = divRattingTag.find_all('div')[0].get_text()
            if divCurrentVersion == "Current Version:":
                divCurrentRating = divRattingTag.find_all('div')[1].find_all("span")[-1].get_text()
            else:
                divCurrentRating = "0"
                logging.error("{} : Current version rating tag not present.".format(outputFile))
        except:
            print "No Current Version Tag Found"

        try:
            divAllVersion = divRattingTag.find_all('div')[-3].get_text()
            if divAllVersion == "All Versions:":
                divAllRating = divRattingTag.find_all('div')[-2].find_all("span")[-1].get_text()
            else:
                logging.error("{} : All version rating tag not present.".format(outputFile))
                divAllRating = "0"
        except:
            print "No All Version Tag Found"

        ios_current_ratings = get_num(divCurrentRating)
        ios_all_ratings = get_num(divAllRating)

        # Find iOS File Size
        try:
            ulFileSize = sourceCode.find("ul", {"class": "list"})
            liFileSize = ulFileSize.find_all('li')[-4].get_text()
        except:
            liFileSize = "0"
            logging.error("{} : IOS file size tag not present.".format(outputFile))
        ios_file_size = get_num(liFileSize)

        outputValue = {outputFile: {'ios_current_ratings': ios_current_ratings, 'ios_all_ratings': ios_all_ratings,
                                    'ios_file_size': ios_file_size}}

        iosDict.update(outputValue)
        # upload_data(outputValue, colName = 'ios')
    except Exception, e:
        print("Exception occured in file {} - {}".format(HtmlFile, e))
        logging.error("Exception occured in file {} - {}".format(HtmlFile, e))


def get_android_html_data(HtmlFile, fileDate, filename):
    try:
        f = codecs.open(HtmlFile, 'r', 'utf-8')
        sourceCode = BeautifulSoup(f.read(), "lxml")

        outputFile1 = fileDate + '-' + filename
        # Remaning .html with _html becasue in dict key doesn't contains '.'
        outputFile = outputFile1.replace('.', '_')

        # Android Avg Rating
        try:
            android_avg_rating = float(sourceCode.find("div", {"class": "score"}).get_text())
        except:
            logging.error("{} : Average rating tag not present.".format(outputFile))
            android_avg_rating = 0.0

        # Android Total Rating
        try:
            divTotalRating = sourceCode.find("div", {"class": "reviews-stats"}).get_text()
            android_total_ratings = get_num(divTotalRating)
        except:
            android_total_ratings = 0
            logging.error("{} : Total rating tag not present.".format(outputFile))

        # Android Rating 1 to 5
        try:
            android_ratings = []
            divRatings = sourceCode.find_all("span", {"class": "bar-number"})
            if not divRatings:
                android_ratings = [0, 0, 0, 0, 0]
            for item in reversed(divRatings):
                android_ratings.append(item.get_text().replace(',', ''))
            android_ratings = [item.encode('utf-8') for item in android_ratings]
            android_ratings = list(map(int, android_ratings))
        except:
            android_ratings = [0, 0, 0, 0, 0]
            logging.error("{} : Android ratings tag not present.".format(outputFile))

        # Android File Size
        try:
            divFileSize = sourceCode.find("div", {"itemprop": "fileSize"}).get_text()
            android_file_size = get_num(divFileSize)
        except:
            android_file_size = 0
            logging.error("{} : Andriod file size tag not present.".format(outputFile))

        outputValue = {
            outputFile: {'android_avg_rating': android_avg_rating, 'android_total_ratings': android_total_ratings,
                         'android_ratings_1': android_ratings[0], 'android_ratings_2': android_ratings[1],
                         'android_ratings_3': android_ratings[2], 'android_ratings_4': android_ratings[3],
                         'android_ratings_5': android_ratings[4], 'android_file_size': android_file_size}}

        androidDict.update(outputValue)
        # upload_data(outputValue, colName = 'android')
    except Exception, e:
        print("Exception occured in file {} - {}".format(HtmlFile, e))
        logging.error("Exception occured in file {} - {}".format(HtmlFile, e))


def extract_data_from_html(filePath):
    iosCnt = 0
    androidCnt = 0
    for root, dirnames, filenames in os.walk(filePath):
        for filename in fnmatch.filter(filenames, '*.html'):
            if os.path.splitext(filename)[0][-3:] == 'ios':
                fullFileName =  root + '\\' + filename
                fileDate = root[-10:]
                iosCnt += 1
                print ("iOS file count : {}".format(iosCnt))
                get_ios_html_data(fullFileName, fileDate, filename)
            elif os.path.splitext(filename)[0][-7:] == 'android':
                fullFileName = root + '\\' + filename
                fileDate = root[-10:]
                androidCnt += 1
                print ("Android file count : {}".format(androidCnt))
                get_android_html_data(fullFileName, fileDate, filename)
    store_json(iosDict, fileExt = 'ios')
    logging.info("Json file for ios created")
    store_json(androidDict, fileExt = 'android')
    logging.info("Json file for android created")


if __name__ == "__main__":
    logging.info("Execution started !!")
    extract_data_from_html(filePath)
    get_merged_file()
    dataFrame = get_data_frame(resultDict)
    create_model(dataFrame, dataFrame['android_total_ratings'], flag = "android_total_ratings")
    create_model(dataFrame, dataFrame['ios_all_ratings'], flag = "ios_all_ratings")
    logging.info("Execution finished !!")
