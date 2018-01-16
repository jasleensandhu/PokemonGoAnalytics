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
import urllib

logging.basicConfig(filename='PokemonGoTest.log', filemode='a', format='%(asctime)s, %(name)s %(levelname)s %(message)s',
                    datefmt='%d/%m/%Y %H:%M:%S', level=logging.DEBUG)

iosDict = dict()
androidDict = dict()
androidScreenShotSet = set()
iosScreenShotSet = set()


def store_json(json_data, fileExt):
    with open('PokemonGo_{}.json'.format(fileExt), 'w') as f:
        json.dump(json_data, f, indent=4)


def get_num(x):
    return int(''.join(ele for ele in x if ele.isdigit()))


def get_ios_html_data(HtmlFile, fileDate, filename):
    try:
        f = codecs.open(HtmlFile, 'r', 'utf-8')
        sourceCode = BeautifulSoup(f.read(), "lxml")

        outputFile1 = fileDate + '-' + filename
        # Remaning .html with _html becasue in dict key doesn't contains '.'
        outputFile = outputFile1.replace('.', '_')

        # Find iOS Screen Shots
        try:
            imageTag = sourceCode.find_all("img", {"class" : "portrait"})
            for image in imageTag:
                scrTag = image["src"]
                # print scrTag
                iosScreenShotSet.add(scrTag)
        except:
            logging.error("{} : iOS screenshot tag not present.".format(outputFile))

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

        # Android ScreenShots
        try:
            imageTag = sourceCode.find_all("img", {"class" : "full-screenshot"})
            for image in imageTag:
                scrTag = image["src"]
                # print scrTag
                androidScreenShotSet.add(scrTag)
        except:
            logging.error("{} : Andriod screenshot tag not present.".format(outputFile))

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


def get_screen_shot():
    androidCnt = 0
    iosCnt = 0
    myPath = "D:\PokemonGoScreenShots"
    for url in androidScreenShotSet:
        fullUrl = "http:" + url
        andImageName = 'android_screenshot_{}.jpg'.format(androidCnt)
        urllib.urlretrieve(fullUrl, os.path.join(myPath, andImageName))
        androidCnt += 1
    for url in iosScreenShotSet:
        iosImageName = 'ios_screenshot_{}.jpg'.format(iosCnt)
        urllib.urlretrieve(url, os.path.join(myPath, iosImageName))
        iosCnt += 1


if __name__ == "__main__":
    logging.info("Execution started !!")
    filePath = "D:\pokemon_5378"
    extract_data_from_html(filePath)
    print androidScreenShotSet
    print iosScreenShotSet
    get_screen_shot()
    logging.info("Execution finished !!")


