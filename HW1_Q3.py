# -*- coding: utf-8 -*-
"""
Created on Wed Feb 27 16:36:55 2019

@author: Sonia

Problem 3:
    The baby names data set has a deficiency: Each name appears multiple times in the file since the data is broken up by ethnicity. 
    If we don't care about the Ethnicity data, can we use Spark to remove this repitition?
    What is the most common baby name for Boys? 
    What percentage of all Boys had this name?
    What is the most common baby name for Girls? 
    What percentage of all Girls had this name?
    How many names were given to both boys and girls? 
    What name is the most gender-neutral? 
        (That is, the name whose granting was most equally distributed between boys and girls.)
"""

from __future__ import print_function
from operator import add
import findspark
import random

findspark.init()

import pyspark
import numpy as np
sc = pyspark.SparkContext(appName="sonia_HW1_Q3")

names = sc.textFile("Popular_Baby_Names.csv").map(lambda line: line.split(","))
#Subset data to look at gender, name, and count columns    
names = names.map(lambda x: (x[1], x[3],x[4]))

#Create functions to check for male and female 
def male_check(tup):
    if tup[0] == "MALE":
        return True
    else:
        return False
    
def female_check(tup):
    if tup[0] == "FEMALE":
        return True
    else:
        return False

#Creating separate datasets for males and females
males = names.filter(male_check).map(lambda x: (x[1], x[2]))
females = names.filter(female_check).map(lambda x: (x[1], x[2]))

#Change counts to integers
male_count = males.map(lambda x: (x[0],float(x[1])))
female_count = females.map(lambda x: (x[0],float(x[1])))

#Removing duplicate names by adding counts
total_males = male_count.combineByKey((lambda x: (x,1)), (lambda x, y: (x[0] + y, x[1] + 1)),(lambda x, y: (x[0] + y[0], x[1] + y[1]))).map(lambda x: (x[0], x[1][0]))
total_females = female_count.combineByKey((lambda x: (x,1)), (lambda x, y: (x[0] + y, x[1] + 1)),(lambda x, y: (x[0] + y[0], x[1] + y[1]))).map(lambda x: (x[0], x[1][0]))

#Getting the 20 most common names by sorting counts in descending order
max_males = total_males.takeOrdered(20,key=lambda x: -x[1])
max_females = total_females.takeOrdered(20,key=lambda x: -x[1])

#Calculating the total number of males and females
all_males = total_males.map(lambda x: x[1]).reduce(lambda x,y: x+y)
all_females = total_females.map(lambda x: x[1]).reduce(lambda x,y: x+y)


print("The most common male name:", max_males[0][0])
print(round(max_males[0][1]/all_males *100, 2), "% of males had this name")
print("The most common female name:", max_females[0][0])
print(round(max_females[0][1]/all_females *100, 2), "% of females had this name")

print("Total number of male names:", total_males.count())
print("Total number of female names:", total_females.count())

#Combining male and female data sets to find most gender neutral name
neutral = total_males.join(total_females)
neutral = neutral.map(lambda x: (x[0], x[1][0]/x[1][1]))
#Calculating the 20 most gender neutral names by sorting counts in ascending order
neutral = neutral.takeOrdered(20,key=lambda x: x[1])

print("The most gender neutral name is:", neutral[0][0])

