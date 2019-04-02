# -*- coding: utf-8 -*-
"""
Created on Wed Feb 27 16:27:49 2019

@author: Sonia

Problem 1:
    Consider the following experiment. 
    You roll 4 fair (6-sided) dice and then roll 3 fair (6-sided) dice. 
    Estimate the probability that the sum of the numbers showing on the first set of (four) dice is greater the sum of the numbers showing on the second set of (three)dice?
"""

from __future__ import print_function
from operator import add
import findspark
import random

findspark.init()

import pyspark
import numpy as np

sc = pyspark.SparkContext(appName="sonia_HW1_Q1")

def rolls(x):
    set1 = np.random.randint(low = 1, high = 7, size = 4)
    set2 = np.random.randint(low = 1, high = 7, size = 3)    
    if sum(set1) > sum(set2):
        success = 1
    else:
        success = 0
    return success  

data = sc.parallelize(range(100000))
success = data.map(rolls)

probability = float(success.sum())/100000

print ("The probability that the set of four dice has a greater sum than that of the three dice is about", np.round(probability*100,1),"%")