# -*- coding: utf-8 -*-
"""
Created on Wed Feb 27 16:36:00 2019

@author: Sonia

Problem 2:
    Consider the following game. 
    You repeatedly shuffle a deck of cards and reveal the top card of the (newly shuffled) deck. 
    You continue this process until you reveal either the Ace of Spades or two Face cards in a row. 
    When the game ends you win $1 for every card that you revealed.
    If this game costs $10 to play, how much to do you expect to win by playing this game?
    If you are going to play the game a single time and you want to be assured that you have at least a 60% chance of coming out ahead, (roughly) how much would you bewilling to pay to play this game?
"""

from __future__ import print_function
from operator import add
import findspark
import random

findspark.init()

import pyspark
import numpy as np

sc = pyspark.SparkContext(appName="sonia_HW1_Q2")
            
def win(x):
    l = []
    count = 0
    #size of deck
    while len(l) < 52:
        a = count
        b = count-1
        #number of card
        number = random.randint(1, 13)
        #Suit
        suit = random.choice(['H', 'C', 'D', 'S'])
        #Combining number and suit
        card = (number, suit)
        #lloking at card rondomly chosen
        if card not in l:
            #Find Ace of Spades
            if "S" in card[1] and card[0] == 1: 
                break
            #Find two cards in a row with face values
            for a in l:
                if a[0] >10:
                    for b in l:
                        if b[0] >10:
                            break
            #increase counter
            count +=1
    #Return number of cards flipped before winning
    return count


#Calculate money won from playing 1000 times    
games =  sc.parallelize(range(1000))        
cards = games.map(win)

#Subtract cost of game and calculate average amount won
wins = cards.map(lambda x: x-10)
total_wins = wins.reduce(lambda x,y: x+y)
avg_wins = total_wins/1000

print("You could expect to win $", np.round(avg_wins,2))

#calculate max price to play to come out ahead 60% of the time
max_pay = cards.reduce(lambda x,y: x+y)
avg_pay = max_pay/1000 *.6

print("To have a 60% chance of coming out ahead, you should not pay more than $", np.round(avg_pay,2))
