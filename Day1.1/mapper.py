#!/usr/bin/python3
import sys
post = 0
date = 0
for x in sys.stdin:
	words = x.split(",")
	if words[9].isdigit() and post < int(words[9]):
		post = int(words[9])
		date = words[2]
print("Funniest post on date",date,"Total hahas:",post);
		
