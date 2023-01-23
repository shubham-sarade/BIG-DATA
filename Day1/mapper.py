#!/usr/bin/python3
import sys
for x in sys.stdin:
	words = x.split(",")
	if words[1] == 'video' and '2017' in words[2] and words[2].startswith('2'):
		print("Shares", words[5])
