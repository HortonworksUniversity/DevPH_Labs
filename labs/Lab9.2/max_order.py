#!/usr/bin/python
import fileinput
import sys
## step through stdin or a file useful for testing
## in hive the script will get stdin
for line in fileinput.input():
    ## get rid of newline
    line=line.strip()
    ## emit first line
    if (fileinput.isfirstline()):
        print line
        ## grab the first column of first line and store as userid
        w = line.split("\t")
        userid=w[0]
    ## split line into list on tabs    
    w = line.split("\t")
    ## compare this line to first line
    ## if they are different we have a new userid and we can emit
    ## if userid is the same then we ignore
    if userid != w[0]:
        print line
        ## when a new userid is encountered we reset the userid to the new value
        userid=w[0]

