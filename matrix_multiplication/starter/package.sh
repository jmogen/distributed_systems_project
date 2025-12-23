#!/bin/bash

# Package matrix multiplication project for distribution
# Creates tarball with all necessary files

FNAME=a1.tar.gz

tar -czf $FNAME *.cpp *.hpp

echo Your tarball file name is: $FNAME
echo 
echo It contains the following files:
echo 
tar -tf $FNAME

echo
echo Done.
echo
