#!/bin/sh

shopt -s expand_aliases

if hash gtac 2>/dev/null; then
    alias tac="gtac"
fi

if hash gsort 2>/dev/null; then
    alias sort="gsort"
fi

java -Dcoverage-outputDir=target -jar jmockit-coverage-1.14.jar target/coverage.ser
tac target/index.html | grep "Line segments" | head -1 | sed 's/.*Line segments: //g' | sed "s/'.*$//g" | awk 'BEGIN {FS = "/"} {print "Line Covered Percentage: "$1/$2*100"%"}'