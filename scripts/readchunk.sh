#!/bin/sh

bs=100000
infile=$1
skip=$2
length=$3

(
  dd bs=1 skip=$skip count=0
  dd bs=$bs count=$(($length / $bs))
  dd bs=$(($length % $bs)) count=1
) < "$infile"
