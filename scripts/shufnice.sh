#!/bin/sh

while true; do
  PID=`pgrep $1`
  TIDS=`ls /proc/$PID/task`
  TID=`echo $TIDS |  tr " " "\n" | shuf -n1`
  NICE=$((`shuf -i 0-39 -n 1` - 20))
  echo "renicing $TID to $NICE"
  renice -n $NICE -p $TID
done
