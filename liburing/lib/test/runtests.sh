#!/bin/bash

TESTS="$@"
RET=0

TIMEOUT=30
FAILED=""

do_kmsg="yes"
if ! [ $(id -u) = 0 ]; then
	do_kmsg="no"
fi

for t in $TESTS; do
	if [ "$do_kmsg" = "yes" ]; then
		echo Running test $t | tee > /dev/kmsg
	else
		echo Running test $t
	fi
	timeout --preserve-status -s INT $TIMEOUT ./$t
	r=$?
	if [ "${r}" -eq 124 ]; then
		echo "Test $t timed out (may not be a failure)"
	elif [ "${r}" -ne 0 ]; then
		echo "Test $t failed with ret ${r}"
		FAILED="$FAILED $t"
		RET=1
	fi
done

if [ "${RET}" -ne 0 ]; then
	echo "Tests $FAILED failed"
	exit $RET
else
	echo "All tests passed"
	exit 0
fi
