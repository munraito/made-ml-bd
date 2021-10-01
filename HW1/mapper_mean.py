#!/usr/bin/env python3

import sys

cum_sum, count = 0, 0
for line in sys.stdin:
	cum_sum += float(line)
	count += 1
chunk_mean = cum_sum / count

print(count, chunk_mean)
