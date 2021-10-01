#!/usr/bin/env python3

import sys

cum_sum = 0
count = 0
chunk_prices = []
for line in sys.stdin:
	price = float(line)
	cum_sum += price
	count += 1
	chunk_prices.append(price)
chunk_mean = cum_sum / count
chunk_var = sum([(cp - chunk_mean) ** 2 for cp in chunk_prices]) / count

print(count, chunk_mean, chunk_var)