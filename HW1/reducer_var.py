
#!/usr/bin/env python3

import sys

for line in sys.stdin:
        cj, mj, vj = [float(el) for el in line.split()]
        break

for line in sys.stdin:
        ck, mk, vk = [float(el) for el in line.split()]
        cjk = cj + ck
        vi = ((cj*vj + ck*vk) / cjk) + cj * ck * ((mj - mk)/cjk)**2
        cj, mj, vj = ck, mk, vi

print(vj)
