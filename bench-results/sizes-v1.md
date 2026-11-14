matier-bench-server: http  on 127.0.0.1:8080 (5 threads)
matier-bench-server: https on 127.0.0.1:8443
[1/100] matier/h1/plain/closed-c64/GET /bytes/64 req0
    157635 req/s, p99 625 us, 0 errors
[2/100] matier/h1/plain/closed-c256/GET /bytes/64 req0
    157562 req/s, p99 1986 us, 0 errors
[3/100] matier/h2/plain/closed-c64/GET /bytes/64 req0
    254053 req/s, p99 451 us, 0 errors
[4/100] matier/h2/plain/closed-c256/GET /bytes/64 req0
    413330 req/s, p99 1293 us, 0 errors
[5/100] hyper-legacy/h1/plain/closed-c64/GET /bytes/64 req0
    150305 req/s, p99 744 us, 0 errors
[6/100] hyper-legacy/h1/plain/closed-c256/GET /bytes/64 req0
    152771 req/s, p99 3138 us, 0 errors
[7/100] hyper-legacy/h2/plain/closed-c64/GET /bytes/64 req0
    84849 req/s, p99 1174 us, 0 errors
[8/100] hyper-legacy/h2/plain/closed-c256/GET /bytes/64 req0
    87632 req/s, p99 3746 us, 0 errors
[9/100] hyper-pooled(pool=4)/h1/plain/closed-c64/GET /bytes/64 req0
    152968 req/s, p99 688 us, 0 errors
[10/100] hyper-pooled(pool=4)/h1/plain/closed-c256/GET /bytes/64 req0
    152817 req/s, p99 2273 us, 0 errors
[11/100] hyper-pooled(pool=4)/h2/plain/closed-c64/GET /bytes/64 req0
    172851 req/s, p99 698 us, 0 errors
[12/100] hyper-pooled(pool=4)/h2/plain/closed-c256/GET /bytes/64 req0
    202466 req/s, p99 2335 us, 0 errors
[13/100] fetch/h1/plain/closed-c64/GET /bytes/64 req0
    119738 req/s, p99 1023 us, 0 errors
[14/100] fetch/h1/plain/closed-c256/GET /bytes/64 req0
    121783 req/s, p99 4895 us, 0 errors
[15/100] fetch/h2/plain/closed-c64/GET /bytes/64 req0
    68172 req/s, p99 1380 us, 0 errors
[16/100] fetch/h2/plain/closed-c256/GET /bytes/64 req0
    68188 req/s, p99 4383 us, 0 errors
[17/100] fetch-pooled(pool=4)/h1/plain/closed-c64/GET /bytes/64 req0
    130452 req/s, p99 937 us, 0 errors
[18/100] fetch-pooled(pool=4)/h1/plain/closed-c256/GET /bytes/64 req0
    136832 req/s, p99 4284 us, 0 errors
[19/100] fetch-pooled(pool=4)/h2/plain/closed-c64/GET /bytes/64 req0
    119359 req/s, p99 1000 us, 0 errors
[20/100] fetch-pooled(pool=4)/h2/plain/closed-c256/GET /bytes/64 req0
    130179 req/s, p99 3582 us, 0 errors
[21/100] matier/h1/plain/closed-c64/POST /echo req64
    157906 req/s, p99 629 us, 0 errors
[22/100] matier/h1/plain/closed-c256/POST /echo req64
    157262 req/s, p99 1976 us, 0 errors
[23/100] matier/h2/plain/closed-c64/POST /echo req64
    262504 req/s, p99 465 us, 0 errors
[24/100] matier/h2/plain/closed-c256/POST /echo req64
    394771 req/s, p99 1359 us, 0 errors
[25/100] hyper-legacy/h1/plain/closed-c64/POST /echo req64
    147833 req/s, p99 773 us, 0 errors
[26/100] hyper-legacy/h1/plain/closed-c256/POST /echo req64
    149643 req/s, p99 4538 us, 0 errors
[27/100] hyper-legacy/h2/plain/closed-c64/POST /echo req64
    57579 req/s, p99 1704 us, 0 errors
[28/100] hyper-legacy/h2/plain/closed-c256/POST /echo req64
    57399 req/s, p99 5800 us, 0 errors
[29/100] hyper-pooled(pool=4)/h1/plain/closed-c64/POST /echo req64
    153202 req/s, p99 699 us, 0 errors
[30/100] hyper-pooled(pool=4)/h1/plain/closed-c256/POST /echo req64
    155146 req/s, p99 2755 us, 0 errors
[31/100] hyper-pooled(pool=4)/h2/plain/closed-c64/POST /echo req64
    129649 req/s, p99 946 us, 0 errors
[32/100] hyper-pooled(pool=4)/h2/plain/closed-c256/POST /echo req64
    149384 req/s, p99 3492 us, 0 errors
[33/100] fetch/h1/plain/closed-c64/POST /echo req64
    98826 req/s, p99 1552 us, 0 errors
[34/100] fetch/h1/plain/closed-c256/POST /echo req64
    108680 req/s, p99 5734 us, 0 errors
[35/100] fetch/h2/plain/closed-c64/POST /echo req64
    48971 req/s, p99 1901 us, 0 errors
[36/100] fetch/h2/plain/closed-c256/POST /echo req64
    48943 req/s, p99 6054 us, 0 errors
[37/100] fetch-pooled(pool=4)/h1/plain/closed-c64/POST /echo req64
    125692 req/s, p99 973 us, 0 errors
[38/100] fetch-pooled(pool=4)/h1/plain/closed-c256/POST /echo req64
    130425 req/s, p99 4567 us, 0 errors
[39/100] fetch-pooled(pool=4)/h2/plain/closed-c64/POST /echo req64
    95833 req/s, p99 1257 us, 0 errors
[40/100] fetch-pooled(pool=4)/h2/plain/closed-c256/POST /echo req64
    106528 req/s, p99 4559 us, 0 errors
[41/100] matier/h1/plain/closed-c8/GET /bytes/1048576 req0
    10720 req/s, p99 1107 us, 0 errors
[42/100] matier/h1/plain/closed-c8/GET /bytes/8388608 req0
    1197 req/s, p99 9249 us, 0 errors
[43/100] matier/h1/plain/closed-c32/GET /bytes/1048576 req0
    10621 req/s, p99 3500 us, 0 errors
[44/100] matier/h1/plain/closed-c32/GET /bytes/8388608 req0
    981 req/s, p99 35422 us, 0 errors
[45/100] matier/h2/plain/closed-c8/GET /bytes/1048576 req0
    9635 req/s, p99 1261 us, 0 errors
[46/100] matier/h2/plain/closed-c8/GET /bytes/8388608 req0
    1152 req/s, p99 8196 us, 0 errors
[47/100] matier/h2/plain/closed-c32/GET /bytes/1048576 req0
    8131 req/s, p99 5161 us, 0 errors
[48/100] matier/h2/plain/closed-c32/GET /bytes/8388608 req0
    973 req/s, p99 36962 us, 0 errors
[49/100] hyper-legacy/h1/plain/closed-c8/GET /bytes/1048576 req0
    10417 req/s, p99 1141 us, 0 errors
[50/100] hyper-legacy/h1/plain/closed-c8/GET /bytes/8388608 req0
    1163 req/s, p99 9249 us, 0 errors
[51/100] hyper-legacy/h1/plain/closed-c32/GET /bytes/1048576 req0
    10312 req/s, p99 3525 us, 0 errors
[52/100] hyper-legacy/h1/plain/closed-c32/GET /bytes/8388608 req0
    946 req/s, p99 37552 us, 0 errors
[53/100] hyper-legacy/h2/plain/closed-c8/GET /bytes/1048576 req0
    6603 req/s, p99 2175 us, 0 errors
[54/100] hyper-legacy/h2/plain/closed-c8/GET /bytes/8388608 req0
    801 req/s, p99 11870 us, 0 errors
[55/100] hyper-legacy/h2/plain/closed-c32/GET /bytes/1048576 req0
    6732 req/s, p99 7959 us, 0 errors
[56/100] hyper-legacy/h2/plain/closed-c32/GET /bytes/8388608 req0
    800 req/s, p99 46957 us, 0 errors
[57/100] hyper-pooled(pool=4)/h1/plain/closed-c8/GET /bytes/1048576 req0
    10401 req/s, p99 1151 us, 0 errors
[58/100] hyper-pooled(pool=4)/h1/plain/closed-c8/GET /bytes/8388608 req0
    1165 req/s, p99 9175 us, 0 errors
[59/100] hyper-pooled(pool=4)/h1/plain/closed-c32/GET /bytes/1048576 req0
    10134 req/s, p99 3703 us, 0 errors
[60/100] hyper-pooled(pool=4)/h1/plain/closed-c32/GET /bytes/8388608 req0
    947 req/s, p99 36372 us, 0 errors
[61/100] hyper-pooled(pool=4)/h2/plain/closed-c8/GET /bytes/1048576 req0
    9587 req/s, p99 1304 us, 0 errors
[62/100] hyper-pooled(pool=4)/h2/plain/closed-c8/GET /bytes/8388608 req0
    1192 req/s, p99 7987 us, 0 errors
[63/100] hyper-pooled(pool=4)/h2/plain/closed-c32/GET /bytes/1048576 req0
    7524 req/s, p99 6787 us, 0 errors
[64/100] hyper-pooled(pool=4)/h2/plain/closed-c32/GET /bytes/8388608 req0
    908 req/s, p99 44663 us, 0 errors
[65/100] fetch/h1/plain/closed-c8/GET /bytes/1048576 req0
    10550 req/s, p99 1127 us, 0 errors
[66/100] fetch/h1/plain/closed-c8/GET /bytes/8388608 req0
    1177 req/s, p99 9101 us, 0 errors
[67/100] fetch/h1/plain/closed-c32/GET /bytes/1048576 req0
    10424 req/s, p99 3490 us, 0 errors
[68/100] fetch/h1/plain/closed-c32/GET /bytes/8388608 req0
    931 req/s, p99 37716 us, 0 errors
[69/100] fetch/h2/plain/closed-c8/GET /bytes/1048576 req0
    2649 req/s, p99 4682 us, 0 errors
[70/100] fetch/h2/plain/closed-c8/GET /bytes/8388608 req0
    323 req/s, p99 28033 us, 0 errors
[71/100] fetch/h2/plain/closed-c32/GET /bytes/1048576 req0
    2555 req/s, p99 18678 us, 0 errors
[72/100] fetch/h2/plain/closed-c32/GET /bytes/8388608 req0
    319 req/s, p99 124256 us, 0 errors
[73/100] fetch-pooled(pool=4)/h1/plain/closed-c8/GET /bytes/1048576 req0
    10485 req/s, p99 1129 us, 0 errors
[74/100] fetch-pooled(pool=4)/h1/plain/closed-c8/GET /bytes/8388608 req0
    1140 req/s, p99 10551 us, 0 errors
[75/100] fetch-pooled(pool=4)/h1/plain/closed-c32/GET /bytes/1048576 req0
    9753 req/s, p99 8016 us, 0 errors
[76/100] fetch-pooled(pool=4)/h1/plain/closed-c32/GET /bytes/8388608 req0
    912 req/s, p99 54100 us, 0 errors
[77/100] fetch-pooled(pool=4)/h2/plain/closed-c8/GET /bytes/1048576 req0
    4249 req/s, p99 3262 us, 0 errors
[78/100] fetch-pooled(pool=4)/h2/plain/closed-c8/GET /bytes/8388608 req0
    545 req/s, p99 17056 us, 0 errors
[79/100] fetch-pooled(pool=4)/h2/plain/closed-c32/GET /bytes/1048576 req0
    3862 req/s, p99 16605 us, 0 errors
[80/100] fetch-pooled(pool=4)/h2/plain/closed-c32/GET /bytes/8388608 req0
    473 req/s, p99 100467 us, 0 errors
[81/100] matier/h1/plain/closed-c16/POST /echo req1048576
    11194 req/s, p99 1823 us, 0 errors
[82/100] matier/h1/plain/closed-c16/POST /echo req8388608
    1051 req/s, p99 17613 us, 0 errors
[83/100] matier/h2/plain/closed-c16/POST /echo req1048576
    9557 req/s, p99 2208 us, 0 errors
[84/100] matier/h2/plain/closed-c16/POST /echo req8388608
    1146 req/s, p99 15663 us, 0 errors
[85/100] hyper-legacy/h1/plain/closed-c16/POST /echo req1048576
    11197 req/s, p99 1830 us, 0 errors
[86/100] hyper-legacy/h1/plain/closed-c16/POST /echo req8388608
    1056 req/s, p99 17613 us, 0 errors
[87/100] hyper-legacy/h2/plain/closed-c16/POST /echo req1048576
    6311 req/s, p99 4391 us, 0 errors
[88/100] hyper-legacy/h2/plain/closed-c16/POST /echo req8388608
    763 req/s, p99 25051 us, 0 errors
[89/100] hyper-pooled(pool=4)/h1/plain/closed-c16/POST /echo req1048576
    11100 req/s, p99 1852 us, 0 errors
[90/100] hyper-pooled(pool=4)/h1/plain/closed-c16/POST /echo req8388608
    1058 req/s, p99 17367 us, 0 errors
[91/100] hyper-pooled(pool=4)/h2/plain/closed-c16/POST /echo req1048576
    8681 req/s, p99 2331 us, 0 errors
[92/100] hyper-pooled(pool=4)/h2/plain/closed-c16/POST /echo req8388608
    1067 req/s, p99 16728 us, 0 errors
[93/100] fetch/h1/plain/closed-c16/POST /echo req1048576
    11156 req/s, p99 1876 us, 0 errors
[94/100] fetch/h1/plain/closed-c16/POST /echo req8388608
    1052 req/s, p99 21185 us, 0 errors
[95/100] fetch/h2/plain/closed-c16/POST /echo req1048576
    6207 req/s, p99 4526 us, 0 errors
[96/100] fetch/h2/plain/closed-c16/POST /echo req8388608
    748 req/s, p99 26018 us, 0 errors
[97/100] fetch-pooled(pool=4)/h1/plain/closed-c16/POST /echo req1048576
    11223 req/s, p99 1815 us, 0 errors
[98/100] fetch-pooled(pool=4)/h1/plain/closed-c16/POST /echo req8388608
    1048 req/s, p99 25166 us, 0 errors
[99/100] fetch-pooled(pool=4)/h2/plain/closed-c16/POST /echo req1048576
    8334 req/s, p99 2836 us, 0 errors
[100/100] fetch-pooled(pool=4)/h2/plain/closed-c16/POST /echo req8388608
    1043 req/s, p99 17908 us, 0 errors
| scenario | req/s | p50 us | p99 us | p99.9 us | errs | drops | cpu us/req | allocs/req | rss MB |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| matier/h1/plain/closed-c64/GET /bytes/64 req0 | 157635 | 400 | 625 | 751 | 0 | 0 | 21.4 | 7.0 | 36 |
| matier/h1/plain/closed-c256/GET /bytes/64 req0 | 157562 | 1618 | 1986 | 2253 | 0 | 0 | 22.2 | 7.0 | 120 |
| matier/h2/plain/closed-c64/GET /bytes/64 req0 | 254053 | 248 | 451 | 546 | 0 | 0 | 15.7 | 11.0 | 132 |
| matier/h2/plain/closed-c256/GET /bytes/64 req0 | 413330 | 593 | 1293 | 1562 | 0 | 0 | 11.0 | 11.0 | 136 |
| hyper-legacy/h1/plain/closed-c64/GET /bytes/64 req0 | 150305 | 420 | 744 | 881 | 0 | 0 | 27.5 | 14.0 | 141 |
| hyper-legacy/h1/plain/closed-c256/GET /bytes/64 req0 | 152771 | 1645 | 3138 | 3940 | 0 | 0 | 28.4 | 14.0 | 154 |
| hyper-legacy/h2/plain/closed-c64/GET /bytes/64 req0 | 84849 | 747 | 1174 | 1354 | 0 | 0 | 37.0 | 14.0 | 157 |
| hyper-legacy/h2/plain/closed-c256/GET /bytes/64 req0 | 87632 | 2920 | 3746 | 4135 | 0 | 0 | 38.0 | 14.0 | 160 |
| hyper-pooled(pool=4)/h1/plain/closed-c64/GET /bytes/64 req0 | 152968 | 412 | 688 | 834 | 0 | 0 | 26.9 | 14.0 | 160 |
| hyper-pooled(pool=4)/h1/plain/closed-c256/GET /bytes/64 req0 | 152817 | 1663 | 2273 | 2718 | 0 | 0 | 27.8 | 14.0 | 165 |
| hyper-pooled(pool=4)/h2/plain/closed-c64/GET /bytes/64 req0 | 172851 | 361 | 698 | 840 | 0 | 0 | 23.0 | 14.0 | 165 |
| hyper-pooled(pool=4)/h2/plain/closed-c256/GET /bytes/64 req0 | 202466 | 1229 | 2335 | 2863 | 0 | 0 | 19.5 | 14.0 | 171 |
| fetch/h1/plain/closed-c64/GET /bytes/64 req0 | 119738 | 525 | 1023 | 1226 | 0 | 0 | 37.3 | 43.0 | 176 |
| fetch/h1/plain/closed-c256/GET /bytes/64 req0 | 121783 | 2033 | 4895 | 6111 | 0 | 0 | 37.9 | 43.0 | 191 |
| fetch/h2/plain/closed-c64/GET /bytes/64 req0 | 68172 | 933 | 1380 | 1569 | 0 | 0 | 49.4 | 46.0 | 191 |
| fetch/h2/plain/closed-c256/GET /bytes/64 req0 | 68188 | 3762 | 4383 | 4665 | 0 | 0 | 49.5 | 46.0 | 194 |
| fetch-pooled(pool=4)/h1/plain/closed-c64/GET /bytes/64 req0 | 130452 | 481 | 937 | 1127 | 0 | 0 | 35.3 | 43.0 | 194 |
| fetch-pooled(pool=4)/h1/plain/closed-c256/GET /bytes/64 req0 | 136832 | 1802 | 4284 | 5329 | 0 | 0 | 35.4 | 43.0 | 197 |
| fetch-pooled(pool=4)/h2/plain/closed-c64/GET /bytes/64 req0 | 119359 | 529 | 1000 | 1174 | 0 | 0 | 35.8 | 46.0 | 197 |
| fetch-pooled(pool=4)/h2/plain/closed-c256/GET /bytes/64 req0 | 130179 | 1933 | 3582 | 4207 | 0 | 0 | 33.1 | 46.0 | 197 |
| matier/h1/plain/closed-c64/POST /echo req64 | 157906 | 400 | 629 | 767 | 0 | 0 | 21.9 | 8.0 | 197 |
| matier/h1/plain/closed-c256/POST /echo req64 | 157262 | 1622 | 1976 | 2200 | 0 | 0 | 22.6 | 8.0 | 201 |
| matier/h2/plain/closed-c64/POST /echo req64 | 262504 | 237 | 465 | 560 | 0 | 0 | 15.1 | 11.0 | 201 |
| matier/h2/plain/closed-c256/POST /echo req64 | 394771 | 621 | 1359 | 1628 | 0 | 0 | 11.5 | 11.0 | 201 |
| hyper-legacy/h1/plain/closed-c64/POST /echo req64 | 147833 | 426 | 773 | 921 | 0 | 0 | 27.9 | 16.0 | 201 |
| hyper-legacy/h1/plain/closed-c256/POST /echo req64 | 149643 | 1572 | 4538 | 13312 | 0 | 0 | 23.5 | 16.0 | 203 |
| hyper-legacy/h2/plain/closed-c64/POST /echo req64 | 57579 | 1102 | 1704 | 1943 | 0 | 0 | 55.4 | 20.8 | 203 |
| hyper-legacy/h2/plain/closed-c256/POST /echo req64 | 57399 | 4477 | 5800 | 9912 | 0 | 0 | 54.8 | 20.9 | 203 |
| hyper-pooled(pool=4)/h1/plain/closed-c64/POST /echo req64 | 153202 | 411 | 699 | 855 | 0 | 0 | 27.0 | 16.0 | 203 |
| hyper-pooled(pool=4)/h1/plain/closed-c256/POST /echo req64 | 155146 | 1618 | 2755 | 5333 | 0 | 0 | 26.2 | 16.0 | 203 |
| hyper-pooled(pool=4)/h2/plain/closed-c64/POST /echo req64 | 129649 | 477 | 946 | 1165 | 0 | 0 | 29.3 | 21.0 | 203 |
| hyper-pooled(pool=4)/h2/plain/closed-c256/POST /echo req64 | 149384 | 1635 | 3492 | 7746 | 0 | 0 | 24.7 | 21.0 | 203 |
| fetch/h1/plain/closed-c64/POST /echo req64 | 98826 | 613 | 1552 | 2724 | 0 | 0 | 39.4 | 46.0 | 203 |
| fetch/h1/plain/closed-c256/POST /echo req64 | 108680 | 2238 | 5734 | 7406 | 0 | 0 | 40.3 | 46.0 | 203 |
| fetch/h2/plain/closed-c64/POST /echo req64 | 48971 | 1299 | 1901 | 2187 | 0 | 0 | 67.7 | 51.8 | 203 |
| fetch/h2/plain/closed-c256/POST /echo req64 | 48943 | 5235 | 6054 | 6423 | 0 | 0 | 66.8 | 51.8 | 217 |
| fetch-pooled(pool=4)/h1/plain/closed-c64/POST /echo req64 | 125692 | 499 | 973 | 1175 | 0 | 0 | 36.6 | 46.0 | 217 |
| fetch-pooled(pool=4)/h1/plain/closed-c256/POST /echo req64 | 130425 | 1873 | 4567 | 6173 | 0 | 0 | 37.0 | 46.0 | 217 |
| fetch-pooled(pool=4)/h2/plain/closed-c64/POST /echo req64 | 95833 | 649 | 1257 | 1496 | 0 | 0 | 44.3 | 52.0 | 217 |
| fetch-pooled(pool=4)/h2/plain/closed-c256/POST /echo req64 | 106528 | 2335 | 4559 | 5382 | 0 | 0 | 40.1 | 52.0 | 217 |
| matier/h1/plain/closed-c8/GET /bytes/1048576 req0 | 10720 | 749 | 1107 | 1222 | 0 | 0 | 175.1 | 7.0 | 217 |
| matier/h1/plain/closed-c8/GET /bytes/8388608 req0 | 1197 | 6697 | 9249 | 10740 | 0 | 0 | 1511.0 | 7.0 | 217 |
| matier/h1/plain/closed-c32/GET /bytes/1048576 req0 | 10621 | 2992 | 3500 | 3684 | 0 | 0 | 180.3 | 7.0 | 217 |
| matier/h1/plain/closed-c32/GET /bytes/8388608 req0 | 981 | 32686 | 35422 | 37028 | 0 | 0 | 1718.4 | 7.0 | 217 |
| matier/h2/plain/closed-c8/GET /bytes/1048576 req0 | 9635 | 810 | 1261 | 1435 | 0 | 0 | 209.5 | 14.0 | 217 |
| matier/h2/plain/closed-c8/GET /bytes/8388608 req0 | 1152 | 6889 | 8196 | 8577 | 0 | 0 | 1720.5 | 43.0 | 217 |
| matier/h2/plain/closed-c32/GET /bytes/1048576 req0 | 8131 | 3940 | 5161 | 5607 | 0 | 0 | 234.2 | 14.0 | 217 |
| matier/h2/plain/closed-c32/GET /bytes/8388608 req0 | 973 | 32965 | 36962 | 38142 | 0 | 0 | 1901.1 | 43.0 | 217 |
| hyper-legacy/h1/plain/closed-c8/GET /bytes/1048576 req0 | 10417 | 766 | 1141 | 1253 | 0 | 0 | 229.4 | 15.3 | 217 |
| hyper-legacy/h1/plain/closed-c8/GET /bytes/8388608 req0 | 1163 | 6889 | 9249 | 10994 | 0 | 0 | 1734.0 | 14.7 | 217 |
| hyper-legacy/h1/plain/closed-c32/GET /bytes/1048576 req0 | 10312 | 3095 | 3525 | 3787 | 0 | 0 | 243.4 | 15.3 | 217 |
| hyper-legacy/h1/plain/closed-c32/GET /bytes/8388608 req0 | 946 | 33751 | 37552 | 38994 | 0 | 0 | 1920.0 | 14.6 | 217 |
| hyper-legacy/h2/plain/closed-c8/GET /bytes/1048576 req0 | 6603 | 1225 | 2175 | 2349 | 0 | 0 | 170.4 | 26.0 | 217 |
| hyper-legacy/h2/plain/closed-c8/GET /bytes/8388608 req0 | 801 | 10019 | 11870 | 12509 | 0 | 0 | 1191.4 | 110.0 | 217 |
| hyper-legacy/h2/plain/closed-c32/GET /bytes/1048576 req0 | 6732 | 4719 | 7959 | 9224 | 0 | 0 | 173.0 | 26.1 | 217 |
| hyper-legacy/h2/plain/closed-c32/GET /bytes/8388608 req0 | 800 | 39944 | 46957 | 48759 | 0 | 0 | 1236.3 | 115.4 | 217 |
| hyper-pooled(pool=4)/h1/plain/closed-c8/GET /bytes/1048576 req0 | 10401 | 765 | 1151 | 1272 | 0 | 0 | 231.6 | 15.3 | 217 |
| hyper-pooled(pool=4)/h1/plain/closed-c8/GET /bytes/8388608 req0 | 1165 | 6861 | 9175 | 10355 | 0 | 0 | 1736.8 | 14.9 | 217 |
| hyper-pooled(pool=4)/h1/plain/closed-c32/GET /bytes/1048576 req0 | 10134 | 3144 | 3703 | 4164 | 0 | 0 | 247.6 | 15.3 | 217 |
| hyper-pooled(pool=4)/h1/plain/closed-c32/GET /bytes/8388608 req0 | 947 | 33849 | 36372 | 37585 | 0 | 0 | 1918.2 | 14.5 | 217 |
| hyper-pooled(pool=4)/h2/plain/closed-c8/GET /bytes/1048576 req0 | 9587 | 817 | 1304 | 1450 | 0 | 0 | 212.4 | 26.0 | 217 |
| hyper-pooled(pool=4)/h2/plain/closed-c8/GET /bytes/8388608 req0 | 1192 | 6681 | 7987 | 8569 | 0 | 0 | 1573.7 | 110.0 | 217 |
| hyper-pooled(pool=4)/h2/plain/closed-c32/GET /bytes/1048576 req0 | 7524 | 4157 | 6787 | 7324 | 0 | 0 | 247.8 | 26.0 | 217 |
| hyper-pooled(pool=4)/h2/plain/closed-c32/GET /bytes/8388608 req0 | 908 | 35127 | 44663 | 46072 | 0 | 0 | 1933.5 | 110.0 | 217 |
| fetch/h1/plain/closed-c8/GET /bytes/1048576 req0 | 10550 | 755 | 1127 | 1230 | 0 | 0 | 235.8 | 51.8 | 217 |
| fetch/h1/plain/closed-c8/GET /bytes/8388608 req0 | 1177 | 6808 | 9101 | 10346 | 0 | 0 | 1752.9 | 113.2 | 217 |
| fetch/h1/plain/closed-c32/GET /bytes/1048576 req0 | 10424 | 3064 | 3490 | 3713 | 0 | 0 | 250.2 | 50.4 | 217 |
| fetch/h1/plain/closed-c32/GET /bytes/8388608 req0 | 931 | 34406 | 37716 | 38797 | 0 | 0 | 2007.6 | 124.4 | 217 |
| fetch/h2/plain/closed-c8/GET /bytes/1048576 req0 | 2649 | 3023 | 4682 | 5501 | 0 | 0 | 720.8 | 301.1 | 217 |
| fetch/h2/plain/closed-c8/GET /bytes/8388608 req0 | 323 | 24822 | 28033 | 29737 | 0 | 0 | 6221.9 | 2098.5 | 217 |
| fetch/h2/plain/closed-c32/GET /bytes/1048576 req0 | 2555 | 12018 | 18678 | 22299 | 0 | 0 | 804.2 | 302.8 | 217 |
| fetch/h2/plain/closed-c32/GET /bytes/8388608 req0 | 319 | 99484 | 124256 | 147456 | 0 | 0 | 6275.0 | 2100.7 | 217 |
| fetch-pooled(pool=4)/h1/plain/closed-c8/GET /bytes/1048576 req0 | 10485 | 760 | 1129 | 1234 | 0 | 0 | 238.9 | 51.8 | 217 |
| fetch-pooled(pool=4)/h1/plain/closed-c8/GET /bytes/8388608 req0 | 1140 | 6930 | 10551 | 34013 | 0 | 0 | 1717.3 | 101.5 | 217 |
| fetch-pooled(pool=4)/h1/plain/closed-c32/GET /bytes/1048576 req0 | 9753 | 3158 | 8016 | 18186 | 0 | 0 | 204.6 | 48.1 | 217 |
| fetch-pooled(pool=4)/h1/plain/closed-c32/GET /bytes/8388608 req0 | 912 | 34996 | 54100 | 73466 | 0 | 0 | 1861.5 | 107.3 | 217 |
| fetch-pooled(pool=4)/h2/plain/closed-c8/GET /bytes/1048576 req0 | 4249 | 1829 | 3262 | 5439 | 0 | 0 | 544.6 | 301.0 | 217 |
| fetch-pooled(pool=4)/h2/plain/closed-c8/GET /bytes/8388608 req0 | 545 | 14631 | 17056 | 19726 | 0 | 0 | 4395.5 | 2093.1 | 217 |
| fetch-pooled(pool=4)/h2/plain/closed-c32/GET /bytes/1048576 req0 | 3862 | 8167 | 16605 | 21119 | 0 | 0 | 700.2 | 302.1 | 217 |
| fetch-pooled(pool=4)/h2/plain/closed-c32/GET /bytes/8388608 req0 | 473 | 66978 | 100467 | 116326 | 0 | 0 | 5887.4 | 2094.9 | 217 |
| matier/h1/plain/closed-c16/POST /echo req1048576 | 11194 | 1433 | 1823 | 1995 | 0 | 0 | 222.6 | 8.0 | 217 |
| matier/h1/plain/closed-c16/POST /echo req8388608 | 1051 | 15229 | 17613 | 19481 | 0 | 0 | 1939.4 | 8.0 | 217 |
| matier/h2/plain/closed-c16/POST /echo req1048576 | 9557 | 1663 | 2208 | 2646 | 0 | 0 | 233.1 | 11.0 | 217 |
| matier/h2/plain/closed-c16/POST /echo req8388608 | 1146 | 13926 | 15663 | 16040 | 0 | 0 | 1989.8 | 11.1 | 217 |
| hyper-legacy/h1/plain/closed-c16/POST /echo req1048576 | 11197 | 1431 | 1830 | 1992 | 0 | 0 | 225.9 | 16.0 | 217 |
| hyper-legacy/h1/plain/closed-c16/POST /echo req8388608 | 1056 | 15180 | 17613 | 18727 | 0 | 0 | 1967.3 | 16.9 | 217 |
| hyper-legacy/h2/plain/closed-c16/POST /echo req1048576 | 6311 | 2480 | 4391 | 4841 | 0 | 0 | 210.2 | 20.8 | 217 |
| hyper-legacy/h2/plain/closed-c16/POST /echo req8388608 | 763 | 20906 | 25051 | 26214 | 0 | 0 | 1550.9 | 21.0 | 217 |
| hyper-pooled(pool=4)/h1/plain/closed-c16/POST /echo req1048576 | 11100 | 1441 | 1852 | 2171 | 0 | 0 | 229.0 | 16.0 | 217 |
| hyper-pooled(pool=4)/h1/plain/closed-c16/POST /echo req8388608 | 1058 | 15131 | 17367 | 18186 | 0 | 0 | 1944.0 | 16.1 | 217 |
| hyper-pooled(pool=4)/h2/plain/closed-c16/POST /echo req1048576 | 8681 | 1827 | 2331 | 2556 | 0 | 0 | 254.1 | 21.0 | 217 |
| hyper-pooled(pool=4)/h2/plain/closed-c16/POST /echo req8388608 | 1067 | 14967 | 16728 | 17433 | 0 | 0 | 1877.6 | 21.0 | 217 |
| fetch/h1/plain/closed-c16/POST /echo req1048576 | 11156 | 1435 | 1876 | 2097 | 0 | 0 | 235.2 | 46.0 | 217 |
| fetch/h1/plain/closed-c16/POST /echo req8388608 | 1052 | 15393 | 21185 | 22921 | 0 | 0 | 2076.4 | 46.8 | 217 |
| fetch/h2/plain/closed-c16/POST /echo req1048576 | 6207 | 2527 | 4526 | 5112 | 0 | 0 | 225.8 | 51.8 | 217 |
| fetch/h2/plain/closed-c16/POST /echo req8388608 | 748 | 21430 | 26018 | 29442 | 0 | 0 | 1594.4 | 52.0 | 217 |
| fetch-pooled(pool=4)/h1/plain/closed-c16/POST /echo req1048576 | 11223 | 1426 | 1815 | 2007 | 0 | 0 | 233.3 | 46.0 | 217 |
| fetch-pooled(pool=4)/h1/plain/closed-c16/POST /echo req8388608 | 1048 | 15278 | 25166 | 40567 | 0 | 0 | 2061.6 | 46.0 | 217 |
| fetch-pooled(pool=4)/h2/plain/closed-c16/POST /echo req1048576 | 8334 | 1884 | 2836 | 4796 | 0 | 0 | 260.6 | 52.0 | 217 |
| fetch-pooled(pool=4)/h2/plain/closed-c16/POST /echo req8388608 | 1043 | 15311 | 17908 | 18645 | 0 | 0 | 1959.8 | 52.0 | 217 |

