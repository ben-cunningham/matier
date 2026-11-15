matier-bench-server: http  on 127.0.0.1:8080 (5 threads)
matier-bench-server: https on 127.0.0.1:8443
[1/100] matier/h1/plain/closed-c64/GET /bytes/64 req0
    157986 req/s, p99 645 us, 0 errors
[2/100] matier/h1/plain/closed-c256/GET /bytes/64 req0
    157173 req/s, p99 1983 us, 0 errors
[3/100] matier/h2/plain/closed-c64/GET /bytes/64 req0
    240619 req/s, p99 473 us, 0 errors
[4/100] matier/h2/plain/closed-c256/GET /bytes/64 req0
    473741 req/s, p99 1087 us, 0 errors
[5/100] hyper-legacy/h1/plain/closed-c64/GET /bytes/64 req0
    150067 req/s, p99 745 us, 0 errors
[6/100] hyper-legacy/h1/plain/closed-c256/GET /bytes/64 req0
    150604 req/s, p99 3244 us, 0 errors
[7/100] hyper-legacy/h2/plain/closed-c64/GET /bytes/64 req0
    85622 req/s, p99 1165 us, 0 errors
[8/100] hyper-legacy/h2/plain/closed-c256/GET /bytes/64 req0
    91892 req/s, p99 3654 us, 0 errors
[9/100] hyper-pooled(pool=4)/h1/plain/closed-c64/GET /bytes/64 req0
    153148 req/s, p99 690 us, 0 errors
[10/100] hyper-pooled(pool=4)/h1/plain/closed-c256/GET /bytes/64 req0
    151169 req/s, p99 2275 us, 0 errors
[11/100] hyper-pooled(pool=4)/h2/plain/closed-c64/GET /bytes/64 req0
    172271 req/s, p99 701 us, 0 errors
[12/100] hyper-pooled(pool=4)/h2/plain/closed-c256/GET /bytes/64 req0
    201298 req/s, p99 2355 us, 0 errors
[13/100] fetch/h1/plain/closed-c64/GET /bytes/64 req0
    119867 req/s, p99 1029 us, 0 errors
[14/100] fetch/h1/plain/closed-c256/GET /bytes/64 req0
    122066 req/s, p99 4891 us, 0 errors
[15/100] fetch/h2/plain/closed-c64/GET /bytes/64 req0
    68162 req/s, p99 1383 us, 0 errors
[16/100] fetch/h2/plain/closed-c256/GET /bytes/64 req0
    67737 req/s, p99 4407 us, 0 errors
[17/100] fetch-pooled(pool=4)/h1/plain/closed-c64/GET /bytes/64 req0
    130238 req/s, p99 932 us, 0 errors
[18/100] fetch-pooled(pool=4)/h1/plain/closed-c256/GET /bytes/64 req0
    137372 req/s, p99 4231 us, 0 errors
[19/100] fetch-pooled(pool=4)/h2/plain/closed-c64/GET /bytes/64 req0
    118408 req/s, p99 1013 us, 0 errors
[20/100] fetch-pooled(pool=4)/h2/plain/closed-c256/GET /bytes/64 req0
    130423 req/s, p99 3551 us, 0 errors
[21/100] matier/h1/plain/closed-c64/POST /echo req64
    156930 req/s, p99 637 us, 0 errors
[22/100] matier/h1/plain/closed-c256/POST /echo req64
    155558 req/s, p99 2025 us, 0 errors
[23/100] matier/h2/plain/closed-c64/POST /echo req64
    245938 req/s, p99 477 us, 0 errors
[24/100] matier/h2/plain/closed-c256/POST /echo req64
    426482 req/s, p99 1213 us, 0 errors
[25/100] hyper-legacy/h1/plain/closed-c64/POST /echo req64
    148138 req/s, p99 762 us, 0 errors
[26/100] hyper-legacy/h1/plain/closed-c256/POST /echo req64
    150428 req/s, p99 3617 us, 0 errors
[27/100] hyper-legacy/h2/plain/closed-c64/POST /echo req64
    57251 req/s, p99 1715 us, 0 errors
[28/100] hyper-legacy/h2/plain/closed-c256/POST /echo req64
    58031 req/s, p99 5382 us, 0 errors
[29/100] hyper-pooled(pool=4)/h1/plain/closed-c64/POST /echo req64
    150435 req/s, p99 715 us, 0 errors
[30/100] hyper-pooled(pool=4)/h1/plain/closed-c256/POST /echo req64
    151223 req/s, p99 2372 us, 0 errors
[31/100] hyper-pooled(pool=4)/h2/plain/closed-c64/POST /echo req64
    128960 req/s, p99 929 us, 0 errors
[32/100] hyper-pooled(pool=4)/h2/plain/closed-c256/POST /echo req64
    156274 req/s, p99 3095 us, 0 errors
[33/100] fetch/h1/plain/closed-c64/POST /echo req64
    114803 req/s, p99 1080 us, 0 errors
[34/100] fetch/h1/plain/closed-c256/POST /echo req64
    117397 req/s, p99 5145 us, 0 errors
[35/100] fetch/h2/plain/closed-c64/POST /echo req64
    49503 req/s, p99 1895 us, 0 errors
[36/100] fetch/h2/plain/closed-c256/POST /echo req64
    48877 req/s, p99 6169 us, 0 errors
[37/100] fetch-pooled(pool=4)/h1/plain/closed-c64/POST /echo req64
    121819 req/s, p99 1008 us, 0 errors
[38/100] fetch-pooled(pool=4)/h1/plain/closed-c256/POST /echo req64
    129021 req/s, p99 4596 us, 0 errors
[39/100] fetch-pooled(pool=4)/h2/plain/closed-c64/POST /echo req64
    96721 req/s, p99 1249 us, 0 errors
[40/100] fetch-pooled(pool=4)/h2/plain/closed-c256/POST /echo req64
    107352 req/s, p99 4604 us, 0 errors
[41/100] matier/h1/plain/closed-c8/GET /bytes/1048576 req0
    11193 req/s, p99 1060 us, 0 errors
[42/100] matier/h1/plain/closed-c8/GET /bytes/8388608 req0
    1276 req/s, p99 8577 us, 0 errors
[43/100] matier/h1/plain/closed-c32/GET /bytes/1048576 req0
    11349 req/s, p99 3265 us, 0 errors
[44/100] matier/h1/plain/closed-c32/GET /bytes/8388608 req0
    994 req/s, p99 35062 us, 0 errors
[45/100] matier/h2/plain/closed-c8/GET /bytes/1048576 req0
    10112 req/s, p99 1230 us, 0 errors
[46/100] matier/h2/plain/closed-c8/GET /bytes/8388608 req0
    1181 req/s, p99 8131 us, 0 errors
[47/100] matier/h2/plain/closed-c32/GET /bytes/1048576 req0
    7879 req/s, p99 5464 us, 0 errors
[48/100] matier/h2/plain/closed-c32/GET /bytes/8388608 req0
    997 req/s, p99 37716 us, 0 errors
[49/100] hyper-legacy/h1/plain/closed-c8/GET /bytes/1048576 req0
    10855 req/s, p99 1096 us, 0 errors
[50/100] hyper-legacy/h1/plain/closed-c8/GET /bytes/8388608 req0
    1231 req/s, p99 8970 us, 0 errors
[51/100] hyper-legacy/h1/plain/closed-c32/GET /bytes/1048576 req0
    10714 req/s, p99 3346 us, 0 errors
[52/100] hyper-legacy/h1/plain/closed-c32/GET /bytes/8388608 req0
    970 req/s, p99 35652 us, 0 errors
[53/100] hyper-legacy/h2/plain/closed-c8/GET /bytes/1048576 req0
    6665 req/s, p99 2136 us, 0 errors
[54/100] hyper-legacy/h2/plain/closed-c8/GET /bytes/8388608 req0
    798 req/s, p99 11846 us, 0 errors
[55/100] hyper-legacy/h2/plain/closed-c32/GET /bytes/1048576 req0
    6802 req/s, p99 7782 us, 0 errors
[56/100] hyper-legacy/h2/plain/closed-c32/GET /bytes/8388608 req0
    805 req/s, p99 46465 us, 0 errors
[57/100] hyper-pooled(pool=4)/h1/plain/closed-c8/GET /bytes/1048576 req0
    10805 req/s, p99 1094 us, 0 errors
[58/100] hyper-pooled(pool=4)/h1/plain/closed-c8/GET /bytes/8388608 req0
    1224 req/s, p99 8667 us, 0 errors
[59/100] hyper-pooled(pool=4)/h1/plain/closed-c32/GET /bytes/1048576 req0
    10687 req/s, p99 3344 us, 0 errors
[60/100] hyper-pooled(pool=4)/h1/plain/closed-c32/GET /bytes/8388608 req0
    972 req/s, p99 35783 us, 0 errors
[61/100] hyper-pooled(pool=4)/h2/plain/closed-c8/GET /bytes/1048576 req0
    9986 req/s, p99 1253 us, 0 errors
[62/100] hyper-pooled(pool=4)/h2/plain/closed-c8/GET /bytes/8388608 req0
    1241 req/s, p99 7700 us, 0 errors
[63/100] hyper-pooled(pool=4)/h2/plain/closed-c32/GET /bytes/1048576 req0
    7795 req/s, p99 6484 us, 0 errors
[64/100] hyper-pooled(pool=4)/h2/plain/closed-c32/GET /bytes/8388608 req0
    939 req/s, p99 43483 us, 0 errors
[65/100] fetch/h1/plain/closed-c8/GET /bytes/1048576 req0
    10747 req/s, p99 1099 us, 0 errors
[66/100] fetch/h1/plain/closed-c8/GET /bytes/8388608 req0
    1227 req/s, p99 8700 us, 0 errors
[67/100] fetch/h1/plain/closed-c32/GET /bytes/1048576 req0
    10755 req/s, p99 3326 us, 0 errors
[68/100] fetch/h1/plain/closed-c32/GET /bytes/8388608 req0
    972 req/s, p99 35684 us, 0 errors
[69/100] fetch/h2/plain/closed-c8/GET /bytes/1048576 req0
    2676 req/s, p99 4706 us, 0 errors
[70/100] fetch/h2/plain/closed-c8/GET /bytes/8388608 req0
    323 req/s, p99 27853 us, 0 errors
[71/100] fetch/h2/plain/closed-c32/GET /bytes/1048576 req0
    2571 req/s, p99 18465 us, 0 errors
[72/100] fetch/h2/plain/closed-c32/GET /bytes/8388608 req0
    324 req/s, p99 121831 us, 0 errors
[73/100] fetch-pooled(pool=4)/h1/plain/closed-c8/GET /bytes/1048576 req0
    10741 req/s, p99 1104 us, 0 errors
[74/100] fetch-pooled(pool=4)/h1/plain/closed-c8/GET /bytes/8388608 req0
    1229 req/s, p99 8987 us, 0 errors
[75/100] fetch-pooled(pool=4)/h1/plain/closed-c32/GET /bytes/1048576 req0
    10765 req/s, p99 3346 us, 0 errors
[76/100] fetch-pooled(pool=4)/h1/plain/closed-c32/GET /bytes/8388608 req0
    976 req/s, p99 35160 us, 0 errors
[77/100] fetch-pooled(pool=4)/h2/plain/closed-c8/GET /bytes/1048576 req0
    4743 req/s, p99 2697 us, 0 errors
[78/100] fetch-pooled(pool=4)/h2/plain/closed-c8/GET /bytes/8388608 req0
    572 req/s, p99 16179 us, 0 errors
[79/100] fetch-pooled(pool=4)/h2/plain/closed-c32/GET /bytes/1048576 req0
    4022 req/s, p99 17531 us, 0 errors
[80/100] fetch-pooled(pool=4)/h2/plain/closed-c32/GET /bytes/8388608 req0
    519 req/s, p99 76481 us, 0 errors
[81/100] matier/h1/plain/closed-c16/POST /echo req1048576
    11516 req/s, p99 1749 us, 0 errors
[82/100] matier/h1/plain/closed-c16/POST /echo req8388608
    1108 req/s, p99 16458 us, 0 errors
[83/100] matier/h2/plain/closed-c16/POST /echo req1048576
    9993 req/s, p99 2122 us, 0 errors
[84/100] matier/h2/plain/closed-c16/POST /echo req8388608
    1173 req/s, p99 15401 us, 0 errors
[85/100] hyper-legacy/h1/plain/closed-c16/POST /echo req1048576
    11578 req/s, p99 1750 us, 0 errors
[86/100] hyper-legacy/h1/plain/closed-c16/POST /echo req8388608
    1100 req/s, p99 16712 us, 0 errors
[87/100] hyper-legacy/h2/plain/closed-c16/POST /echo req1048576
    6564 req/s, p99 4284 us, 0 errors
[88/100] hyper-legacy/h2/plain/closed-c16/POST /echo req8388608
    811 req/s, p99 24248 us, 0 errors
[89/100] hyper-pooled(pool=4)/h1/plain/closed-c16/POST /echo req1048576
    11550 req/s, p99 1724 us, 0 errors
[90/100] hyper-pooled(pool=4)/h1/plain/closed-c16/POST /echo req8388608
    1102 req/s, p99 16400 us, 0 errors
[91/100] hyper-pooled(pool=4)/h2/plain/closed-c16/POST /echo req1048576
    9042 req/s, p99 2210 us, 0 errors
[92/100] hyper-pooled(pool=4)/h2/plain/closed-c16/POST /echo req8388608
    1094 req/s, p99 16433 us, 0 errors
[93/100] fetch/h1/plain/closed-c16/POST /echo req1048576
    11457 req/s, p99 1796 us, 0 errors
[94/100] fetch/h1/plain/closed-c16/POST /echo req8388608
    1091 req/s, p99 19792 us, 0 errors
[95/100] fetch/h2/plain/closed-c16/POST /echo req1048576
    6497 req/s, p99 4342 us, 0 errors
[96/100] fetch/h2/plain/closed-c16/POST /echo req8388608
    801 req/s, p99 24265 us, 0 errors
[97/100] fetch-pooled(pool=4)/h1/plain/closed-c16/POST /echo req1048576
    11438 req/s, p99 1769 us, 0 errors
[98/100] fetch-pooled(pool=4)/h1/plain/closed-c16/POST /echo req8388608
    1080 req/s, p99 19628 us, 0 errors
[99/100] fetch-pooled(pool=4)/h2/plain/closed-c16/POST /echo req1048576
    9177 req/s, p99 2259 us, 0 errors
[100/100] fetch-pooled(pool=4)/h2/plain/closed-c16/POST /echo req8388608
    1101 req/s, p99 16269 us, 0 errors
| scenario | req/s | p50 us | p99 us | p99.9 us | errs | drops | cpu us/req | allocs/req | rss MB |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| matier/h1/plain/closed-c64/GET /bytes/64 req0 | 157986 | 398 | 645 | 823 | 0 | 0 | 20.8 | 2.0 | 37 |
| matier/h1/plain/closed-c256/GET /bytes/64 req0 | 157173 | 1622 | 1983 | 2195 | 0 | 0 | 22.6 | 2.0 | 125 |
| matier/h2/plain/closed-c64/GET /bytes/64 req0 | 240619 | 261 | 473 | 561 | 0 | 0 | 9.2 | 4.0 | 131 |
| matier/h2/plain/closed-c256/GET /bytes/64 req0 | 473741 | 524 | 1087 | 1293 | 0 | 0 | 8.7 | 4.0 | 141 |
| hyper-legacy/h1/plain/closed-c64/GET /bytes/64 req0 | 150067 | 421 | 745 | 888 | 0 | 0 | 27.5 | 14.0 | 145 |
| hyper-legacy/h1/plain/closed-c256/GET /bytes/64 req0 | 150604 | 1665 | 3244 | 4051 | 0 | 0 | 28.9 | 14.0 | 157 |
| hyper-legacy/h2/plain/closed-c64/GET /bytes/64 req0 | 85622 | 740 | 1165 | 1349 | 0 | 0 | 36.7 | 14.0 | 159 |
| hyper-legacy/h2/plain/closed-c256/GET /bytes/64 req0 | 91892 | 2779 | 3654 | 4155 | 0 | 0 | 36.6 | 14.0 | 166 |
| hyper-pooled(pool=4)/h1/plain/closed-c64/GET /bytes/64 req0 | 153148 | 412 | 690 | 826 | 0 | 0 | 27.1 | 14.0 | 166 |
| hyper-pooled(pool=4)/h1/plain/closed-c256/GET /bytes/64 req0 | 151169 | 1680 | 2275 | 2644 | 0 | 0 | 28.4 | 14.0 | 167 |
| hyper-pooled(pool=4)/h2/plain/closed-c64/GET /bytes/64 req0 | 172271 | 363 | 701 | 835 | 0 | 0 | 23.0 | 14.0 | 170 |
| hyper-pooled(pool=4)/h2/plain/closed-c256/GET /bytes/64 req0 | 201298 | 1236 | 2355 | 2791 | 0 | 0 | 19.6 | 14.0 | 171 |
| fetch/h1/plain/closed-c64/GET /bytes/64 req0 | 119867 | 525 | 1029 | 1237 | 0 | 0 | 37.3 | 43.0 | 175 |
| fetch/h1/plain/closed-c256/GET /bytes/64 req0 | 122066 | 2034 | 4891 | 6033 | 0 | 0 | 38.0 | 43.0 | 186 |
| fetch/h2/plain/closed-c64/GET /bytes/64 req0 | 68162 | 934 | 1383 | 1582 | 0 | 0 | 49.4 | 46.0 | 187 |
| fetch/h2/plain/closed-c256/GET /bytes/64 req0 | 67737 | 3783 | 4407 | 4678 | 0 | 0 | 49.7 | 46.0 | 190 |
| fetch-pooled(pool=4)/h1/plain/closed-c64/GET /bytes/64 req0 | 130238 | 483 | 932 | 1114 | 0 | 0 | 35.5 | 43.0 | 190 |
| fetch-pooled(pool=4)/h1/plain/closed-c256/GET /bytes/64 req0 | 137372 | 1799 | 4231 | 5247 | 0 | 0 | 35.5 | 43.0 | 193 |
| fetch-pooled(pool=4)/h2/plain/closed-c64/GET /bytes/64 req0 | 118408 | 534 | 1013 | 1191 | 0 | 0 | 36.1 | 46.0 | 194 |
| fetch-pooled(pool=4)/h2/plain/closed-c256/GET /bytes/64 req0 | 130423 | 1928 | 3551 | 4219 | 0 | 0 | 33.0 | 46.0 | 196 |
| matier/h1/plain/closed-c64/POST /echo req64 | 156930 | 402 | 637 | 768 | 0 | 0 | 22.3 | 3.0 | 196 |
| matier/h1/plain/closed-c256/POST /echo req64 | 155558 | 1638 | 2025 | 2286 | 0 | 0 | 23.5 | 3.0 | 203 |
| matier/h2/plain/closed-c64/POST /echo req64 | 245938 | 254 | 477 | 557 | 0 | 0 | 11.9 | 9.0 | 203 |
| matier/h2/plain/closed-c256/POST /echo req64 | 426482 | 582 | 1213 | 1436 | 0 | 0 | 10.1 | 9.0 | 211 |
| hyper-legacy/h1/plain/closed-c64/POST /echo req64 | 148138 | 426 | 762 | 911 | 0 | 0 | 28.0 | 16.0 | 211 |
| hyper-legacy/h1/plain/closed-c256/POST /echo req64 | 150428 | 1651 | 3617 | 4522 | 0 | 0 | 29.0 | 16.0 | 212 |
| hyper-legacy/h2/plain/closed-c64/POST /echo req64 | 57251 | 1108 | 1715 | 1978 | 0 | 0 | 55.4 | 20.8 | 212 |
| hyper-legacy/h2/plain/closed-c256/POST /echo req64 | 58031 | 4415 | 5382 | 5968 | 0 | 0 | 55.2 | 20.8 | 212 |
| hyper-pooled(pool=4)/h1/plain/closed-c64/POST /echo req64 | 150435 | 419 | 715 | 854 | 0 | 0 | 27.8 | 16.0 | 212 |
| hyper-pooled(pool=4)/h1/plain/closed-c256/POST /echo req64 | 151223 | 1677 | 2372 | 3119 | 0 | 0 | 28.5 | 16.0 | 212 |
| hyper-pooled(pool=4)/h2/plain/closed-c64/POST /echo req64 | 128960 | 481 | 929 | 1112 | 0 | 0 | 30.7 | 21.0 | 212 |
| hyper-pooled(pool=4)/h2/plain/closed-c256/POST /echo req64 | 156274 | 1584 | 3095 | 3707 | 0 | 0 | 25.2 | 21.0 | 212 |
| fetch/h1/plain/closed-c64/POST /echo req64 | 114803 | 547 | 1080 | 1307 | 0 | 0 | 39.3 | 46.0 | 212 |
| fetch/h1/plain/closed-c256/POST /echo req64 | 117397 | 2101 | 5145 | 6476 | 0 | 0 | 39.7 | 46.0 | 213 |
| fetch/h2/plain/closed-c64/POST /echo req64 | 49503 | 1284 | 1895 | 2179 | 0 | 0 | 66.8 | 51.8 | 213 |
| fetch/h2/plain/closed-c256/POST /echo req64 | 48877 | 5243 | 6169 | 6554 | 0 | 0 | 67.9 | 51.8 | 213 |
| fetch-pooled(pool=4)/h1/plain/closed-c64/POST /echo req64 | 121819 | 515 | 1008 | 1207 | 0 | 0 | 38.1 | 46.0 | 213 |
| fetch-pooled(pool=4)/h1/plain/closed-c256/POST /echo req64 | 129021 | 1906 | 4596 | 5759 | 0 | 0 | 37.5 | 46.0 | 218 |
| fetch-pooled(pool=4)/h2/plain/closed-c64/POST /echo req64 | 96721 | 643 | 1249 | 1484 | 0 | 0 | 43.3 | 52.0 | 218 |
| fetch-pooled(pool=4)/h2/plain/closed-c256/POST /echo req64 | 107352 | 2308 | 4604 | 5562 | 0 | 0 | 39.6 | 52.0 | 220 |
| matier/h1/plain/closed-c8/GET /bytes/1048576 req0 | 11193 | 718 | 1060 | 1153 | 0 | 0 | 165.7 | 2.0 | 220 |
| matier/h1/plain/closed-c8/GET /bytes/8388608 req0 | 1276 | 6283 | 8577 | 9445 | 0 | 0 | 1367.5 | 2.0 | 220 |
| matier/h1/plain/closed-c32/GET /bytes/1048576 req0 | 11349 | 2808 | 3265 | 3437 | 0 | 0 | 167.8 | 2.0 | 220 |
| matier/h1/plain/closed-c32/GET /bytes/8388608 req0 | 994 | 32162 | 35062 | 36438 | 0 | 0 | 1666.2 | 2.0 | 220 |
| matier/h2/plain/closed-c8/GET /bytes/1048576 req0 | 10112 | 771 | 1230 | 1399 | 0 | 0 | 200.0 | 4.0 | 220 |
| matier/h2/plain/closed-c8/GET /bytes/8388608 req0 | 1181 | 6722 | 8131 | 8462 | 0 | 0 | 1656.5 | 4.0 | 220 |
| matier/h2/plain/closed-c32/GET /bytes/1048576 req0 | 7879 | 4106 | 5464 | 5837 | 0 | 0 | 243.9 | 4.0 | 220 |
| matier/h2/plain/closed-c32/GET /bytes/8388608 req0 | 997 | 31752 | 37716 | 39813 | 0 | 0 | 1965.9 | 4.0 | 220 |
| hyper-legacy/h1/plain/closed-c8/GET /bytes/1048576 req0 | 10855 | 736 | 1096 | 1192 | 0 | 0 | 209.5 | 15.3 | 220 |
| hyper-legacy/h1/plain/closed-c8/GET /bytes/8388608 req0 | 1231 | 6496 | 8970 | 10052 | 0 | 0 | 1598.5 | 14.7 | 220 |
| hyper-legacy/h1/plain/closed-c32/GET /bytes/1048576 req0 | 10714 | 2982 | 3346 | 3504 | 0 | 0 | 228.7 | 15.4 | 220 |
| hyper-legacy/h1/plain/closed-c32/GET /bytes/8388608 req0 | 970 | 33014 | 35652 | 37093 | 0 | 0 | 1844.4 | 14.5 | 221 |
| hyper-legacy/h2/plain/closed-c8/GET /bytes/1048576 req0 | 6665 | 1207 | 2136 | 2316 | 0 | 0 | 169.9 | 26.0 | 221 |
| hyper-legacy/h2/plain/closed-c8/GET /bytes/8388608 req0 | 798 | 10093 | 11846 | 12378 | 0 | 0 | 1203.6 | 110.0 | 221 |
| hyper-legacy/h2/plain/closed-c32/GET /bytes/1048576 req0 | 6802 | 4674 | 7782 | 8962 | 0 | 0 | 170.6 | 26.1 | 221 |
| hyper-legacy/h2/plain/closed-c32/GET /bytes/8388608 req0 | 805 | 39748 | 46465 | 47809 | 0 | 0 | 1231.4 | 115.6 | 221 |
| hyper-pooled(pool=4)/h1/plain/closed-c8/GET /bytes/1048576 req0 | 10805 | 739 | 1094 | 1187 | 0 | 0 | 210.6 | 15.3 | 221 |
| hyper-pooled(pool=4)/h1/plain/closed-c8/GET /bytes/8388608 req0 | 1224 | 6529 | 8667 | 9986 | 0 | 0 | 1609.5 | 14.8 | 221 |
| hyper-pooled(pool=4)/h1/plain/closed-c32/GET /bytes/1048576 req0 | 10687 | 2990 | 3344 | 3523 | 0 | 0 | 229.1 | 15.4 | 221 |
| hyper-pooled(pool=4)/h1/plain/closed-c32/GET /bytes/8388608 req0 | 972 | 32981 | 35783 | 36766 | 0 | 0 | 1852.9 | 14.5 | 221 |
| hyper-pooled(pool=4)/h2/plain/closed-c8/GET /bytes/1048576 req0 | 9986 | 783 | 1253 | 1362 | 0 | 0 | 201.5 | 26.0 | 221 |
| hyper-pooled(pool=4)/h2/plain/closed-c8/GET /bytes/8388608 req0 | 1241 | 6406 | 7700 | 8241 | 0 | 0 | 1515.8 | 110.0 | 221 |
| hyper-pooled(pool=4)/h2/plain/closed-c32/GET /bytes/1048576 req0 | 7795 | 4047 | 6484 | 7070 | 0 | 0 | 241.2 | 26.0 | 221 |
| hyper-pooled(pool=4)/h2/plain/closed-c32/GET /bytes/8388608 req0 | 939 | 33686 | 43483 | 44794 | 0 | 0 | 1865.2 | 110.0 | 221 |
| fetch/h1/plain/closed-c8/GET /bytes/1048576 req0 | 10747 | 744 | 1099 | 1201 | 0 | 0 | 219.9 | 51.5 | 221 |
| fetch/h1/plain/closed-c8/GET /bytes/8388608 req0 | 1227 | 6504 | 8700 | 9650 | 0 | 0 | 1642.8 | 108.4 | 221 |
| fetch/h1/plain/closed-c32/GET /bytes/1048576 req0 | 10755 | 2972 | 3326 | 3494 | 0 | 0 | 236.5 | 50.1 | 221 |
| fetch/h1/plain/closed-c32/GET /bytes/8388608 req0 | 972 | 32834 | 35684 | 36372 | 0 | 0 | 1881.1 | 118.8 | 221 |
| fetch/h2/plain/closed-c8/GET /bytes/1048576 req0 | 2676 | 2984 | 4706 | 5423 | 0 | 0 | 689.1 | 301.2 | 222 |
| fetch/h2/plain/closed-c8/GET /bytes/8388608 req0 | 323 | 24773 | 27853 | 28672 | 0 | 0 | 6068.7 | 2101.1 | 222 |
| fetch/h2/plain/closed-c32/GET /bytes/1048576 req0 | 2571 | 11936 | 18465 | 22217 | 0 | 0 | 784.8 | 302.8 | 222 |
| fetch/h2/plain/closed-c32/GET /bytes/8388608 req0 | 324 | 98173 | 121831 | 134742 | 0 | 0 | 6036.0 | 2101.7 | 222 |
| fetch-pooled(pool=4)/h1/plain/closed-c8/GET /bytes/1048576 req0 | 10741 | 743 | 1104 | 1189 | 0 | 0 | 221.8 | 51.5 | 222 |
| fetch-pooled(pool=4)/h1/plain/closed-c8/GET /bytes/8388608 req0 | 1229 | 6509 | 8987 | 9757 | 0 | 0 | 1635.5 | 108.8 | 222 |
| fetch-pooled(pool=4)/h1/plain/closed-c32/GET /bytes/1048576 req0 | 10765 | 2968 | 3346 | 3506 | 0 | 0 | 236.9 | 50.2 | 222 |
| fetch-pooled(pool=4)/h1/plain/closed-c32/GET /bytes/8388608 req0 | 976 | 32768 | 35160 | 36438 | 0 | 0 | 1884.2 | 118.8 | 222 |
| fetch-pooled(pool=4)/h2/plain/closed-c8/GET /bytes/1048576 req0 | 4743 | 1657 | 2697 | 3127 | 0 | 0 | 506.9 | 301.0 | 226 |
| fetch-pooled(pool=4)/h2/plain/closed-c8/GET /bytes/8388608 req0 | 572 | 13951 | 16179 | 17220 | 0 | 0 | 4183.6 | 2091.1 | 226 |
| fetch-pooled(pool=4)/h2/plain/closed-c32/GET /bytes/1048576 req0 | 4022 | 6935 | 17531 | 20333 | 0 | 0 | 644.8 | 301.9 | 226 |
| fetch-pooled(pool=4)/h2/plain/closed-c32/GET /bytes/8388608 req0 | 519 | 61571 | 76481 | 81134 | 0 | 0 | 5512.8 | 2089.3 | 226 |
| matier/h1/plain/closed-c16/POST /echo req1048576 | 11516 | 1397 | 1749 | 1888 | 0 | 0 | 216.3 | 3.0 | 226 |
| matier/h1/plain/closed-c16/POST /echo req8388608 | 1108 | 14467 | 16458 | 17220 | 0 | 0 | 1870.3 | 3.0 | 234 |
| matier/h2/plain/closed-c16/POST /echo req1048576 | 9993 | 1584 | 2122 | 2480 | 0 | 0 | 215.5 | 9.0 | 234 |
| matier/h2/plain/closed-c16/POST /echo req8388608 | 1173 | 13631 | 15401 | 16024 | 0 | 0 | 1918.8 | 9.0 | 234 |
| hyper-legacy/h1/plain/closed-c16/POST /echo req1048576 | 11578 | 1389 | 1750 | 1893 | 0 | 0 | 221.1 | 16.9 | 234 |
| hyper-legacy/h1/plain/closed-c16/POST /echo req8388608 | 1100 | 14516 | 16712 | 17629 | 0 | 0 | 1892.9 | 17.0 | 234 |
| hyper-legacy/h2/plain/closed-c16/POST /echo req1048576 | 6564 | 2396 | 4284 | 4788 | 0 | 0 | 199.5 | 20.9 | 234 |
| hyper-legacy/h2/plain/closed-c16/POST /echo req8388608 | 811 | 19743 | 24248 | 25739 | 0 | 0 | 1446.8 | 21.0 | 234 |
| hyper-pooled(pool=4)/h1/plain/closed-c16/POST /echo req1048576 | 11550 | 1391 | 1724 | 1900 | 0 | 0 | 221.5 | 16.0 | 234 |
| hyper-pooled(pool=4)/h1/plain/closed-c16/POST /echo req8388608 | 1102 | 14508 | 16400 | 17089 | 0 | 0 | 1883.0 | 16.1 | 234 |
| hyper-pooled(pool=4)/h2/plain/closed-c16/POST /echo req1048576 | 9042 | 1753 | 2210 | 2417 | 0 | 0 | 241.8 | 21.0 | 234 |
| hyper-pooled(pool=4)/h2/plain/closed-c16/POST /echo req8388608 | 1094 | 14623 | 16433 | 16810 | 0 | 0 | 1808.3 | 21.0 | 234 |
| fetch/h1/plain/closed-c16/POST /echo req1048576 | 11457 | 1401 | 1796 | 1958 | 0 | 0 | 229.0 | 46.9 | 234 |
| fetch/h1/plain/closed-c16/POST /echo req8388608 | 1091 | 14885 | 19792 | 21332 | 0 | 0 | 2052.0 | 46.8 | 234 |
| fetch/h2/plain/closed-c16/POST /echo req1048576 | 6497 | 2433 | 4342 | 4825 | 0 | 0 | 212.9 | 51.8 | 234 |
| fetch/h2/plain/closed-c16/POST /echo req8388608 | 801 | 19972 | 24265 | 26149 | 0 | 0 | 1465.6 | 51.9 | 234 |
| fetch-pooled(pool=4)/h1/plain/closed-c16/POST /echo req1048576 | 11438 | 1403 | 1769 | 1943 | 0 | 0 | 231.6 | 46.0 | 234 |
| fetch-pooled(pool=4)/h1/plain/closed-c16/POST /echo req8388608 | 1080 | 14926 | 19628 | 21299 | 0 | 0 | 2063.9 | 46.0 | 234 |
| fetch-pooled(pool=4)/h2/plain/closed-c16/POST /echo req1048576 | 9177 | 1721 | 2259 | 2492 | 0 | 0 | 231.2 | 52.0 | 234 |
| fetch-pooled(pool=4)/h2/plain/closed-c16/POST /echo req8388608 | 1101 | 14524 | 16269 | 16794 | 0 | 0 | 1790.5 | 52.0 | 234 |

