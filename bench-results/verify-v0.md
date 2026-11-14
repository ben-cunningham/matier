matier-bench-server: http  on 127.0.0.1:8080 (5 threads)
matier-bench-server: https on 127.0.0.1:8443
[1/46] matier/h1/plain/closed-c64/GET /bytes/1024 req0
    154855 req/s, p99 627 us, 0 errors
[2/46] matier/h1/plain/closed-c64/GET /bytes/65536 req0
    95319 req/s, p99 832 us, 0 errors
[3/46] matier/h1/plain/closed-c256/GET /bytes/1024 req0
    153912 req/s, p99 2022 us, 0 errors
[4/46] matier/h1/plain/closed-c256/GET /bytes/65536 req0
    86818 req/s, p99 3217 us, 0 errors
[5/46] matier/h2/plain/closed-c64/GET /bytes/1024 req0
    227318 req/s, p99 524 us, 0 errors
[6/46] matier/h2/plain/closed-c64/GET /bytes/65536 req0
    99125 req/s, p99 847 us, 0 errors
[7/46] matier/h2/plain/closed-c256/GET /bytes/1024 req0
    324403 req/s, p99 1526 us, 0 errors
[8/46] matier/h2/plain/closed-c256/GET /bytes/65536 req0
    85457 req/s, p99 3482 us, 0 errors
[9/46] hyper-legacy/h1/plain/closed-c64/GET /bytes/1024 req0
    144911 req/s, p99 778 us, 0 errors
[10/46] hyper-legacy/h1/plain/closed-c64/GET /bytes/65536 req0
    93009 req/s, p99 861 us, 0 errors
[11/46] hyper-legacy/h1/plain/closed-c256/GET /bytes/1024 req0
    150876 req/s, p99 3322 us, 0 errors
[12/46] hyper-legacy/h1/plain/closed-c256/GET /bytes/65536 req0
    54738 req/s, p99 5415 us, 0 errors
[13/46] hyper-legacy/h2/plain/closed-c64/GET /bytes/1024 req0
    78778 req/s, p99 1252 us, 0 errors
[14/46] hyper-legacy/h2/plain/closed-c64/GET /bytes/65536 req0
    49283 req/s, p99 2572 us, 0 errors
[15/46] hyper-legacy/h2/plain/closed-c256/GET /bytes/1024 req0
    82219 req/s, p99 4338 us, 0 errors
[16/46] hyper-legacy/h2/plain/closed-c256/GET /bytes/65536 req0
    53784 req/s, p99 6250 us, 0 errors
[17/46] hyper-pooled(pool=4)/h1/plain/closed-c64/GET /bytes/1024 req0
    149944 req/s, p99 705 us, 0 errors
[18/46] hyper-pooled(pool=4)/h1/plain/closed-c64/GET /bytes/65536 req0
    92165 req/s, p99 862 us, 0 errors
[19/46] hyper-pooled(pool=4)/h1/plain/closed-c256/GET /bytes/1024 req0
    152591 req/s, p99 2384 us, 0 errors
[20/46] hyper-pooled(pool=4)/h1/plain/closed-c256/GET /bytes/65536 req0
    57889 req/s, p99 5001 us, 0 errors
[21/46] hyper-pooled(pool=4)/h2/plain/closed-c64/GET /bytes/1024 req0
    162343 req/s, p99 748 us, 0 errors
[22/46] hyper-pooled(pool=4)/h2/plain/closed-c64/GET /bytes/65536 req0
    102376 req/s, p99 980 us, 0 errors
[23/46] hyper-pooled(pool=4)/h2/plain/closed-c256/GET /bytes/1024 req0
    183641 req/s, p99 2564 us, 0 errors
[24/46] hyper-pooled(pool=4)/h2/plain/closed-c256/GET /bytes/65536 req0
    129915 req/s, p99 3437 us, 0 errors
[25/46] fetch/h1/plain/closed-c64/GET /bytes/1024 req0
    118743 req/s, p99 1026 us, 0 errors
[26/46] fetch/h1/plain/closed-c64/GET /bytes/65536 req0
    90059 req/s, p99 921 us, 0 errors
[27/46] fetch/h1/plain/closed-c256/GET /bytes/1024 req0
    120411 req/s, p99 4956 us, 0 errors
[28/46] fetch/h1/plain/closed-c256/GET /bytes/65536 req0
    60803 req/s, p99 4571 us, 0 errors
[29/46] fetch/h2/plain/closed-c64/GET /bytes/1024 req0
    65462 req/s, p99 1441 us, 0 errors
[30/46] fetch/h2/plain/closed-c64/GET /bytes/65536 req0
    29701 req/s, p99 3252 us, 0 errors
[31/46] fetch/h2/plain/closed-c256/GET /bytes/1024 req0
    63637 req/s, p99 4604 us, 0 errors
[32/46] fetch/h2/plain/closed-c256/GET /bytes/65536 req0
    26692 req/s, p99 13574 us, 0 errors
[33/46] fetch-pooled(pool=4)/h1/plain/closed-c64/GET /bytes/1024 req0
    126048 req/s, p99 955 us, 0 errors
[34/46] fetch-pooled(pool=4)/h1/plain/closed-c64/GET /bytes/65536 req0
    90025 req/s, p99 897 us, 0 errors
[35/46] fetch-pooled(pool=4)/h1/plain/closed-c256/GET /bytes/1024 req0
    130252 req/s, p99 4444 us, 0 errors
[36/46] fetch-pooled(pool=4)/h1/plain/closed-c256/GET /bytes/65536 req0
    55423 req/s, p99 5071 us, 0 errors
[37/46] fetch-pooled(pool=4)/h2/plain/closed-c64/GET /bytes/1024 req0
    116632 req/s, p99 1034 us, 0 errors
[38/46] fetch-pooled(pool=4)/h2/plain/closed-c64/GET /bytes/65536 req0
    66331 req/s, p99 1684 us, 0 errors
[39/46] fetch-pooled(pool=4)/h2/plain/closed-c256/GET /bytes/1024 req0
    126783 req/s, p99 3695 us, 0 errors
[40/46] fetch-pooled(pool=4)/h2/plain/closed-c256/GET /bytes/65536 req0
    72143 req/s, p99 6033 us, 0 errors
[41/46] matier/h2/plain/closed-c64/GET /bytes/1024 req0
    112328 req/s, p99 969 us, 0 errors
[42/46] matier/h2/plain/closed-c64/GET /bytes/65536 req0
    64260 req/s, p99 1585 us, 0 errors
[43/46] hyper-legacy/h2/plain/closed-c64/GET /bytes/1024 req0
    79214 req/s, p99 1234 us, 0 errors
[44/46] hyper-legacy/h2/plain/closed-c64/GET /bytes/65536 req0
    55223 req/s, p99 1764 us, 0 errors
[45/46] fetch/h2/plain/closed-c64/GET /bytes/1024 req0
    64888 req/s, p99 1450 us, 0 errors
[46/46] fetch/h2/plain/closed-c64/GET /bytes/65536 req0
    29597 req/s, p99 3240 us, 0 errors
| scenario | req/s | p50 us | p99 us | p99.9 us | errs | drops | cpu us/req | allocs/req | rss MB |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| matier/h1/plain/closed-c64/GET /bytes/1024 req0 | 154855 | 408 | 627 | 765 | 0 | 0 | 22.8 | 7.0 | 35 |
| matier/h1/plain/closed-c64/GET /bytes/65536 req0 | 95319 | 668 | 832 | 901 | 0 | 0 | 27.8 | 7.0 | 43 |
| matier/h1/plain/closed-c256/GET /bytes/1024 req0 | 153912 | 1655 | 2022 | 2281 | 0 | 0 | 23.7 | 7.0 | 108 |
| matier/h1/plain/closed-c256/GET /bytes/65536 req0 | 86818 | 2947 | 3217 | 3316 | 0 | 0 | 33.9 | 7.0 | 136 |
| matier/h2/plain/closed-c64/GET /bytes/1024 req0 | 227318 | 274 | 524 | 628 | 0 | 0 | 17.3 | 10.0 | 147 |
| matier/h2/plain/closed-c64/GET /bytes/65536 req0 | 99125 | 643 | 847 | 934 | 0 | 0 | 30.9 | 11.0 | 148 |
| matier/h2/plain/closed-c256/GET /bytes/1024 req0 | 324403 | 756 | 1526 | 1938 | 0 | 0 | 13.0 | 10.0 | 156 |
| matier/h2/plain/closed-c256/GET /bytes/65536 req0 | 85457 | 2984 | 3482 | 3672 | 0 | 0 | 32.7 | 11.0 | 163 |
| hyper-legacy/h1/plain/closed-c64/GET /bytes/1024 req0 | 144911 | 436 | 778 | 928 | 0 | 0 | 28.1 | 14.0 | 169 |
| hyper-legacy/h1/plain/closed-c64/GET /bytes/65536 req0 | 93009 | 685 | 861 | 950 | 0 | 0 | 34.9 | 14.0 | 170 |
| hyper-legacy/h1/plain/closed-c256/GET /bytes/1024 req0 | 150876 | 1659 | 3322 | 4141 | 0 | 0 | 28.2 | 14.0 | 181 |
| hyper-legacy/h1/plain/closed-c256/GET /bytes/65536 req0 | 54738 | 4649 | 5415 | 5915 | 0 | 0 | 45.8 | 14.4 | 227 |
| hyper-legacy/h2/plain/closed-c64/GET /bytes/1024 req0 | 78778 | 805 | 1252 | 1436 | 0 | 0 | 39.9 | 14.3 | 228 |
| hyper-legacy/h2/plain/closed-c64/GET /bytes/65536 req0 | 49283 | 1234 | 2572 | 5464 | 0 | 0 | 50.5 | 17.0 | 228 |
| hyper-legacy/h2/plain/closed-c256/GET /bytes/1024 req0 | 82219 | 3099 | 4338 | 9134 | 0 | 0 | 40.0 | 14.3 | 228 |
| hyper-legacy/h2/plain/closed-c256/GET /bytes/65536 req0 | 53784 | 4809 | 6250 | 6840 | 0 | 0 | 47.7 | 17.0 | 228 |
| hyper-pooled(pool=4)/h1/plain/closed-c64/GET /bytes/1024 req0 | 149944 | 421 | 705 | 848 | 0 | 0 | 27.3 | 14.0 | 228 |
| hyper-pooled(pool=4)/h1/plain/closed-c64/GET /bytes/65536 req0 | 92165 | 691 | 862 | 944 | 0 | 0 | 35.2 | 14.0 | 228 |
| hyper-pooled(pool=4)/h1/plain/closed-c256/GET /bytes/1024 req0 | 152591 | 1664 | 2384 | 2935 | 0 | 0 | 27.5 | 14.0 | 228 |
| hyper-pooled(pool=4)/h1/plain/closed-c256/GET /bytes/65536 req0 | 57889 | 4395 | 5001 | 5308 | 0 | 0 | 45.1 | 14.4 | 228 |
| hyper-pooled(pool=4)/h2/plain/closed-c64/GET /bytes/1024 req0 | 162343 | 384 | 748 | 893 | 0 | 0 | 24.2 | 14.3 | 228 |
| hyper-pooled(pool=4)/h2/plain/closed-c64/GET /bytes/65536 req0 | 102376 | 628 | 980 | 1159 | 0 | 0 | 38.8 | 17.0 | 228 |
| hyper-pooled(pool=4)/h2/plain/closed-c256/GET /bytes/1024 req0 | 183641 | 1360 | 2564 | 3060 | 0 | 0 | 21.4 | 14.3 | 228 |
| hyper-pooled(pool=4)/h2/plain/closed-c256/GET /bytes/65536 req0 | 129915 | 1924 | 3437 | 4166 | 0 | 0 | 31.8 | 17.0 | 228 |
| fetch/h1/plain/closed-c64/GET /bytes/1024 req0 | 118743 | 530 | 1026 | 1234 | 0 | 0 | 37.1 | 43.0 | 228 |
| fetch/h1/plain/closed-c64/GET /bytes/65536 req0 | 90059 | 707 | 921 | 1148 | 0 | 0 | 45.6 | 43.0 | 228 |
| fetch/h1/plain/closed-c256/GET /bytes/1024 req0 | 120411 | 2054 | 4956 | 6291 | 0 | 0 | 38.1 | 43.0 | 228 |
| fetch/h1/plain/closed-c256/GET /bytes/65536 req0 | 60803 | 4198 | 4571 | 5919 | 0 | 0 | 52.9 | 43.5 | 228 |
| fetch/h2/plain/closed-c64/GET /bytes/1024 req0 | 65462 | 972 | 1441 | 1644 | 0 | 0 | 51.3 | 46.3 | 228 |
| fetch/h2/plain/closed-c64/GET /bytes/65536 req0 | 29701 | 2146 | 3252 | 3711 | 0 | 0 | 89.8 | 61.0 | 228 |
| fetch/h2/plain/closed-c256/GET /bytes/1024 req0 | 63637 | 4028 | 4604 | 4829 | 0 | 0 | 52.2 | 46.3 | 228 |
| fetch/h2/plain/closed-c256/GET /bytes/65536 req0 | 26692 | 9552 | 13574 | 15139 | 0 | 0 | 97.7 | 61.8 | 228 |
| fetch-pooled(pool=4)/h1/plain/closed-c64/GET /bytes/1024 req0 | 126048 | 499 | 955 | 1141 | 0 | 0 | 36.1 | 43.0 | 228 |
| fetch-pooled(pool=4)/h1/plain/closed-c64/GET /bytes/65536 req0 | 90025 | 709 | 897 | 994 | 0 | 0 | 45.1 | 43.0 | 228 |
| fetch-pooled(pool=4)/h1/plain/closed-c256/GET /bytes/1024 req0 | 130252 | 1903 | 4444 | 5476 | 0 | 0 | 36.5 | 43.0 | 228 |
| fetch-pooled(pool=4)/h1/plain/closed-c256/GET /bytes/65536 req0 | 55423 | 4604 | 5071 | 5243 | 0 | 0 | 54.9 | 43.7 | 228 |
| fetch-pooled(pool=4)/h2/plain/closed-c64/GET /bytes/1024 req0 | 116632 | 539 | 1034 | 1229 | 0 | 0 | 36.4 | 46.3 | 228 |
| fetch-pooled(pool=4)/h2/plain/closed-c64/GET /bytes/65536 req0 | 66331 | 941 | 1684 | 2001 | 0 | 0 | 64.7 | 61.0 | 228 |
| fetch-pooled(pool=4)/h2/plain/closed-c256/GET /bytes/1024 req0 | 126783 | 1983 | 3695 | 4375 | 0 | 0 | 33.5 | 46.3 | 228 |
| fetch-pooled(pool=4)/h2/plain/closed-c256/GET /bytes/65536 req0 | 72143 | 3478 | 6033 | 6959 | 0 | 0 | 60.1 | 61.0 | 228 |
| matier/h2/plain/closed-c64/GET /bytes/1024 req0 | 112328 | 563 | 969 | 1110 | 0 | 0 | 14.3 | 10.0 | 228 |
| matier/h2/plain/closed-c64/GET /bytes/65536 req0 | 64260 | 982 | 1585 | 1802 | 0 | 0 | 25.1 | 11.0 | 228 |
| hyper-legacy/h2/plain/closed-c64/GET /bytes/1024 req0 | 79214 | 802 | 1234 | 1413 | 0 | 0 | 39.6 | 14.3 | 228 |
| hyper-legacy/h2/plain/closed-c64/GET /bytes/65536 req0 | 55223 | 1148 | 1764 | 1987 | 0 | 0 | 46.8 | 17.0 | 228 |
| fetch/h2/plain/closed-c64/GET /bytes/1024 req0 | 64888 | 981 | 1450 | 1669 | 0 | 0 | 51.8 | 46.3 | 228 |
| fetch/h2/plain/closed-c64/GET /bytes/65536 req0 | 29597 | 2152 | 3240 | 3766 | 0 | 0 | 90.4 | 61.0 | 228 |

