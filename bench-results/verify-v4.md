matier-bench-server: http  on 127.0.0.1:8080 (5 threads)
matier-bench-server: https on 127.0.0.1:8443
[1/46] matier/h1/plain/closed-c64/GET /bytes/1024 req0
    149098 req/s, p99 699 us, 0 errors
[2/46] matier/h1/plain/closed-c64/GET /bytes/65536 req0
    93635 req/s, p99 864 us, 0 errors
[3/46] matier/h1/plain/closed-c256/GET /bytes/1024 req0
    147720 req/s, p99 3551 us, 0 errors
[4/46] matier/h1/plain/closed-c256/GET /bytes/65536 req0
    60042 req/s, p99 4960 us, 0 errors
[5/46] matier/h2/plain/closed-c64/GET /bytes/1024 req0
    216012 req/s, p99 571 us, 0 errors
[6/46] matier/h2/plain/closed-c64/GET /bytes/65536 req0
    105832 req/s, p99 850 us, 0 errors
[7/46] matier/h2/plain/closed-c256/GET /bytes/1024 req0
    354630 req/s, p99 1385 us, 0 errors
[8/46] matier/h2/plain/closed-c256/GET /bytes/65536 req0
    133094 req/s, p99 2331 us, 0 errors
[9/46] hyper-legacy/h1/plain/closed-c64/GET /bytes/1024 req0
    150276 req/s, p99 740 us, 0 errors
[10/46] hyper-legacy/h1/plain/closed-c64/GET /bytes/65536 req0
    95397 req/s, p99 831 us, 0 errors
[11/46] hyper-legacy/h1/plain/closed-c256/GET /bytes/1024 req0
    150659 req/s, p99 3090 us, 0 errors
[12/46] hyper-legacy/h1/plain/closed-c256/GET /bytes/65536 req0
    55440 req/s, p99 5124 us, 0 errors
[13/46] hyper-legacy/h2/plain/closed-c64/GET /bytes/1024 req0
    80215 req/s, p99 1211 us, 0 errors
[14/46] hyper-legacy/h2/plain/closed-c64/GET /bytes/65536 req0
    56728 req/s, p99 1701 us, 0 errors
[15/46] hyper-legacy/h2/plain/closed-c256/GET /bytes/1024 req0
    83398 req/s, p99 4248 us, 0 errors
[16/46] hyper-legacy/h2/plain/closed-c256/GET /bytes/65536 req0
    54888 req/s, p99 6074 us, 0 errors
[17/46] hyper-pooled(pool=4)/h1/plain/closed-c64/GET /bytes/1024 req0
    153778 req/s, p99 672 us, 0 errors
[18/46] hyper-pooled(pool=4)/h1/plain/closed-c64/GET /bytes/65536 req0
    94790 req/s, p99 830 us, 0 errors
[19/46] hyper-pooled(pool=4)/h1/plain/closed-c256/GET /bytes/1024 req0
    151091 req/s, p99 2296 us, 0 errors
[20/46] hyper-pooled(pool=4)/h1/plain/closed-c256/GET /bytes/65536 req0
    60709 req/s, p99 4735 us, 0 errors
[21/46] hyper-pooled(pool=4)/h2/plain/closed-c64/GET /bytes/1024 req0
    166008 req/s, p99 727 us, 0 errors
[22/46] hyper-pooled(pool=4)/h2/plain/closed-c64/GET /bytes/65536 req0
    97519 req/s, p99 937 us, 0 errors
[23/46] hyper-pooled(pool=4)/h2/plain/closed-c256/GET /bytes/1024 req0
    192581 req/s, p99 2400 us, 0 errors
[24/46] hyper-pooled(pool=4)/h2/plain/closed-c256/GET /bytes/65536 req0
    109907 req/s, p99 3191 us, 0 errors
[25/46] fetch/h1/plain/closed-c64/GET /bytes/1024 req0
    118474 req/s, p99 1036 us, 0 errors
[26/46] fetch/h1/plain/closed-c64/GET /bytes/65536 req0
    93196 req/s, p99 865 us, 0 errors
[27/46] fetch/h1/plain/closed-c256/GET /bytes/1024 req0
    122508 req/s, p99 4829 us, 0 errors
[28/46] fetch/h1/plain/closed-c256/GET /bytes/65536 req0
    59819 req/s, p99 4706 us, 0 errors
[29/46] fetch/h2/plain/closed-c64/GET /bytes/1024 req0
    64305 req/s, p99 1450 us, 0 errors
[30/46] fetch/h2/plain/closed-c64/GET /bytes/65536 req0
    27795 req/s, p99 3635 us, 0 errors
[31/46] fetch/h2/plain/closed-c256/GET /bytes/1024 req0
    66530 req/s, p99 4530 us, 0 errors
[32/46] fetch/h2/plain/closed-c256/GET /bytes/65536 req0
    25108 req/s, p99 14803 us, 0 errors
[33/46] fetch-pooled(pool=4)/h1/plain/closed-c64/GET /bytes/1024 req0
    129071 req/s, p99 937 us, 0 errors
[34/46] fetch-pooled(pool=4)/h1/plain/closed-c64/GET /bytes/65536 req0
    93813 req/s, p99 856 us, 0 errors
[35/46] fetch-pooled(pool=4)/h1/plain/closed-c256/GET /bytes/1024 req0
    134539 req/s, p99 4297 us, 0 errors
[36/46] fetch-pooled(pool=4)/h1/plain/closed-c256/GET /bytes/65536 req0
    61322 req/s, p99 4502 us, 0 errors
[37/46] fetch-pooled(pool=4)/h2/plain/closed-c64/GET /bytes/1024 req0
    117092 req/s, p99 1019 us, 0 errors
[38/46] fetch-pooled(pool=4)/h2/plain/closed-c64/GET /bytes/65536 req0
    67106 req/s, p99 1623 us, 0 errors
[39/46] fetch-pooled(pool=4)/h2/plain/closed-c256/GET /bytes/1024 req0
    125758 req/s, p99 3676 us, 0 errors
[40/46] fetch-pooled(pool=4)/h2/plain/closed-c256/GET /bytes/65536 req0
    73071 req/s, p99 5992 us, 0 errors
[41/46] hyper-legacy/h2/plain/closed-c64/GET /bytes/1024 req0
    78227 req/s, p99 1249 us, 0 errors
[42/46] hyper-legacy/h2/plain/closed-c64/GET /bytes/65536 req0
    55035 req/s, p99 1742 us, 0 errors
[43/46] fetch/h2/plain/closed-c64/GET /bytes/1024 req0
    64812 req/s, p99 1447 us, 0 errors
[44/46] fetch/h2/plain/closed-c64/GET /bytes/65536 req0
    27635 req/s, p99 3639 us, 0 errors
[45/46] matier(min=auto,max=1)/h2/plain/closed-c64/GET /bytes/1024 req0
    120873 req/s, p99 886 us, 0 errors
[46/46] matier(min=auto,max=1)/h2/plain/closed-c64/GET /bytes/65536 req0
    74592 req/s, p99 1375 us, 0 errors
| scenario | req/s | p50 us | p99 us | p99.9 us | errs | drops | cpu us/req | allocs/req | rss MB |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| matier/h1/plain/closed-c64/GET /bytes/1024 req0 | 149098 | 418 | 699 | 1345 | 0 | 0 | 21.8 | 2.0 | 35 |
| matier/h1/plain/closed-c64/GET /bytes/65536 req0 | 93635 | 678 | 864 | 1428 | 0 | 0 | 28.4 | 2.0 | 45 |
| matier/h1/plain/closed-c256/GET /bytes/1024 req0 | 147720 | 1687 | 3551 | 7410 | 0 | 0 | 23.4 | 2.0 | 107 |
| matier/h1/plain/closed-c256/GET /bytes/65536 req0 | 60042 | 4198 | 4960 | 5517 | 0 | 0 | 33.6 | 2.0 | 125 |
| matier/h2/plain/closed-c64/GET /bytes/1024 req0 | 216012 | 285 | 571 | 835 | 0 | 0 | 14.9 | 4.0 | 128 |
| matier/h2/plain/closed-c64/GET /bytes/65536 req0 | 105832 | 599 | 850 | 998 | 0 | 0 | 28.4 | 4.0 | 132 |
| matier/h2/plain/closed-c256/GET /bytes/1024 req0 | 354630 | 709 | 1385 | 1639 | 0 | 0 | 11.1 | 4.0 | 135 |
| matier/h2/plain/closed-c256/GET /bytes/65536 req0 | 133094 | 1922 | 2331 | 2523 | 0 | 0 | 23.6 | 4.0 | 138 |
| hyper-legacy/h1/plain/closed-c64/GET /bytes/1024 req0 | 150276 | 420 | 740 | 881 | 0 | 0 | 27.2 | 14.0 | 142 |
| hyper-legacy/h1/plain/closed-c64/GET /bytes/65536 req0 | 95397 | 668 | 831 | 909 | 0 | 0 | 33.4 | 14.0 | 143 |
| hyper-legacy/h1/plain/closed-c256/GET /bytes/1024 req0 | 150659 | 1669 | 3090 | 3834 | 0 | 0 | 28.9 | 14.0 | 162 |
| hyper-legacy/h1/plain/closed-c256/GET /bytes/65536 req0 | 55440 | 4588 | 5124 | 5255 | 0 | 0 | 44.3 | 14.6 | 205 |
| hyper-legacy/h2/plain/closed-c64/GET /bytes/1024 req0 | 80215 | 792 | 1211 | 1379 | 0 | 0 | 39.5 | 14.3 | 206 |
| hyper-legacy/h2/plain/closed-c64/GET /bytes/65536 req0 | 56728 | 1117 | 1701 | 1923 | 0 | 0 | 46.0 | 17.0 | 206 |
| hyper-legacy/h2/plain/closed-c256/GET /bytes/1024 req0 | 83398 | 3070 | 4248 | 4735 | 0 | 0 | 40.1 | 14.3 | 209 |
| hyper-legacy/h2/plain/closed-c256/GET /bytes/65536 req0 | 54888 | 4723 | 6074 | 6472 | 0 | 0 | 46.5 | 17.0 | 214 |
| hyper-pooled(pool=4)/h1/plain/closed-c64/GET /bytes/1024 req0 | 153778 | 410 | 672 | 805 | 0 | 0 | 26.8 | 14.0 | 214 |
| hyper-pooled(pool=4)/h1/plain/closed-c64/GET /bytes/65536 req0 | 94790 | 672 | 830 | 909 | 0 | 0 | 34.0 | 14.0 | 214 |
| hyper-pooled(pool=4)/h1/plain/closed-c256/GET /bytes/1024 req0 | 151091 | 1682 | 2296 | 2783 | 0 | 0 | 28.2 | 14.0 | 216 |
| hyper-pooled(pool=4)/h1/plain/closed-c256/GET /bytes/65536 req0 | 60709 | 4198 | 4735 | 5026 | 0 | 0 | 43.9 | 14.4 | 224 |
| hyper-pooled(pool=4)/h2/plain/closed-c64/GET /bytes/1024 req0 | 166008 | 375 | 727 | 865 | 0 | 0 | 24.0 | 14.3 | 224 |
| hyper-pooled(pool=4)/h2/plain/closed-c64/GET /bytes/65536 req0 | 97519 | 658 | 937 | 1094 | 0 | 0 | 40.3 | 17.0 | 224 |
| hyper-pooled(pool=4)/h2/plain/closed-c256/GET /bytes/1024 req0 | 192581 | 1296 | 2400 | 2836 | 0 | 0 | 20.5 | 14.3 | 228 |
| hyper-pooled(pool=4)/h2/plain/closed-c256/GET /bytes/65536 req0 | 109907 | 2476 | 3191 | 3465 | 0 | 0 | 35.1 | 17.0 | 230 |
| fetch/h1/plain/closed-c64/GET /bytes/1024 req0 | 118474 | 531 | 1036 | 1244 | 0 | 0 | 37.5 | 43.0 | 233 |
| fetch/h1/plain/closed-c64/GET /bytes/65536 req0 | 93196 | 684 | 865 | 956 | 0 | 0 | 44.3 | 43.0 | 233 |
| fetch/h1/plain/closed-c256/GET /bytes/1024 req0 | 122508 | 2031 | 4829 | 5931 | 0 | 0 | 37.7 | 43.0 | 248 |
| fetch/h1/plain/closed-c256/GET /bytes/65536 req0 | 59819 | 4264 | 4706 | 4932 | 0 | 0 | 52.1 | 43.7 | 248 |
| fetch/h2/plain/closed-c64/GET /bytes/1024 req0 | 64305 | 990 | 1450 | 1650 | 0 | 0 | 52.4 | 46.3 | 248 |
| fetch/h2/plain/closed-c64/GET /bytes/65536 req0 | 27795 | 2265 | 3635 | 4164 | 0 | 0 | 95.2 | 61.0 | 249 |
| fetch/h2/plain/closed-c256/GET /bytes/1024 req0 | 66530 | 3848 | 4530 | 4821 | 0 | 0 | 51.4 | 46.3 | 250 |
| fetch/h2/plain/closed-c256/GET /bytes/65536 req0 | 25108 | 10109 | 14803 | 16605 | 0 | 0 | 102.1 | 61.9 | 250 |
| fetch-pooled(pool=4)/h1/plain/closed-c64/GET /bytes/1024 req0 | 129071 | 487 | 937 | 1115 | 0 | 0 | 35.8 | 43.0 | 250 |
| fetch-pooled(pool=4)/h1/plain/closed-c64/GET /bytes/65536 req0 | 93813 | 680 | 856 | 946 | 0 | 0 | 42.9 | 43.0 | 250 |
| fetch-pooled(pool=4)/h1/plain/closed-c256/GET /bytes/1024 req0 | 134539 | 1841 | 4297 | 5243 | 0 | 0 | 36.1 | 43.0 | 252 |
| fetch-pooled(pool=4)/h1/plain/closed-c256/GET /bytes/65536 req0 | 61322 | 4162 | 4502 | 4641 | 0 | 0 | 51.2 | 43.6 | 252 |
| fetch-pooled(pool=4)/h2/plain/closed-c64/GET /bytes/1024 req0 | 117092 | 538 | 1019 | 1199 | 0 | 0 | 36.5 | 46.3 | 252 |
| fetch-pooled(pool=4)/h2/plain/closed-c64/GET /bytes/65536 req0 | 67106 | 931 | 1623 | 1939 | 0 | 0 | 63.5 | 61.0 | 254 |
| fetch-pooled(pool=4)/h2/plain/closed-c256/GET /bytes/1024 req0 | 125758 | 2002 | 3676 | 4362 | 0 | 0 | 34.2 | 46.3 | 254 |
| fetch-pooled(pool=4)/h2/plain/closed-c256/GET /bytes/65536 req0 | 73071 | 3424 | 5992 | 7021 | 0 | 0 | 58.5 | 61.0 | 265 |
| hyper-legacy/h2/plain/closed-c64/GET /bytes/1024 req0 | 78227 | 811 | 1249 | 1428 | 0 | 0 | 39.8 | 14.3 | 265 |
| hyper-legacy/h2/plain/closed-c64/GET /bytes/65536 req0 | 55035 | 1154 | 1742 | 1949 | 0 | 0 | 47.0 | 17.0 | 265 |
| fetch/h2/plain/closed-c64/GET /bytes/1024 req0 | 64812 | 981 | 1447 | 1650 | 0 | 0 | 51.6 | 46.3 | 265 |
| fetch/h2/plain/closed-c64/GET /bytes/65536 req0 | 27635 | 2275 | 3639 | 4202 | 0 | 0 | 94.5 | 61.0 | 265 |
| matier(min=auto,max=1)/h2/plain/closed-c64/GET /bytes/1024 req0 | 120873 | 524 | 886 | 1011 | 0 | 0 | 9.4 | 4.0 | 265 |
| matier(min=auto,max=1)/h2/plain/closed-c64/GET /bytes/65536 req0 | 74592 | 846 | 1375 | 1578 | 0 | 0 | 15.7 | 4.0 | 265 |

