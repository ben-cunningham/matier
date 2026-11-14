matier-bench-server: http  on 127.0.0.1:8080 (5 threads)
matier-bench-server: https on 127.0.0.1:8443
[1/46] matier/h1/plain/closed-c64/GET /bytes/1024 req0
    152553 req/s, p99 734 us, 0 errors
[2/46] matier/h1/plain/closed-c64/GET /bytes/65536 req0
    81095 req/s, p99 2126 us, 0 errors
[3/46] matier/h1/plain/closed-c256/GET /bytes/1024 req0
    151238 req/s, p99 3021 us, 0 errors
[4/46] matier/h1/plain/closed-c256/GET /bytes/65536 req0
    85693 req/s, p99 3539 us, 0 errors
[5/46] matier/h2/plain/closed-c64/GET /bytes/1024 req0
    205967 req/s, p99 608 us, 0 errors
[6/46] matier/h2/plain/closed-c64/GET /bytes/65536 req0
    98079 req/s, p99 1034 us, 0 errors
[7/46] matier/h2/plain/closed-c256/GET /bytes/1024 req0
    251728 req/s, p99 1996 us, 0 errors
[8/46] matier/h2/plain/closed-c256/GET /bytes/65536 req0
    98921 req/s, p99 3191 us, 0 errors
[9/46] hyper-legacy/h1/plain/closed-c64/GET /bytes/1024 req0
    139560 req/s, p99 901 us, 0 errors
[10/46] hyper-legacy/h1/plain/closed-c64/GET /bytes/65536 req0
    93342 req/s, p99 847 us, 0 errors
[11/46] hyper-legacy/h1/plain/closed-c256/GET /bytes/1024 req0
    152712 req/s, p99 2980 us, 0 errors
[12/46] hyper-legacy/h1/plain/closed-c256/GET /bytes/65536 req0
    79935 req/s, p99 3629 us, 0 errors
[13/46] hyper-legacy/h2/plain/closed-c64/GET /bytes/1024 req0
    80029 req/s, p99 1223 us, 0 errors
[14/46] hyper-legacy/h2/plain/closed-c64/GET /bytes/65536 req0
    56306 req/s, p99 1745 us, 0 errors
[15/46] hyper-legacy/h2/plain/closed-c256/GET /bytes/1024 req0
    84602 req/s, p99 4248 us, 0 errors
[16/46] hyper-legacy/h2/plain/closed-c256/GET /bytes/65536 req0
    54938 req/s, p99 6054 us, 0 errors
[17/46] hyper-pooled(pool=4)/h1/plain/closed-c64/GET /bytes/1024 req0
    151620 req/s, p99 676 us, 0 errors
[18/46] hyper-pooled(pool=4)/h1/plain/closed-c64/GET /bytes/65536 req0
    93109 req/s, p99 844 us, 0 errors
[19/46] hyper-pooled(pool=4)/h1/plain/closed-c256/GET /bytes/1024 req0
    152209 req/s, p99 2220 us, 0 errors
[20/46] hyper-pooled(pool=4)/h1/plain/closed-c256/GET /bytes/65536 req0
    78489 req/s, p99 3643 us, 0 errors
[21/46] hyper-pooled(pool=4)/h2/plain/closed-c64/GET /bytes/1024 req0
    164047 req/s, p99 746 us, 0 errors
[22/46] hyper-pooled(pool=4)/h2/plain/closed-c64/GET /bytes/65536 req0
    102765 req/s, p99 953 us, 0 errors
[23/46] hyper-pooled(pool=4)/h2/plain/closed-c256/GET /bytes/1024 req0
    192147 req/s, p99 2482 us, 0 errors
[24/46] hyper-pooled(pool=4)/h2/plain/closed-c256/GET /bytes/65536 req0
    132604 req/s, p99 3299 us, 0 errors
[25/46] fetch/h1/plain/closed-c64/GET /bytes/1024 req0
    119176 req/s, p99 1029 us, 0 errors
[26/46] fetch/h1/plain/closed-c64/GET /bytes/65536 req0
    89102 req/s, p99 1027 us, 0 errors
[27/46] fetch/h1/plain/closed-c256/GET /bytes/1024 req0
    121696 req/s, p99 4919 us, 0 errors
[28/46] fetch/h1/plain/closed-c256/GET /bytes/65536 req0
    83249 req/s, p99 3482 us, 0 errors
[29/46] fetch/h2/plain/closed-c64/GET /bytes/1024 req0
    66438 req/s, p99 1428 us, 0 errors
[30/46] fetch/h2/plain/closed-c64/GET /bytes/65536 req0
    29670 req/s, p99 3215 us, 0 errors
[31/46] fetch/h2/plain/closed-c256/GET /bytes/1024 req0
    65775 req/s, p99 4567 us, 0 errors
[32/46] fetch/h2/plain/closed-c256/GET /bytes/65536 req0
    26614 req/s, p99 13681 us, 0 errors
[33/46] fetch-pooled(pool=4)/h1/plain/closed-c64/GET /bytes/1024 req0
    127921 req/s, p99 949 us, 0 errors
[34/46] fetch-pooled(pool=4)/h1/plain/closed-c64/GET /bytes/65536 req0
    90060 req/s, p99 1018 us, 0 errors
[35/46] fetch-pooled(pool=4)/h1/plain/closed-c256/GET /bytes/1024 req0
    136382 req/s, p99 4506 us, 0 errors
[36/46] fetch-pooled(pool=4)/h1/plain/closed-c256/GET /bytes/65536 req0
    84243 req/s, p99 3400 us, 0 errors
[37/46] fetch-pooled(pool=4)/h2/plain/closed-c64/GET /bytes/1024 req0
    115697 req/s, p99 1065 us, 0 errors
[38/46] fetch-pooled(pool=4)/h2/plain/closed-c64/GET /bytes/65536 req0
    67094 req/s, p99 1708 us, 0 errors
[39/46] fetch-pooled(pool=4)/h2/plain/closed-c256/GET /bytes/1024 req0
    124880 req/s, p99 3746 us, 0 errors
[40/46] fetch-pooled(pool=4)/h2/plain/closed-c256/GET /bytes/65536 req0
    72063 req/s, p99 6214 us, 0 errors
[41/46] hyper-legacy/h2/plain/closed-c64/GET /bytes/1024 req0
    79490 req/s, p99 1229 us, 0 errors
[42/46] hyper-legacy/h2/plain/closed-c64/GET /bytes/65536 req0
    56406 req/s, p99 1738 us, 0 errors
[43/46] fetch/h2/plain/closed-c64/GET /bytes/1024 req0
    65988 req/s, p99 1435 us, 0 errors
[44/46] fetch/h2/plain/closed-c64/GET /bytes/65536 req0
    29773 req/s, p99 3215 us, 0 errors
[45/46] matier(min=auto,max=1)/h2/plain/closed-c64/GET /bytes/1024 req0
    111845 req/s, p99 977 us, 0 errors
[46/46] matier(min=auto,max=1)/h2/plain/closed-c64/GET /bytes/65536 req0
    66129 req/s, p99 1542 us, 0 errors
| scenario | req/s | p50 us | p99 us | p99.9 us | errs | drops | cpu us/req | allocs/req | rss MB |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| matier/h1/plain/closed-c64/GET /bytes/1024 req0 | 152553 | 409 | 734 | 1015 | 0 | 0 | 19.4 | 7.0 | 35 |
| matier/h1/plain/closed-c64/GET /bytes/65536 req0 | 81095 | 720 | 2126 | 5075 | 0 | 0 | 27.9 | 7.0 | 42 |
| matier/h1/plain/closed-c256/GET /bytes/1024 req0 | 151238 | 1656 | 3021 | 5599 | 0 | 0 | 20.8 | 7.0 | 112 |
| matier/h1/plain/closed-c256/GET /bytes/65536 req0 | 85693 | 2968 | 3539 | 6226 | 0 | 0 | 32.9 | 7.0 | 128 |
| matier/h2/plain/closed-c64/GET /bytes/1024 req0 | 205967 | 303 | 608 | 903 | 0 | 0 | 15.5 | 10.0 | 132 |
| matier/h2/plain/closed-c64/GET /bytes/65536 req0 | 98079 | 649 | 1034 | 1538 | 0 | 0 | 29.4 | 11.0 | 132 |
| matier/h2/plain/closed-c256/GET /bytes/1024 req0 | 251728 | 1035 | 1996 | 3049 | 0 | 0 | 13.5 | 10.0 | 166 |
| matier/h2/plain/closed-c256/GET /bytes/65536 req0 | 98921 | 2619 | 3191 | 4817 | 0 | 0 | 30.1 | 11.0 | 166 |
| hyper-legacy/h1/plain/closed-c64/GET /bytes/1024 req0 | 139560 | 442 | 901 | 1361 | 0 | 0 | 26.6 | 14.0 | 166 |
| hyper-legacy/h1/plain/closed-c64/GET /bytes/65536 req0 | 93342 | 682 | 847 | 930 | 0 | 0 | 33.9 | 14.0 | 166 |
| hyper-legacy/h1/plain/closed-c256/GET /bytes/1024 req0 | 152712 | 1648 | 2980 | 3713 | 0 | 0 | 28.0 | 14.0 | 195 |
| hyper-legacy/h1/plain/closed-c256/GET /bytes/65536 req0 | 79935 | 3187 | 3629 | 4020 | 0 | 0 | 39.4 | 14.0 | 249 |
| hyper-legacy/h2/plain/closed-c64/GET /bytes/1024 req0 | 80029 | 793 | 1223 | 1409 | 0 | 0 | 39.5 | 14.3 | 250 |
| hyper-legacy/h2/plain/closed-c64/GET /bytes/65536 req0 | 56306 | 1125 | 1745 | 1960 | 0 | 0 | 46.3 | 17.0 | 250 |
| hyper-legacy/h2/plain/closed-c256/GET /bytes/1024 req0 | 84602 | 3023 | 4248 | 4719 | 0 | 0 | 39.6 | 14.3 | 251 |
| hyper-legacy/h2/plain/closed-c256/GET /bytes/65536 req0 | 54938 | 4706 | 6054 | 6504 | 0 | 0 | 46.9 | 17.0 | 251 |
| hyper-pooled(pool=4)/h1/plain/closed-c64/GET /bytes/1024 req0 | 151620 | 416 | 676 | 807 | 0 | 0 | 27.0 | 14.0 | 251 |
| hyper-pooled(pool=4)/h1/plain/closed-c64/GET /bytes/65536 req0 | 93109 | 685 | 844 | 921 | 0 | 0 | 34.1 | 14.0 | 252 |
| hyper-pooled(pool=4)/h1/plain/closed-c256/GET /bytes/1024 req0 | 152209 | 1671 | 2220 | 2681 | 0 | 0 | 27.7 | 14.0 | 254 |
| hyper-pooled(pool=4)/h1/plain/closed-c256/GET /bytes/65536 req0 | 78489 | 3246 | 3643 | 3869 | 0 | 0 | 39.9 | 14.1 | 255 |
| hyper-pooled(pool=4)/h2/plain/closed-c64/GET /bytes/1024 req0 | 164047 | 379 | 746 | 895 | 0 | 0 | 24.0 | 14.3 | 255 |
| hyper-pooled(pool=4)/h2/plain/closed-c64/GET /bytes/65536 req0 | 102765 | 627 | 953 | 1128 | 0 | 0 | 39.0 | 17.0 | 255 |
| hyper-pooled(pool=4)/h2/plain/closed-c256/GET /bytes/1024 req0 | 192147 | 1294 | 2482 | 2974 | 0 | 0 | 20.1 | 14.3 | 255 |
| hyper-pooled(pool=4)/h2/plain/closed-c256/GET /bytes/65536 req0 | 132604 | 1889 | 3299 | 3949 | 0 | 0 | 31.4 | 17.0 | 256 |
| fetch/h1/plain/closed-c64/GET /bytes/1024 req0 | 119176 | 528 | 1029 | 1235 | 0 | 0 | 37.4 | 43.0 | 259 |
| fetch/h1/plain/closed-c64/GET /bytes/65536 req0 | 89102 | 711 | 1027 | 1350 | 0 | 0 | 46.7 | 43.0 | 260 |
| fetch/h1/plain/closed-c256/GET /bytes/1024 req0 | 121696 | 2035 | 4919 | 6148 | 0 | 0 | 38.0 | 43.0 | 276 |
| fetch/h1/plain/closed-c256/GET /bytes/65536 req0 | 83249 | 3068 | 3482 | 3969 | 0 | 0 | 49.6 | 43.0 | 276 |
| fetch/h2/plain/closed-c64/GET /bytes/1024 req0 | 66438 | 956 | 1428 | 1650 | 0 | 0 | 51.1 | 46.3 | 276 |
| fetch/h2/plain/closed-c64/GET /bytes/65536 req0 | 29670 | 2150 | 3215 | 3643 | 0 | 0 | 90.7 | 61.0 | 276 |
| fetch/h2/plain/closed-c256/GET /bytes/1024 req0 | 65775 | 3891 | 4567 | 5288 | 0 | 0 | 51.6 | 46.3 | 277 |
| fetch/h2/plain/closed-c256/GET /bytes/65536 req0 | 26614 | 9626 | 13681 | 15229 | 0 | 0 | 98.2 | 61.9 | 277 |
| fetch-pooled(pool=4)/h1/plain/closed-c64/GET /bytes/1024 req0 | 127921 | 490 | 949 | 1144 | 0 | 0 | 36.0 | 43.0 | 277 |
| fetch-pooled(pool=4)/h1/plain/closed-c64/GET /bytes/65536 req0 | 90060 | 703 | 1018 | 1725 | 0 | 0 | 44.1 | 43.0 | 277 |
| fetch-pooled(pool=4)/h1/plain/closed-c256/GET /bytes/1024 req0 | 136382 | 1789 | 4506 | 5931 | 0 | 0 | 34.0 | 43.0 | 277 |
| fetch-pooled(pool=4)/h1/plain/closed-c256/GET /bytes/65536 req0 | 84243 | 3033 | 3400 | 4049 | 0 | 0 | 48.7 | 43.0 | 277 |
| fetch-pooled(pool=4)/h2/plain/closed-c64/GET /bytes/1024 req0 | 115697 | 541 | 1065 | 1554 | 0 | 0 | 36.1 | 46.3 | 277 |
| fetch-pooled(pool=4)/h2/plain/closed-c64/GET /bytes/65536 req0 | 67094 | 928 | 1708 | 2159 | 0 | 0 | 61.3 | 61.0 | 277 |
| fetch-pooled(pool=4)/h2/plain/closed-c256/GET /bytes/1024 req0 | 124880 | 2013 | 3746 | 4493 | 0 | 0 | 34.2 | 46.3 | 277 |
| fetch-pooled(pool=4)/h2/plain/closed-c256/GET /bytes/65536 req0 | 72063 | 3471 | 6214 | 7447 | 0 | 0 | 59.8 | 61.0 | 277 |
| hyper-legacy/h2/plain/closed-c64/GET /bytes/1024 req0 | 79490 | 798 | 1229 | 1407 | 0 | 0 | 39.9 | 14.3 | 277 |
| hyper-legacy/h2/plain/closed-c64/GET /bytes/65536 req0 | 56406 | 1123 | 1738 | 1963 | 0 | 0 | 45.8 | 17.0 | 277 |
| fetch/h2/plain/closed-c64/GET /bytes/1024 req0 | 65988 | 963 | 1435 | 1639 | 0 | 0 | 51.4 | 46.3 | 277 |
| fetch/h2/plain/closed-c64/GET /bytes/65536 req0 | 29773 | 2142 | 3215 | 3635 | 0 | 0 | 90.1 | 61.0 | 277 |
| matier(min=auto,max=1)/h2/plain/closed-c64/GET /bytes/1024 req0 | 111845 | 566 | 977 | 1110 | 0 | 0 | 13.4 | 10.0 | 277 |
| matier(min=auto,max=1)/h2/plain/closed-c64/GET /bytes/65536 req0 | 66129 | 955 | 1542 | 1765 | 0 | 0 | 24.0 | 11.0 | 277 |

