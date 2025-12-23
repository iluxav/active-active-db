# Redis vs Counter-DB Comparison

## === a2db Full Benchmark Suite Last Run Before the optimization===

--- Concurrency Test ---
Clients: 1
INCR: 17531.56 requests per second, p50=0.055 msec  
Clients: 10
INCR: 167785.23 requests per second, p50=0.039 msec  
Clients: 50
INCR: 185873.61 requests per second, p50=0.135 msec  
Clients: 100
INCR: 197238.64 requests per second, p50=0.239 msec  
Clients: 200
INCR: 197238.64 requests per second, p50=0.479 msec  
Clients: 500
INCR: 188679.25 requests per second, p50=1.207 msec

--- Pipeline Test ---
Pipeline: 1
INCR: 216450.20 requests per second, p50=0.215 msec  
Pipeline: 10
INCR: 458715.59 requests per second, p50=2.119 msec  
Pipeline: 50
INCR: 529100.56 requests per second, p50=9.239 msec  
Pipeline: 100
INCR: 563380.31 requests per second, p50=17.247 msec

## === a2db Full Benchmark Suite after the optimization with the match instead of if-else ===

--- Concurrency Test ---
Clients: 1
INCR: 17828.49 requests per second, p50=0.055 msec  
Clients: 10
INCR: 170940.17 requests per second, p50=0.039 msec  
Clients: 50
INCR: 179533.22 requests per second, p50=0.135 msec  
Clients: 100
INCR: 196850.39 requests per second, p50=0.239 msec  
Clients: 200
INCR: 197628.47 requests per second, p50=0.471 msec  
Clients: 500
INCR: 186219.73 requests per second, p50=1.255 msec

--- Pipeline Test ---
Pipeline: 1
INCR: 208333.34 requests per second, p50=0.223 msec  
Pipeline: 10
INCR: 1176470.62 requests per second, p50=0.623 msec  
Pipeline: 50
INCR: 722021.62 requests per second, p50=5.839 msec  
Pipeline: 100
INCR: 754717.00 requests per second, p50=9.615 msec

## === Redis Full Benchmark Suite ===

--- Concurrency Test ---
Clients: 1
INCR: 9914.73 requests per second, p50=0.095 msec  
Clients: 10
INCR: 64432.99 requests per second, p50=0.127 msec  
Clients: 50
INCR: 152671.77 requests per second, p50=0.247 msec  
Clients: 100
INCR: 173310.22 requests per second, p50=0.431 msec  
Clients: 200
INCR: 209643.61 requests per second, p50=0.695 msec  
Clients: 500
INCR: 223214.28 requests per second, p50=1.519 msec

--- Pipeline Test ---
Pipeline: 1
INCR: 198412.69 requests per second, p50=0.391 msec  
Pipeline: 10
INCR: 729927.06 requests per second, p50=1.095 msec  
Pipeline: 50
INCR: 1020408.19 requests per second, p50=3.887 msec  
Pipeline: 100
INCR: 1212121.12 requests per second, p50=7.311 msec
