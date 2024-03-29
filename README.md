# GoLogMatrix
GoLogMatrix: A high-performance Write Ahead Log (WAL) implementation in Go, optimizing sequential data operations for durability, efficiency, and concurrency.

# Benchmark Report

## System Specifications

- Operating System: Darwin
- Architecture: amd64
- CPU: Intel(R) Core(TM) i7-9750H CPU @ 2.60GHz

## Write Throughput

- Average Write Throughput: 362748.826031 entries/sec
- Benchmark Results:
  - BenchmarkWriteThroughput-12: 1 iteration, average time/op: 27606327162 ns/op

## Read Throughput

## Read Throughput

- Read Throughput Data:
  - 1. 73037739.958739 entries/sec
  - 2. 86811862.979975 entries/sec
  - 3. 81466642.664421 entries/sec
  - 4. 79318426.910318 entries/sec
  - 5. 87802423.264348 entries/sec
  - 6. 98858224.089311 entries/sec
  - 7. 87541712.969667 entries/sec
  - 8. 127440391.095189 entries/sec
  - 9. 95307441.840468 entries/sec
  - 10. 102740723.879085 entries/sec
  - 11. 97362184.825342 entries/sec
  - 12. 99515239.375649 entries/sec
  - 13. 104447312.242691 entries/sec
  - 14. 96782439.724453 entries/sec
  - 15. 104312695.747989 entries/sec
  - 16. 99664035.526162 entries/sec
  - 17. 125950175.999627 entries/sec
  - 18. 100666404.551482 entries/sec
  - 19. 103411143.773061 entries/sec
  - 20. 98106134.650179 entries/sec
  - 21. 92956667.710198 entries/sec
  - 22. 107951823.044508 entries/sec
  - 23. 94345075.074810 entries/sec
  - 24. 92782444.373909 entries/sec
  - 25. 120563161.135221 entries/sec
  - 26. 101296592.329960 entries/sec

- Average Read Throughput: 97348448.623486 entries/sec
- Benchmark Result: 10 iterations, average time/op: 100521993 ns/op

## Concurrent Write Throughput

- Average Concurrent Write Throughput: 14582.800808 entries/sec
- Benchmark Results:
  - BenchmarkConcurrentWriteThroughPut-12: 1 iteration, average time/op: 68595948875 ns/op


