Classification: Public
Implement multithread binary file sort utility. Number of threads and input file should be configurable from command line. Input file can be large.

Implementation requirements and environment:
- Java 1.6+
- maximum head size for single thread -Xmx8m -XX:MaxPermSize=8m and increase maximum heap size by 1m per thread
- input file size: small, medium, hundred megabytes or few gigabytes; file size is always multiple of four.
- you may allocate additional disk space but not larger than the input file size
- input file structure: sequence of 32-bit signed integer numbers in binary format (big-endian) with no separators and additional markers
- recommended test input data: descending sorted numbers with size, at least 8 millions numbers
- 1Gb input file shouldn't be processed longer than 15 minutes
- input file should contain sorted numbers at finish and it shouldn't be any garbage in memory or filesystem

Analyze performance and explain how performance depends on threads count, internal implementation parameters and possibly other environment conditions.