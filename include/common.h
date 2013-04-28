
#ifndef INCLUDE__COMMON_H
#define INCLUDE__COMMON_H

#ifdef DEBUG
#define LOG( format, ...) ( fprintf( stderr, format, ##__VA_ARGS__))
#else
#define LOG( format, ...) 
#endif

#define MEASURE_TIME_MS( timerVar, ...) do { \
	struct timeval timerVar##_start, timerVar##_end; \
    long timerVar##_seconds, timerVar##_useconds; \
    gettimeofday( &timerVar##_start, NULL); \
    __VA_ARGS__ \
    gettimeofday( &timerVar##_end, NULL); \
    timerVar##_seconds  = timerVar##_end.tv_sec  - timerVar##_start.tv_sec; \
    timerVar##_useconds = timerVar##_end.tv_usec - timerVar##_start.tv_usec; \
    timerVar = ( timerVar##_seconds * 1000 + timerVar##_useconds / 1000.0); \
} while (0)

struct Request
{
	double startPoint;
	double endPoint;
	double delta;
};
typedef struct Request Request;

struct Response
{
	double timeElapsed;
	double result;
};
typedef struct Response Response;

struct Benchmark
{
	double timeMs;
	double delta;
};
typedef struct Benchmark Benchmark;

struct Interval
{
	double start;
	double end;
};
typedef struct Interval Interval;

#endif  // INCLUDE__COMMON_H 