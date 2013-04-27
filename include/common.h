
#ifndef INCLUDE__COMMON_H
#define INCLUDE__COMMON_H

#ifdef DEBUG
#define LOG( format, ...) ( fprintf( stderr, format, ##__VA_ARGS__))
#else
#define LOG( format, ...) 
#endif

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

#endif  // INCLUDE__COMMON_H 