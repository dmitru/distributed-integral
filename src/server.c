
/*
  server.c

  Author: dmitriy.borodiy@gmail.com

  Usage:
  server <server port> 
         <broadcast address> <broadcast port> 
         <start point> <end point> <delta>
         [<use load balancing>]
         [<maximum number of workers>] [<waiting time in seconds>]

  Desription

  When run, a server sends broadcast message on <broadcast port>. 
  Each worker that receives such a message tries to connect
  to the server on <server port> (which is given to workers as 
  a command line argument), and sends a Benchmark structure,
  which the server then uses to estimate the worker's performance.

  The server divides the work among workers, accordingly
  to their estimated performance, and sends out the 
  tasks to them.

  Then it receives the partial results from the workers, 
  adds them together and prints the overall result of computation.
*/

#include <stdio.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/time.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <errno.h>
#include <stdbool.h>
#include <string.h>

#include "integral.h"
#include "common.h"

#define DEFAULT_NUMBER_OF_WORKERS 16
#define DEFAULT_SECONDS_TO_WAIT 5
#define MAX_SECONDS_TO_WAIT 3600

struct Args
{
  int serverPort;
  struct sockaddr_in broadcastAddress;
  Interval interval;
  double delta;
  bool useLoadBalancing; 
  int maxNumberOfWorkers;
  int waitingTimeSeconds;
};
typedef struct Args Args;

static void printUsageAndDie();
static void printAndDie(const char *msg);
static void printErrorAndDie(const char *msg);
static void parseArgumentsOrDie( int argc, char **argv, Args *argsOut);
static  int createListeningSocketOrDie( int listenPort, int backlog, int timeoutSeconds);
static bool sendBroadcast( struct sockaddr_in broadcastAddress, 
  const char *bytes, size_t length);
static  int recvResponse( int socket, Response *responseOut);
static  int recvBenchmark( int socket, Benchmark *benchmarkOut);
static  int sendRequest( int socket, Request request);
static void computeIntervalsForWorkers( bool useLoadBalancing, Benchmark benchmarks[], 
  int numberOfWorkers, Interval interval, Interval workerIntervalsOut[]);
static void populateWorkerPool( int serverSocket, int maxNumberOfWorkers, int workerSocketsOut[], 
  struct sockaddr_in workerAddressesOut[], int *numberOfWorkersOut);
static void receiveBenchmarksOrDie( int workerSockets[], struct sockaddr_in workerAddresses[], 
  int numberOfWorkers, Benchmark benchmarksOut[]);
static void sendRequestsOrDie( int numberOfWorkers, Interval workerIntervals[], 
  int workerSockets[], struct sockaddr_in workerAddresses[], double delta);
static void gatherResultsOrDie( int numberOfWorkers, int workerSockets[], 
  struct sockaddr_in workerAddresses[], double *answerOut);

int main( int argc, char **argv)
{
  Args args;
  parseArgumentsOrDie( argc, argv, &args);

  int serverSocket = createListeningSocketOrDie( args.serverPort, 
    args.maxNumberOfWorkers, args.waitingTimeSeconds);

  if ( !sendBroadcast( args.broadcastAddress, "hello", 6))
    printErrorAndDie( "Error: can't send broadcast message");

  int workerSockets[ args.maxNumberOfWorkers];
  struct sockaddr_in workerAddresses[ args.maxNumberOfWorkers];
  int numberOfWorkers = 0;
  populateWorkerPool( serverSocket, args.maxNumberOfWorkers, workerSockets, workerAddresses, &numberOfWorkers);
  if ( numberOfWorkers < 1)
    printAndDie( "Sorry, no workers found. Exiting...");

  Benchmark benchmarks[ args.maxNumberOfWorkers];
  receiveBenchmarksOrDie( workerSockets, workerAddresses, numberOfWorkers, benchmarks);

  Interval workerIntervals[ args.maxNumberOfWorkers];
  computeIntervalsForWorkers( args.useLoadBalancing, benchmarks, numberOfWorkers, 
    args.interval, workerIntervals);

  sendRequestsOrDie( numberOfWorkers, workerIntervals, workerSockets, workerAddresses, args.delta);

  double answer;
  gatherResultsOrDie( numberOfWorkers, workerSockets, workerAddresses, &answer);

  close( serverSocket);

  LOG( "Done!\n\n");
  printf( "%.10lf\n", answer);
}

static int createListeningSocketOrDie( int listeningPort, int backlog, int timeoutSeconds)
{
  int listeningSocket = socket( AF_INET, SOCK_STREAM, 0);
  if ( listeningSocket < 0)
    printErrorAndDie("Error when creating listening socket");

  struct sockaddr_in listeningAddr;
  listeningAddr.sin_family = AF_INET;
  listeningAddr.sin_addr.s_addr = htonl( INADDR_ANY);
  listeningAddr.sin_port = htons( listeningPort);

  struct timeval timeout;      
  timeout.tv_sec = timeoutSeconds;
  timeout.tv_usec = 0;

  if ( setsockopt ( listeningSocket, SOL_SOCKET, SO_RCVTIMEO, ( char *) &timeout,
        sizeof( timeout)) < 0)
    printErrorAndDie( "Error when calling setsockopt()");       

  int on = 1;
  if ( setsockopt ( listeningSocket, SOL_SOCKET, SO_REUSEADDR, ( char *) &on,
        sizeof( on)) < 0)
    printErrorAndDie( "Error when calling setsockopt()");       

  if ( bind( listeningSocket, (struct sockaddr*)&listeningAddr, 
        sizeof(listeningAddr)) < 0)
    printErrorAndDie( "Error when binding the listening socket");

  if ( listen( listeningSocket, backlog) < 0)
    printErrorAndDie( "Error when listen() on the listening socket");

  return listeningSocket;
}

static void printUsageAndDie()
{
  fprintf( stderr, "Usage: server <server port> <broadcast address> <broadcast port>\n"
    "       <start point> <end point> <delta> [<use load balancing?>]\n"
    "      [<maximum number of workers>] [<waiting time in seconds>]\n");
  exit( EXIT_FAILURE);
}

static void printAndDie(const char *msg)
{
  fprintf( stderr, "%s\n", msg);
  exit( EXIT_FAILURE);
}

static void printErrorAndDie(const char *msg)
{
  fprintf( stderr, "%s: %s\n", msg, strerror( errno));
  exit( EXIT_FAILURE);
}

static void parseArgumentsOrDie( int argc, char **argv, Args *argsOut)
{
  if ( argc < 7)
    printUsageAndDie();

  int serverPort = atoi( argv[1]);
  int broadcastPort = atoi( argv[3]);

  char *broadcastAddr = argv[2];
  struct in_addr inAddr;
  if ( !inet_aton( broadcastAddr, &inAddr))
    printErrorAndDie( "Error: invalid broadcast address");
  struct sockaddr_in broadcastAddress;
  memset( &broadcastAddress, 0, sizeof( broadcastAddress));
  broadcastAddress.sin_family = AF_INET;
  broadcastAddress.sin_addr.s_addr = inAddr.s_addr;
  broadcastAddress.sin_port = htons(broadcastPort);

  double startPoint = atof( argv[4]);
  double endPoint = atof( argv[5]);
  double delta = atof( argv[6]);

  int useLoadBalancing = 1;
  if ( argc >= 8)
  {
    char *endPtr;
    useLoadBalancing = strtol( argv[ 7], &endPtr, 10);
    if ( endPtr == argv[ 7])
      printAndDie( "Error: <use load balancing> must be 1 or 0");
  }

  if ( delta == 0)
    printAndDie( "Error: <delta> must be a positive real number");

  if ( startPoint > endPoint)
    printAndDie( "Error: <start point> must be lesser than <end point>");    

  int maxNumberOfWorkers = DEFAULT_NUMBER_OF_WORKERS;
  if ( argc >= 9)
  {
    maxNumberOfWorkers = atoi( argv[8]);
    if ( maxNumberOfWorkers < 1)
      printAndDie( "Error: <maximum number of workers> must be a positive integer");
  }

  int waitingTimeSeconds = DEFAULT_SECONDS_TO_WAIT;
  if ( argc >= 10)
  {
    waitingTimeSeconds = atoi( argv[9]);
    if ( waitingTimeSeconds < 1 || waitingTimeSeconds > MAX_SECONDS_TO_WAIT)
      printAndDie( "Error: <waiting time in seconds> must be a positive integer lesser than 3600");
  }

  LOG( "Started at port %d with parameters:\n", serverPort);
  LOG( "    load balancing: %s\n", ( ( useLoadBalancing)? "on" : "off"));
  LOG( "\n");

  argsOut->interval.start = startPoint;
  argsOut->interval.end = endPoint;
  argsOut->delta = delta;
  argsOut->broadcastAddress = broadcastAddress;
  argsOut->serverPort = serverPort;
  argsOut->useLoadBalancing = useLoadBalancing;
  argsOut->maxNumberOfWorkers = maxNumberOfWorkers;
  argsOut->waitingTimeSeconds = waitingTimeSeconds;
}

static bool sendBroadcast( struct sockaddr_in broadcastAddress, const char *bytes, size_t length)
{
  LOG( "Sending broadcast message...\n"); 
  int broadcastSocket = socket( AF_INET, SOCK_DGRAM, 0);
  int optValue = 1;
  socklen_t optLength = sizeof( optValue);
  if ( setsockopt( broadcastSocket, SOL_SOCKET, SO_BROADCAST, &optValue, optLength) < 0)
  {
    close( broadcastSocket);
    return false;
  }
  if ( sendto( broadcastSocket, bytes, length, 0, (struct sockaddr *) &broadcastAddress, 
    sizeof( broadcastAddress)) < 0)
  {
    close( broadcastSocket);
    return false; 
  }
  close( broadcastSocket);
  LOG( "Broadcast message sent. Now waiting for workers...\n");
  return true;
}

static int acceptWorker( int serverSocket, int *workerSocketOut, 
  struct sockaddr_in *workerAddressOut)
{
  struct sockaddr_in workerAddress;
  socklen_t workerAddressLength = sizeof( workerAddress);
  int workerSocket = accept( serverSocket, 
    (struct sockaddr *) &workerAddress, &workerAddressLength);

  if ( workerSocket < 0)
    return -1;

  struct timeval timeout;      
  timeout.tv_sec = 0;
  timeout.tv_usec = 0;
  if ( setsockopt ( workerSocket, SOL_SOCKET, SO_RCVTIMEO, ( char *) &timeout,
        sizeof( timeout)) < 0)
    return -1;

  *workerSocketOut = workerSocket;
  *workerAddressOut = workerAddress;
  return 0;
}

static void computeIntervalsForWorkersWithLoadBalancing( Benchmark *benchmarks, int numberOfWorkers,
    Interval interval, Interval *workerIntervalsOut)
{
  double performanceIndeces[ numberOfWorkers];
  double sumOfPerformanceIndeces = 0.0l;
  for ( int i = 0; i < numberOfWorkers; ++i)
  {
    double performanceIndecex = 1e-6 / ( benchmarks[ i].timeMs * benchmarks[ i].delta);
    sumOfPerformanceIndeces += performanceIndecex;
    performanceIndeces[ i] = performanceIndecex;
  }
  
  double lastEnd = interval.start;
  double intervalLength = interval.end - interval.start;
  for ( int i = 0; i < numberOfWorkers; ++i) 
  {
    double workerIntervalLength = 
      intervalLength * ( performanceIndeces[ i] / sumOfPerformanceIndeces);
    workerIntervalsOut[ i].start = lastEnd;
    workerIntervalsOut[ i].end = lastEnd + workerIntervalLength;
    lastEnd += workerIntervalLength;
  }
}

static void computeIntervalsForWorkers( bool useLoadBalancing, Benchmark benchmarks[], 
  int numberOfWorkers, Interval interval, Interval workerIntervalsOut[])
{
  if ( useLoadBalancing)
  {  
    computeIntervalsForWorkersWithLoadBalancing( benchmarks, numberOfWorkers, 
      interval, workerIntervalsOut);
  }
  else
  {
    double d = ( interval.end - interval.start) / numberOfWorkers;
    for ( int i = 0; i < numberOfWorkers; ++i)
    {
      workerIntervalsOut[ i].start = interval.start + d * i;
      workerIntervalsOut[ i].end = interval.start + d * (i + 1);
    }
  }
}

static int recvResponse( int socket, Response *responseOut)
{
  Response response;
  int recvStatus = recv( socket, &response, sizeof( response), 0);
  if ( recvStatus != sizeof( response))
    return -1;
  *responseOut = response;
  return 0;
}

static int recvBenchmark( int socket, Benchmark *benchmarkOut)
{
  Benchmark benchmark;
  int recvStatus = recv( socket, &benchmark, sizeof( benchmark), 0);
  if ( recvStatus != sizeof( benchmark))
    return recvStatus;
  *benchmarkOut = benchmark;
  return 0;
}

static int sendRequest( int socket, Request request)
{
  int sendStatus = send( socket, &request, sizeof( request), MSG_NOSIGNAL);
  if ( sendStatus != sizeof( request))
    return sendStatus;
  return 0;
}

static void populateWorkerPool( int serverSocket, int maxNumberOfWorkers, int workerSocketsOut[], 
  struct sockaddr_in workerAddressesOut[], int *numberOfWorkersOut)
{
  int numberOfWorkers = 0;
  while ( numberOfWorkers < maxNumberOfWorkers)
  {
    int workerSocket;
    struct sockaddr_in workerAddress;
    if ( acceptWorker( serverSocket, &workerSocket, &workerAddress))
    {
      if ( errno == EWOULDBLOCK)  // timeout
        break;
      LOG( "Error when connecting to worker %s:%d\n", 
        inet_ntoa( workerAddress.sin_addr),
        ntohs( workerAddress.sin_port));
    } 
    LOG( "Connected to worker %s:%d\n", 
      inet_ntoa( workerAddress.sin_addr),
      ntohs( workerAddress.sin_port));
    workerSocketsOut[ numberOfWorkers] = workerSocket;
    workerAddressesOut[ numberOfWorkers] = workerAddress;
    numberOfWorkers ++;
  }

  *numberOfWorkersOut = numberOfWorkers;
}

static void receiveBenchmarksOrDie( int workerSockets[], struct sockaddr_in workerAddresses[], 
  int numberOfWorkers, Benchmark benchmarksOut[])
{
  for ( int i = 0; i < numberOfWorkers; ++i)
  {
    Benchmark benchmark;
    if ( recvBenchmark( workerSockets[ i], &benchmark))
      printErrorAndDie( "Error: can't receive benchmark from a worker");
    LOG( "Received benchmark from %s:%d:\n    %.12lf ms\n", 
      inet_ntoa( workerAddresses[ i].sin_addr),
      ntohs( workerAddresses[ i].sin_port),
      benchmark.timeMs);
    benchmarksOut[ i] = benchmark;
  }
}

static void sendRequestsOrDie( int numberOfWorkers, Interval workerIntervals[], 
  int workerSockets[], struct sockaddr_in workerAddresses[], double delta)
{
  for ( int i = 0; i < numberOfWorkers; ++i)
  {
    Request request;
    request.startPoint = workerIntervals[ i].start;
    request.endPoint = workerIntervals[ i].end;
    request.delta = delta;
    if ( sendRequest( workerSockets[ i], request))
      printErrorAndDie( "Error: can't send request to a worker");
    LOG( "Sent request to worker %s:%d\n", 
      inet_ntoa( workerAddresses[ i].sin_addr),
      ntohs( workerAddresses[ i].sin_port));
  }

  LOG( "All requests are sent; now waiting for responses...\n");
}

static void gatherResultsOrDie( int numberOfWorkers, int workerSockets[], 
  struct sockaddr_in workerAddresses[], double *answerOut)
{
  double answer = 0.0f;
  for ( int i = 0; i < numberOfWorkers; ++i)
  {
    Response response;
    if ( recvResponse( workerSockets[ i], &response))
      printErrorAndDie( "Error: can't get response from a worker");
    LOG( "Received response from worker %s:%d\n    Result: %.10lf\n    Time: %.3lf ms\n",
      inet_ntoa( workerAddresses[ i].sin_addr), ntohs( workerAddresses[ i].sin_port), 
      response.result, response.timeElapsed);
    answer += response.result;
    close( workerSockets[ i]);
  }
  *answerOut = answer;
}
