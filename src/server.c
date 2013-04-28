
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
#include <string.h>

#include "integral.h"
#include "common.h"

static void printUsageAndDie();
static void printAndDie(const char *msg);
static void printErrorAndDie(const char *msg);
static void parseArgumentsOrDie( int argc, char **argv, int *serverPortOut, 
  struct sockaddr_in *broadcastAddressOut, 
  double *startPointOut, double *endPointOut, double *deltaOut,
  int *useLoadBalancingOut, 
  int *maxNumberOfWorkersOut, int *waitingTimeSecondsOut);
static  int createListeningSocketOrDie( int listenPort, int backlog, int timeoutSeconds);
static  int sendBroadcast( struct sockaddr_in broadcastAddress, 
  const char *bytes, size_t length);
static  int acceptWorker( int serverSocket, int *workerSocketOut, 
  struct sockaddr_in *workerAddressOut);
static  int recvResponse( int socket, Response *responseOut);
static  int recvBenchmark( int socket, Benchmark *benchmarkOut);
static  int sendRequest( int socket, Request request);
static void computeIntervalsForWorkers( Benchmark *benchmarks, int numberOfWorkers,
    Interval interval, Interval *workerIntervalsOut);

int main( int argc, char **argv)
{
  int serverPort;
  int maxNumberOfWorkers;
  int waitingTimeSeconds;
  int useLoadBalancing;
  struct sockaddr_in broadcastAddress;
  double startPoint, endPoint, delta;
  parseArgumentsOrDie( argc, argv, &serverPort, &broadcastAddress, 
    &startPoint, &endPoint, &delta,
    &useLoadBalancing,
    &maxNumberOfWorkers, &waitingTimeSeconds);
  int serverSocket = createListeningSocketOrDie( serverPort, 
    maxNumberOfWorkers, waitingTimeSeconds);
  
  LOG( "Started at port %d with parameters:\n", serverPort);
  LOG( "    load balancing: %s\n", ( ( useLoadBalancing)? "on" : "off"));
  LOG( "\n");

  LOG( "Sending broadcast message...\n"); 
  if ( sendBroadcast( broadcastAddress, "hello", 6))
    printErrorAndDie( "Error: can't send broadcast message");
  LOG( "Broadcast message sent. Now waiting for workers...\n");

  int numberOfWorkers = 0;
  int workerSockets[ maxNumberOfWorkers];
  struct sockaddr_in workerAddresses[ maxNumberOfWorkers];
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
    else 
    {
      LOG( "Connected to worker %s:%d\n", 
        inet_ntoa( workerAddress.sin_addr),
        ntohs( workerAddress.sin_port));
      workerSockets[ numberOfWorkers] = workerSocket;
      workerAddresses[ numberOfWorkers] = workerAddress;
      numberOfWorkers ++;
    }
  }

  if ( numberOfWorkers < 1)
    printAndDie( "No available workers found, exiting...");

  Benchmark benchmarks[ maxNumberOfWorkers];
  for ( int i = 0; i < numberOfWorkers; ++i)
  {
    if ( recvBenchmark( workerSockets[ i], &benchmarks[ i]))
      printErrorAndDie( "Error: can't receive benchmark from a worker");
    LOG( "Received benchmark from %s:%d:\n    %.12lf ms\n", 
      inet_ntoa( workerAddresses[ i].sin_addr),
      ntohs( workerAddresses[ i].sin_port),
      benchmarks[ i].timeMs);
  }

  Interval workerIntervals[ maxNumberOfWorkers];
  if ( useLoadBalancing)
  {  
    Interval interval;
    interval.start = startPoint;
    interval.end = endPoint;
    computeIntervalsForWorkers( benchmarks, numberOfWorkers, 
      interval, workerIntervals);
  }
  else
  {
    double d = ( endPoint - startPoint) / numberOfWorkers;
    for ( int i = 0; i < numberOfWorkers; ++i)
    {
      workerIntervals[ i].start = startPoint + d * i;
      workerIntervals[ i].end = startPoint + d * (i + 1);
    }
  }
  
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
    "       <start point> <end point> <delta>\n"
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

static void parseArgumentsOrDie( int argc, char **argv, int *serverPortOut, 
  struct sockaddr_in *broadcastAddressOut, 
  double *startPointOut, double *endPointOut, double *deltaOut, 
  int *useLoadBalancingOut,
  int *maxNumberOfWorkersOut, int *waitingTimeSecondsOut)
{
  if ( argc < 7)
    printUsageAndDie();

  *serverPortOut = atoi( argv[1]);
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
  *broadcastAddressOut = broadcastAddress;

  *startPointOut = atof( argv[4]);
  *endPointOut = atof( argv[5]);
  *deltaOut = atof( argv[6]);

  int useLoadBalancing = 1;
  if ( argc >= 8)
  {
    char *endPtr;
    useLoadBalancing = strtol( argv[ 7], &endPtr, 10);
    if ( endPtr == argv[ 7])
      printErrorAndDie( "Error: <use load balancing> must be 1 or 0");
  }
  *useLoadBalancingOut = useLoadBalancing;

  if ( *deltaOut == 0)
    printErrorAndDie( "Error: <delta> must be a positive real number");

  if ( *startPointOut > *endPointOut)
    printErrorAndDie( "Error: <start point> must be lesser than <end point>");    

  int maxNumberOfWorkers = 1024;
  if ( argc >= 9)
  {
    maxNumberOfWorkers = atoi( argv[8]);
    if ( maxNumberOfWorkers < 1)
      printErrorAndDie( "Error: <maximum number of workers> must be a positive integer");
  }
  *maxNumberOfWorkersOut = maxNumberOfWorkers;

  int waitingTimeSeconds = 3;
  if ( argc >= 10)
  {
    waitingTimeSeconds = atoi( argv[9]);
    if ( waitingTimeSeconds < 1)
      printErrorAndDie( "Error: <waiting time in seconds> must be a positive integer");
  }
  *waitingTimeSecondsOut = waitingTimeSeconds;
}

static int sendBroadcast( struct sockaddr_in broadcastAddress, const char *bytes, size_t length)
{
  int broadcastSocket = socket( AF_INET, SOCK_DGRAM, 0);
  int optValue = 1;
  socklen_t optLength = sizeof( optValue);
  if ( setsockopt( broadcastSocket, SOL_SOCKET, SO_BROADCAST, &optValue, optLength) < 0)
  {
    close( broadcastSocket);
    return -1;
  }
  if ( sendto( broadcastSocket, bytes, length, 0, (struct sockaddr *) &broadcastAddress, 
    sizeof( broadcastAddress)) < 0)
  {
    close( broadcastSocket);
    return -1; 
  }
  close( broadcastSocket);
  return 0;
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

static void computeIntervalsForWorkers( Benchmark *benchmarks, int numberOfWorkers,
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

static int recvResponse( int socket, Response *responseOut)
{
  Response response;
  int recvStatus = recv( socket, &response, sizeof( response), 0);
  if ( recvStatus != sizeof( response))
    return recvStatus;
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