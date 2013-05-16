
/*
  worker.c

  Author: dmitriy.borodiy@gmail.com

  Usage:
  worker <listening port> <server port> [<number of threads>] 
         [<benchmark delta>]

  Desription

  When run, the program estimates its performance by measuring
  how much time it takes to take the integral over [0, 1] with
  the specified delta <benchmark delta>. 

  The program listens to a port <listening port> and waits 
  for any message to come from a server.

  On receiving a message, the program connects to the server
  to port <server port>. Then, it sends the server the 
  measured time and <benchmark delta> in a Benchmark structure. 
  After that, it receives the starting and ending points of 
  integration interval and the integration step from the 
  server in a Request structure.

  Then the program computes the integral (the function
  being hard-coded), possibly with many threads,
  sends the result back to the server in a Response structure 
  closes the socket to server and waits for another request.
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
#include <stdbool.h>

#include "integral.h"
#include "common.h"

struct Args
{
  int listeningPort; 
  int serverPort; 
  int numberOfThreads;
  double benchmarkDelta;
};
typedef struct Args Args;

static void parseArgumentsOrDie( int argc, char **argv, Args *argsOut);
static  int createWorkerSocketOrDie( int listenPort);
static bool waitForServerAddress( int workerSocket, int serverPort, struct sockaddr_in *serverAddressOut);
static bool createServerSocket( struct sockaddr_in serverAddress, int *serverSocketOut);
static bool receiveRequest( int serverSocket, struct sockaddr_in serverAddress, Request *requestOut);
static bool computeIntegral( Request request, int numberOfThreads, Response *responseOut);
static bool sendResponse( int serverSocket, struct sockaddr_in serverAddress, Response response);
static void doBenchmark( int numberOfThreads, double benchmarkDelta, Benchmark *benchmarkOut);
static bool sendBenchmark( int serverSocket, struct sockaddr_in serverAddress, Benchmark benchmark);

static double functionToIntegrate( double x)
{
  return x * x;
}

int main( int argc, char **argv)
{
  Args args;
  parseArgumentsOrDie( argc, argv, &args);

  Benchmark benchmark;
  doBenchmark( args.numberOfThreads, args.benchmarkDelta, &benchmark);

  int workerSocket = createWorkerSocketOrDie( args.listeningPort);

  for( ;;) 
  {
    struct sockaddr_in serverAddress;
    if ( !waitForServerAddress( workerSocket, args.serverPort, &serverAddress))
      continue;
    
    int serverSocket;
    if ( !createServerSocket( serverAddress, &serverSocket))
      continue;

    if ( !sendBenchmark( serverSocket, serverAddress, benchmark)) 
    {
      close( serverSocket);
      continue;    
    }

    Request request;
    if ( !receiveRequest( serverSocket, serverAddress, &request)) 
    {
      close( serverSocket);
      continue;
    }

    Response response;
    if ( !computeIntegral( request, args.numberOfThreads, &response)) 
    {
      close( serverSocket);
      continue;
    }

    if ( !sendResponse( serverSocket, serverAddress, response)) 
    {
      close( serverSocket);
      continue;
    }

    close( serverSocket);
  } 

  close( workerSocket);
}

static void printUsageAndDie()
{
  fprintf( stderr, "Usage: worker <listening port> <server port> "
    "[<number of threads>]\n");
  exit( EXIT_FAILURE);
}

static void printErrorAndDie(const char *msg)
{
  fprintf( stderr, "%s: %s\n", msg, strerror( errno));
  exit( EXIT_FAILURE);
}

static int createWorkerSocketOrDie( int listeningPort)
{
  int workerSocket = socket( AF_INET, SOCK_DGRAM, 0);
  if ( workerSocket < 0)
    printErrorAndDie("Error when creating worker socket");

  struct sockaddr_in listeningAddr;
  listeningAddr.sin_family = AF_INET;
  listeningAddr.sin_addr.s_addr = htonl( INADDR_ANY);
  listeningAddr.sin_port = htons( listeningPort);

  if ( bind( workerSocket, (struct sockaddr*)&listeningAddr, 
        sizeof(listeningAddr)) < 0)
    printErrorAndDie( "Error when binding the worker socket");

  return workerSocket;
}

static int waitForServerAddressHelper( int workerSocket, 
  struct sockaddr_in *serverAddressOut)
{
  struct sockaddr_in serverAddress;
  socklen_t addressLength = sizeof( serverAddress);
  char buf;

  ssize_t recvStatus = recvfrom( workerSocket, ( void*) &buf, 1,
    0, ( struct sockaddr*) &serverAddress, &addressLength);

  if ( recvStatus > 0)
    *serverAddressOut = serverAddress;

  return recvStatus;
}

static bool waitForServerAddress( int workerSocket, int serverPort, 
  struct sockaddr_in *serverAddressOut)
{
  int error;
  error = waitForServerAddressHelper( workerSocket, serverAddressOut);
  if ( !error) 
  {
    LOG( "Error when processing a request");
    return false;
  }
  serverAddressOut->sin_port = htons( serverPort);
  LOG( "Request received from %s\n", inet_ntoa( serverAddressOut->sin_addr));
  return true;
}

static int createServerSocketHelper( struct sockaddr_in serverAddress, 
  int *serverSocketOut)
{
  int serverSocket = socket( AF_INET, SOCK_STREAM, 0);
  int connectStatus = connect( serverSocket, (struct sockaddr*) &serverAddress, 
    sizeof( serverAddress));
  if ( connectStatus)
    return connectStatus;
  *serverSocketOut = serverSocket;
  return 0;
}

static bool createServerSocket( struct sockaddr_in serverAddress, 
  int *serverSocketOut)
{
  int error = createServerSocketHelper( serverAddress, serverSocketOut);
  if ( error) 
  {
    LOG( "Failed to connect to server at %s:%d\n", inet_ntoa( serverAddress.sin_addr),
      ntohs( serverAddress.sin_port));
    return false;
  }
  LOG( "Connected to %s:%d\n", inet_ntoa( serverAddress.sin_addr),
    ntohs( serverAddress.sin_port));
  return true;
}

static void parseArgumentsOrDie( int argc, char **argv, Args *argsOut)
{
  if ( argc < 3)
    printUsageAndDie();
  argsOut->listeningPort = atoi( argv[1]);
  argsOut->serverPort = atoi( argv[2]);

  int numberOfThreads = 1;
  if ( argc >= 4)
  {
    numberOfThreads = atoi( argv[3]);
    if ( numberOfThreads < 1)
      printErrorAndDie( "Error: <number of threads> must be a positive integer");
  }
  argsOut->numberOfThreads = numberOfThreads;

  argsOut->benchmarkDelta = 10e-9;
  if ( argc >= 5)
  {
    argsOut->benchmarkDelta = atof( argv[4]);
    if ( argsOut->benchmarkDelta <= 0)
      printErrorAndDie( "Error: <benchmark delta> must be a positive real number");
  }
}

static bool receiveRequestHelper( int serverSocket, Request *requestOut)
{
  Request request;
  int recvStatus = recv( serverSocket, &request, sizeof( request), 0);
  if ( recvStatus != sizeof( request))
    return false;
  *requestOut = request;
  return true;
}

static bool receiveRequest( int serverSocket, struct sockaddr_in serverAddress, Request *requestOut)
{
  int is_ok = receiveRequestHelper( serverSocket, requestOut);
  if ( !is_ok)
  {
    LOG( "Error when receiving task from %s:%d\n", inet_ntoa( serverAddress.sin_addr),
      ntohs( serverAddress.sin_port));
    return false;
  }
  LOG( "Received task from %s:%d\n", inet_ntoa( serverAddress.sin_addr),
    ntohs( serverAddress.sin_port));
  LOG( "Start point: %.8lf\n", requestOut->startPoint); 
  LOG( "End point: %.8lf\n", requestOut->endPoint);
  LOG( "Delta: %.16lf\n", requestOut->delta);
  return true;
}

static bool sendResponseHelper( int serverSocket, Response response)
{
  int bytesCount = sizeof( response);
  int sentBytesCount = send( serverSocket, &response, bytesCount, MSG_NOSIGNAL);
  if ( sentBytesCount != bytesCount)
    return -1;
  return 0;
}

static bool sendResponse( int serverSocket, struct sockaddr_in serverAddress, Response response)
{
  bool is_ok = sendResponseHelper( serverSocket, response);
  if ( is_ok)
  {
    LOG( "Failed to send the result to %s:%d\n", 
      inet_ntoa( serverAddress.sin_addr),
      ntohs( serverAddress.sin_port));
    return false;
  }

  LOG( "The result is sent to %s:%d\n", 
    inet_ntoa( serverAddress.sin_addr),
    ntohs( serverAddress.sin_port));
  return true;
}

static void doBenchmark( int numberOfThreads, double benchmarkDelta, Benchmark *benchmarkOut)
{
  LOG( "Running benchmark with delta = %.12lf...\n", benchmarkDelta);
  double benchmarkTimeMs;
  MEASURE_TIME_MS( 
    benchmarkTimeMs, 
    {
      integrate( functionToIntegrate, 0.0f, 1.0f,
        numberOfThreads, benchmarkDelta, benchmarkOut);
    }
  );
  benchmarkOut->timeMs = benchmarkTimeMs;
  benchmarkOut->delta = benchmarkDelta;
  LOG( "Done! Benchmark time is %.6lf ms\n", benchmarkTimeMs);
  LOG( "Now waiting for requests...\n");
}

static bool sendBenchmarkHelper( int serverSocket, Benchmark benchmark)
{
  int sendStatus = send( serverSocket, &benchmark, sizeof( benchmark), MSG_NOSIGNAL);
  if ( sendStatus != sizeof( benchmark))
    return false;
  return true;
}

static bool sendBenchmark( int serverSocket, struct sockaddr_in serverAddress, Benchmark benchmark)
{
  LOG( "Sending benchmark to %s:%d\n", inet_ntoa( serverAddress.sin_addr),
    ntohs( serverAddress.sin_port));
  bool is_ok = sendBenchmarkHelper( serverSocket, benchmark);
  if ( !is_ok)
  {
    LOG( "Error when sending benchmark to %s:%d\n", inet_ntoa( serverAddress.sin_addr),
      ntohs( serverAddress.sin_port));
  }
  return is_ok;
}

static bool computeIntegral( Request request, int numberOfThreads, Response *responseOut)
{
  LOG( "Computing the result using %d thread(s)...\n", numberOfThreads);
  Response response;
  double msElapsed;
  MEASURE_TIME_MS( 
    msElapsed, 
    {
      if ( integrate( functionToIntegrate, request.startPoint, request.endPoint,
              numberOfThreads, request.delta, &response.result)) 
      {
        LOG( "Error when computing integral\n");
        return false;
      }
    }
  );
  response.timeElapsed = msElapsed;
  LOG( "The result is %.8lf\n", response.result);
  LOG( "It was computed in %.3lf ms\n", response.timeElapsed);

  *responseOut = response;
  return true;
}