
/*
  worker.c

  Author: dmitriy.borodiy@gmail.com

  Usage:
  worker <listening port> <server port> [<number of threads>]

  Desription

  The program listens to a port <listening port> and waits 
  for any message to come from a server.

  On receiving a message, the program connects to the server
  to port <server port>. After that, it receives 
  the starting and ending points of integration interval and 
  the integration step from the server.

  Then the program computes the integral (the function
  being hard-coded), possibly with many threads,
  sends the result back to the server,
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

#include "integral.h"
#include "common.h"

static void printUsageAndDie();
static void printErrorAndDie(const char *msg);
static void parseArgumentsOrDie( int argc, char **argv, int *listenPortOut, 
  int *serverPortOut, int *numberOfThreadsOut);
static  int createWorkerSocketOrDie( int listenPort);
static  int waitForRequest( int workerSocket, struct sockaddr_in *serverAddressOut);
static  int connectToServer( struct sockaddr_in serverAddress, int *serverSocketOut);
static  int recvRequest( int serverSocket, Request *requestOut);
static  int sendResponse( int serverSocket, Response response);

static double functionToIntegrate( double x)
{
  return x;
}

int main( int argc, char **argv)
{
  int listeningPort;
  int serverPort;
  int numberOfThreads;
  parseArgumentsOrDie( argc, argv, &listeningPort, &serverPort, &numberOfThreads);
  int workerSocket = createWorkerSocketOrDie( listeningPort);

  while ( 1)
  {
    struct sockaddr_in serverAddress;
    if ( waitForRequest( workerSocket, &serverAddress) < 0)
      printErrorAndDie( "Error when processing a request");
    
    LOG( "Request received from %s\n", inet_ntoa( serverAddress.sin_addr));
    serverAddress.sin_port = htons( serverPort);
    int serverSocket;
    if ( connectToServer( serverAddress, &serverSocket)) 
    {
      LOG( "Failed to connect to server at %s:%d\n", inet_ntoa( serverAddress.sin_addr),
        ntohs( serverAddress.sin_port));
      continue;
    }
    LOG( "Connected to %s:%d\n", inet_ntoa( serverAddress.sin_addr),
      ntohs( serverAddress.sin_port));

    Request request;
    if ( recvRequest( serverSocket, &request))
    {
      LOG( "Error when receiving request from %s:%d\n", inet_ntoa( serverAddress.sin_addr),
        ntohs( serverAddress.sin_port));
    }
    else
    {
      LOG( "Received request from %s:%d\n", inet_ntoa( serverAddress.sin_addr),
        ntohs( serverAddress.sin_port));

      LOG( "Start point: %.8lf\n", request.startPoint); 
      LOG( "End point: %.8lf\n", request.endPoint);
      LOG( "Delta: %.16lf\n", request.delta);

      LOG( "Computing the result using %d thread(s)...\n", numberOfThreads);

      struct timeval start, end;
      long seconds, useconds;    
      gettimeofday( &start, NULL);

      Response response;
      integrate( functionToIntegrate, request.startPoint, request.endPoint,
        numberOfThreads, request.delta, &response.result);

      gettimeofday( &end, NULL);
      seconds  = end.tv_sec  - start.tv_sec;
      useconds = end.tv_usec - start.tv_usec;
      response.timeElapsed = ( seconds * 1000 + useconds / 1000.0);

      LOG( "The result is %.8lf\n", response.result);
      LOG( "It was computed in %.3lf ms\n", response.timeElapsed);

      if ( sendResponse( serverSocket, response))
        // TODO: The error is not caught
        LOG( "Failed to send the result to %s:%d\n", 
          inet_ntoa( serverAddress.sin_addr),
          ntohs( serverAddress.sin_port));
      else
        LOG( "The result is sent to %s:%d\n", 
          inet_ntoa( serverAddress.sin_addr),
          ntohs( serverAddress.sin_port));
    }

    close( serverSocket);
  } 

  close( workerSocket);
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

static int waitForRequest( int workerSocket, struct sockaddr_in *serverAddressOut)
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

static int connectToServer( struct sockaddr_in serverAddress, 
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

static void parseArgumentsOrDie( int argc, char **argv, int *listenPortOut, 
  int *serverPortOut, int *numberOfThreadsOut)
{
  if ( argc < 3)
    printUsageAndDie();
  *listenPortOut = atoi( argv[1]);
  *serverPortOut = atoi( argv[2]);
  int numberOfThreads = 1;
  if ( argc >= 4)
  {
    numberOfThreads = atoi( argv[3]);
    if ( numberOfThreads < 1)
      printErrorAndDie( "Error: <number of threads> must be a positive integer");
  }
  *numberOfThreadsOut = numberOfThreads;
}

static int recvRequest( int serverSocket, Request *requestOut)
{
  Request request;
  int recvStatus = recv( serverSocket, &request, sizeof( request), 0);
  if ( recvStatus < 0)
    return recvStatus;
  *requestOut = request;
  return 0;
}

static int sendResponse( int serverSocket, Response response)
{
  int sendStatus = send( serverSocket, &response, sizeof( response), MSG_NOSIGNAL);
  if ( sendStatus < 0)
    return sendStatus;
  return 0;
}
