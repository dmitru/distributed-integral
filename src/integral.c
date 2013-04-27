
#include <pthread.h>
#include <stdlib.h>
#include <stdio.h>

#include "integral.h"

struct Task {
  double a;
  double b;
  double delta;
  double (*f)(double);
};

typedef struct Task Task;

static void* thread_integrate(void *arg)
{
  Task *task = (Task*)arg;
  double a = task->a;
  double b = task->b;
  double delta = task->delta;
  double (*f)(double) = task->f;
  free(task);
  double *ans = (double*)malloc(sizeof(double));
  if (!ans)
    return NULL;

  double res = 0.0;
  for (double x = a; x + delta <= b; x += delta) {
    double y1 = f(x);
    double y2 = f(x + delta);
    res += delta * (y2 + y1);
  }

  *ans = res / 2.0;

  return ans;
}

int integrate(double (*f)(double), double a, double b, 
  int n_threads, double delta, double *res)
{
  if (n_threads < 1) {
    return 1;
  }
  pthread_t *threads_handles = (pthread_t*) malloc(n_threads * sizeof(pthread_t));
  if (threads_handles == NULL) {
    return 2;
  }

  double d = (b - a) / n_threads;
  for (int i = 0; i < n_threads; ++i) {
    Task* task = (Task*) malloc(sizeof(Task));
    task->a = a + d * i;
    task->b = a + d * (i + 1);
    task->delta = delta;
    task->f = f;
    int create_status = pthread_create(&threads_handles[i], NULL, 
      thread_integrate, (void*)task);
    if (create_status) {
      free(threads_handles);
      return 3;
    }
  }

  double t_res = 0.0;
  for (int i = 0; i < n_threads; ++i) {
    double *ans;
    int join_status = pthread_join(threads_handles[i], (void**)&ans);
    if (join_status || ans == NULL) {
      free(threads_handles);
      return 4;
    }
    t_res += *ans;
    free(ans);
  }
  *res = t_res;

  free(threads_handles);

  return 0;
}
