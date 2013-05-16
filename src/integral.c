
#include <pthread.h>
#include <stdlib.h>
#include <stdbool.h>
#include <stdio.h>

#include "integral.h"

struct Task {
  double a;
  double b;
  double delta;
  double (*f)(double);
};
typedef struct Task Task;

static double* thread_integrate(Task *task)
{
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

  Task* tasks[n_threads];
  bool is_ok = true;

  double d = (b - a) / n_threads;
  int i;
  for (i = 0; i < n_threads; ++i) {
    Task *task = (Task*) malloc(sizeof(Task));
    task->a = a + d * i;
    task->b = a + d * (i + 1);
    task->delta = delta;
    task->f = f;
    tasks[i] = task;

    int create_status = pthread_create(&threads_handles[i], NULL, 
      (void * (*)(void *))thread_integrate, (void*)task);
    if (create_status) {
      is_ok = false;
      break;
    }
  }

  if (!is_ok) {
    for (int j = 0; j < i; ++j)
      free(tasks[i]);
    free(threads_handles);
    return 3;
  }

  is_ok = true;
  double t_res = 0.0;
  for (int i = 0; i < n_threads; ++i) {
    double *ans;
    int join_status = pthread_join(threads_handles[i], (void**)&ans);
    if (join_status || ans == NULL) {
      is_ok = false;
      break;
    }
    t_res += *ans;
    free(ans);
  }
  *res = t_res;

  if (!is_ok)
  {
    free(threads_handles);
    return 4;
  }

  free(threads_handles);
  return 0;
}
