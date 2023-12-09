#include "header.h"

void thread_pool_init(ThreadPool *, size_t);

void *worker(void *arg){
    //worker code
    ThreadPool *tp = (ThreadPool *) arg;
    while(true){
        pthread_mutex_lock(&tp->m);
        //while tp's queue is empty wait on mutex
        while(tp->queue.empty()){
            pthread_cond_wait(&tp->not_empty, &tp->m);
        }

        //get the job from front of the queue
        Work w = tp->queue.front();
        tp->queue.pop_front();
        pthread_mutex_unlock(&tp->m);

        //do the work using its arguments
        w.f(w.arg);
    }
    
    return NULL;
}

void *producer(ThreadPool* T, void (*f)(void *), void *arg){
    Work w;
    w.f = f;
    w.arg = arg;

    pthread_mutex_lock(&T->m);
    T->queue.push_back(w);
    pthread_cond_signal(&T->not_empty);
    pthread_mutex_unlock(&T->m);   
}

void thread_pool_init(ThreadPool *tp, size_t n_threads){
    //cout << "thread_pool_init: in\n";

//initialise mutex and condition
    assert(n_threads > 0);
    //initialise pthread with attribute as null
    int rv = pthread_mutex_init(&tp->m, NULL);
    
    assert(rv == 0);
    rv = pthread_cond_init(&tp->not_empty, NULL);
    assert(rv == 0);
    //cout << "thread_pool_init: resize\n";
    tp->threads.resize(n_threads);
    //cout << "thread_pool_init: create\n";
    for(size_t i=0; i<n_threads ; ++i){
        int rv = pthread_create(&tp->threads[i], NULL, &worker, tp);
        assert(rv == 0);
    }
}