/**
 * Etapa 3: Integração Relógios Vetoriais + Produtor/Consumidor
 * Lógica ajustada para reproduzir exatamente a saída do rvet.c
 * ------------------------------------------------------------------------
 * Compilação: mpicc -o etapa3 etapa3.c -lpthread
 * Execução:   mpiexec -n 3 ./etapa3
 */

#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <mpi.h>
#include <unistd.h>

/* --- Configurações --- */
#define BUFFER_SIZE 5
#define TAG_DATA 0
#define TAG_EXIT 1

/* --- Estruturas de Dados --- */
typedef struct Clock {
    int p[3];
} Clock;

typedef struct SendItem {
    Clock clock;
    int dest;
} SendItem;

/* --- Variáveis Globais (Filas e Sincronização) --- */
Clock recv_queue[BUFFER_SIZE];
int recv_count = 0, recv_in = 0, recv_out = 0;
pthread_mutex_t mutex_recv;
pthread_cond_t cond_recv_full;
pthread_cond_t cond_recv_empty;

SendItem send_queue[BUFFER_SIZE];
int send_count = 0, send_in = 0, send_out = 0;
pthread_mutex_t mutex_send;
pthread_cond_t cond_send_full;
pthread_cond_t cond_send_empty;

volatile int running = 1;

/* --- Funções Auxiliares --- */

void PrintClock(const char *acao, Clock *clock) {
    int pid;
    MPI_Comm_rank(MPI_COMM_WORLD, &pid);
    printf("[Atualizado por P%d] %s -> Clock = (%d,%d,%d)\n",
           pid, acao, clock->p[0], clock->p[1], clock->p[2]);
}

void MergeClocks(Clock *local, Clock *remote) {
    for (int i = 0; i < 3; i++) {
        if (remote->p[i] > local->p[i]) {
            local->p[i] = remote->p[i];
        }
    }
}

/* --- Threads Auxiliares --- */

// Thread Emissora: Consome da Fila -> Envia via MPI
void* sender_thread_func(void* args) {
    while (running) {
        SendItem item;

        pthread_mutex_lock(&mutex_send);
        while (send_count == 0 && running) {
            pthread_cond_wait(&cond_send_empty, &mutex_send);
        }

        if (!running && send_count == 0) {
            pthread_mutex_unlock(&mutex_send);
            break;
        }

        item = send_queue[send_out];
        send_out = (send_out + 1) % BUFFER_SIZE;
        send_count--;
        
        pthread_cond_signal(&cond_send_full);
        pthread_mutex_unlock(&mutex_send);

        // Envia efetivamente via MPI
        MPI_Send(item.clock.p, 3, MPI_INT, item.dest, TAG_DATA, MPI_COMM_WORLD);
    }
    return NULL;
}

// Thread Receptora: Recebe via MPI -> Produz na Fila
void* receiver_thread_func(void* args) {
    while (running) {
        Clock received_clock;
        MPI_Status status;
        
        // Bloqueia no MPI esperando qualquer mensagem
        MPI_Recv(received_clock.p, 3, MPI_INT, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
        
        // Protocolo de Saída: Se receber a tag de exit, encerra
        if (status.MPI_TAG == TAG_EXIT) {
            running = 0;
            break; 
        }

        pthread_mutex_lock(&mutex_recv);
        while (recv_count == BUFFER_SIZE && running) {
            pthread_cond_wait(&cond_recv_full, &mutex_recv);
        }

        if (!running) {
            pthread_mutex_unlock(&mutex_recv);
            break;
        }

        recv_queue[recv_in] = received_clock;
        recv_in = (recv_in + 1) % BUFFER_SIZE;
        recv_count++;

        pthread_cond_signal(&cond_recv_empty);
        pthread_mutex_unlock(&mutex_recv);
    }
    return NULL;
}

/* --- Operações da Thread Central --- */

void Event(Clock *clock) {
    int pid;
    MPI_Comm_rank(MPI_COMM_WORLD, &pid);
    
    clock->p[pid]++;
    PrintClock("Evento interno", clock);
}

void Send(int dest, Clock *clock) {
    int pid;
    MPI_Comm_rank(MPI_COMM_WORLD, &pid);

    // 1. Incrementar clock local ANTES de colocar na fila (conforme seu código base)
    clock->p[pid]++;

    // 2. Preparar item
    SendItem item;
    item.clock = *clock;
    item.dest = dest;

    // 3. Colocar na fila de envio
    pthread_mutex_lock(&mutex_send);
    while (send_count == BUFFER_SIZE) {
        pthread_cond_wait(&cond_send_full, &mutex_send);
    }

    send_queue[send_in] = item;
    send_in = (send_in + 1) % BUFFER_SIZE;
    send_count++;

    pthread_cond_signal(&cond_send_empty);
    pthread_mutex_unlock(&mutex_send);

    PrintClock("Envio de mensagem", clock);
}

void Receive(Clock *clock) {
    int pid;
    MPI_Comm_rank(MPI_COMM_WORLD, &pid);
    Clock remote_clock;

    // 1. Retirar da fila de recepção
    pthread_mutex_lock(&mutex_recv);
    while (recv_count == 0) {
        pthread_cond_wait(&cond_recv_empty, &mutex_recv);
    }

    remote_clock = recv_queue[recv_out];
    recv_out = (recv_out + 1) % BUFFER_SIZE;
    recv_count--;

    pthread_cond_signal(&cond_recv_full);
    pthread_mutex_unlock(&mutex_recv);

    // 2. Merge e Atualização
    MergeClocks(clock, &remote_clock);
    clock->p[pid]++;

    PrintClock("Recebimento de mensagem", clock);
}

/* --- Lógica dos Processos (Copiada do seu gabarito) --- */

void process0() {
    Clock clock = {{0,0,0}};
    PrintClock("Estado inicial", &clock);
    
    Event(&clock);      // Evento
    Send(1, &clock);    // Envia para P1
    Receive(&clock);    // Espera msg (idealmente de P1)
    Send(2, &clock);    // Envia para P2
    Receive(&clock);    // Espera msg (idealmente de P2)
    Send(1, &clock);    // Envia para P1
    Event(&clock);      // Evento final
}

void process1() {
    Clock clock = {{0,0,0}};
    PrintClock("Estado inicial", &clock);
    
    Send(0, &clock);    // Envia para P0
    Receive(&clock);    // Espera P0
    Receive(&clock);    // Espera P0
}

void process2() {
    Clock clock = {{0,0,0}};
    PrintClock("Estado inicial", &clock);
    
    Event(&clock);      // Evento
    Send(0, &clock);    // Envia P0
    Receive(&clock);    // Espera P0
}

/* --- Main --- */

int main(int argc, char *argv[]) {
    int my_rank, provided;

    // Inicializa MPI com suporte a Threads
    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
    MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);

    // Inicialização das Filas
    pthread_mutex_init(&mutex_recv, NULL);
    pthread_cond_init(&cond_recv_full, NULL);
    pthread_cond_init(&cond_recv_empty, NULL);

    pthread_mutex_init(&mutex_send, NULL);
    pthread_cond_init(&cond_send_full, NULL);
    pthread_cond_init(&cond_send_empty, NULL);

    // Criação das threads
    pthread_t t_sender, t_receiver;
    pthread_create(&t_sender, NULL, sender_thread_func, NULL);
    pthread_create(&t_receiver, NULL, receiver_thread_func, NULL);

    // Executa a lógica
    if (my_rank == 0) process0();
    else if (my_rank == 1) process1();
    else if (my_rank == 2) process2();

    // Espera um momento para garantir que os prints saiam (não é estritamente necessário para lógica)
    sleep(1);

    // --- PROTOCOLO DE ENCERRAMENTO ---
    // 1. Setar flag de parada
    running = 0;

    // 2. Destravar thread emissora (se estiver vazia)
    pthread_mutex_lock(&mutex_send);
    pthread_cond_broadcast(&cond_send_empty);
    pthread_mutex_unlock(&mutex_send);

    // 3. Destravar thread receptora (Envia msg para SI MESMO para sair do MPI_Recv)
    int dummy[3];
    MPI_Send(dummy, 3, MPI_INT, my_rank, TAG_EXIT, MPI_COMM_WORLD);

    // 4. Aguardar threads morrerem
    pthread_join(t_sender, NULL);
    pthread_join(t_receiver, NULL);

    // 5. Limpeza
    pthread_mutex_destroy(&mutex_recv);
    pthread_mutex_destroy(&mutex_send);
    
    MPI_Finalize();
    return 0;
}
