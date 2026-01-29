/**
 * Etapa 3: Integração Relógios Vetoriais + Produtor/Consumidor
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
#define CLOCK_SIZE 3

/* --- Estruturas de Dados --- */

typedef struct Clock {
    int p[3];
} Clock;

// Estrutura para a fila de envio (precisa saber o destino)
typedef struct SendItem {
    Clock clock;
    int dest;
} SendItem;

/* --- Variáveis Globais (Filas e Sincronização) --- */

// Fila de Recepção (Produtor: Thread Receptora | Consumidor: Thread Central)
Clock recv_queue[BUFFER_SIZE];
int recv_count = 0;
int recv_in = 0;
int recv_out = 0;
pthread_mutex_t mutex_recv;
pthread_cond_t cond_recv_full;
pthread_cond_t cond_recv_empty;

// Fila de Envio (Produtor: Thread Central | Consumidor: Thread Emissora)
SendItem send_queue[BUFFER_SIZE];
int send_count = 0;
int send_in = 0;
int send_out = 0;
pthread_mutex_t mutex_send;
pthread_cond_t cond_send_full;
pthread_cond_t cond_send_empty;

// Flag para encerrar as threads auxiliares ao final
volatile int running = 1;

/* --- Funções Auxiliares --- */

void PrintClock(const char *acao, Clock *clock) {
    int pid;
    MPI_Comm_rank(MPI_COMM_WORLD, &pid);
    printf("[P%d] %s -> Clock = (%d, %d, %d)\n",
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

// Thread Emissora: Consome da send_queue -> Envia via MPI
void* sender_thread_func(void* args) {
    while (running) {
        SendItem item;

        pthread_mutex_lock(&mutex_send);
        while (send_count == 0 && running) {
            pthread_cond_wait(&cond_send_empty, &mutex_send);
        }

        if (!running) { // Saída limpa
            pthread_mutex_unlock(&mutex_send);
            break;
        }

        item = send_queue[send_out];
        send_out = (send_out + 1) % BUFFER_SIZE;
        send_count--;
        
        pthread_cond_signal(&cond_send_full);
        pthread_mutex_unlock(&mutex_send);

        // Envia efetivamente via MPI
        MPI_Send(item.clock.p, 3, MPI_INT, item.dest, 0, MPI_COMM_WORLD);
    }
    return NULL;
}

// Thread Receptora: Recebe via MPI -> Produz na recv_queue
void* receiver_thread_func(void* args) {
    int pid;
    MPI_Comm_rank(MPI_COMM_WORLD, &pid);

    while (running) {
        Clock received_clock;
        MPI_Status status;
        
        // Bloqueia esperando mensagem de QUALQUER origem (MPI_ANY_SOURCE)
        // Nota: Não colocamos mutex aqui para não travar o processo todo
        int mpi_ret = MPI_Recv(received_clock.p, 3, MPI_INT, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &status);
        
        if (mpi_ret != MPI_SUCCESS || !running) break;

        // Entra na Seção Crítica para colocar na fila
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
    PrintClock("Evento Interno", clock);
}

// Send: Coloca na fila de envio
void Send(int dest, Clock *clock) {
    int pid;
    MPI_Comm_rank(MPI_COMM_WORLD, &pid);

    // 1. Incrementar clock local
    clock->p[pid]++;

    // 2. Preparar item para envio
    SendItem item;
    item.clock = *clock; // Copia o estado atual
    item.dest = dest;

    // 3. Produzir na fila de envio
    pthread_mutex_lock(&mutex_send);
    while (send_count == BUFFER_SIZE) {
        pthread_cond_wait(&cond_send_full, &mutex_send);
    }

    send_queue[send_in] = item;
    send_in = (send_in + 1) % BUFFER_SIZE;
    send_count++;

    pthread_cond_signal(&cond_send_empty);
    pthread_mutex_unlock(&mutex_send);

    PrintClock("Postou na Fila de Envio", clock);
}

// Receive: Retira da fila de recepção (não especifica fonte, pega o que tiver)
void Receive(Clock *clock) {
    int pid;
    MPI_Comm_rank(MPI_COMM_WORLD, &pid);
    Clock remote_clock;

    // 1. Consumir da fila de recepção
    pthread_mutex_lock(&mutex_recv);
    while (recv_count == 0) {
        // Se a fila estiver vazia, a thread central dorme até a receptora colocar algo
        pthread_cond_wait(&cond_recv_empty, &mutex_recv);
    }

    remote_clock = recv_queue[recv_out];
    recv_out = (recv_out + 1) % BUFFER_SIZE;
    recv_count--;

    pthread_cond_signal(&cond_recv_full);
    pthread_mutex_unlock(&mutex_recv);

    // 2. Merge e Atualização
    MergeClocks(clock, &remote_clock); // Max(local, remoto)
    clock->p[pid]++; // Incremento do evento de receive

    PrintClock("Retirou da Fila de Recepção", clock);
}

/* --- Lógica dos Processos (Diagrama) --- */

void process0() {
    Clock clock = {{0,0,0}};
    PrintClock("Estado inicial", &clock);
   
    Event(&clock); 
    Event(&clock); 
    Send(1, &clock); 
    Receive(&clock); // Não especificamos ID, pega da fila
    Event(&clock); 
    Send(2, &clock);
    Event(&clock); 
    Receive(&clock); 
    Send(1, &clock);
    Event(&clock);
}

void process1() {
    Clock clock = {{0,0,0}};
    PrintClock("Estado inicial", &clock);
   
    Event(&clock); 
    Send(0, &clock); 
    Receive(&clock); 
    Receive(&clock); 
}

void process2() {
    Clock clock = {{0,0,0}};
    PrintClock("Estado inicial", &clock);
   
    Event(&clock);
    Event(&clock); 
    Send(0, &clock); 
    Receive(&clock); 
}

/* --- Main --- */

int main(int argc, char *argv[]) {
    int my_rank;
    int provided;

    // Inicializa MPI com suporte a Threads (MPI_THREAD_MULTIPLE é o ideal, 
    // mas MPI_THREAD_SERIALIZED serve pois main e sender não acessam MPI simultaneamente em conflito direto,
    // mas sender e receiver sim. Vamos pedir MULTIPLE por segurança).
    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
    if (provided < MPI_THREAD_MULTIPLE) {
        printf("Aviso: Suporte a threads MPI insuficiente.\n");
    }

    MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);

    // Inicialização de Mutexes e Conditions
    pthread_mutex_init(&mutex_recv, NULL);
    pthread_cond_init(&cond_recv_full, NULL);
    pthread_cond_init(&cond_recv_empty, NULL);

    pthread_mutex_init(&mutex_send, NULL);
    pthread_cond_init(&cond_send_full, NULL);
    pthread_cond_init(&cond_send_empty, NULL);

    // Criação das threads auxiliares
    pthread_t t_sender, t_receiver;
    pthread_create(&t_sender, NULL, sender_thread_func, NULL);
    pthread_create(&t_receiver, NULL, receiver_thread_func, NULL);

    // Executa a lógica da Thread Central
    if (my_rank == 0) process0();
    else if (my_rank == 1) process1();
    else if (my_rank == 2) process2();

    // Pequeno delay para garantir que mensagens pendentes na fila de saída sejam enviadas
    sleep(1); 
    
    // Encerrando (Abordagem simplificada para laboratório)
    // Em um sistema real, usaríamos mensagens de controle para parar as threads.
    running = 0;
    // Acordar threads que podem estar dormindo nas conditions
    pthread_cond_broadcast(&cond_send_empty);
    pthread_cond_broadcast(&cond_recv_full);
    
    // Força bruta para parar MPI_Recv bloqueante se necessário, 
    // mas MPI_Finalize fará a limpeza.
    
    MPI_Finalize();
    return 0;
}
