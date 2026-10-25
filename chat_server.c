
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <pthread.h>
#include <signal.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdatomic.h>
#include <time.h>

#define BUFFER_SIZE 1024
#define MAX_CLIENTS 100
#define BENCH_BUFFER_SIZE 65536

//dados do benchmarck, vai do primeiro ao ultimo cliente
pthread_mutex_t bench_mutex = PTHREAD_MUTEX_INITIALIZER;
int bench_active = 0;
int bench_peak = 0;
int bench_has_data = 0;
unsigned long long bench_bytes = 0;
struct timespec bench_start, bench_end;

// Contador global, por segundo
atomic_ullong live_bytes = 0;

typedef struct Client {
    int fd;
    char name[32];
    struct Client *next;
} Client;

Client *clients = NULL;
int num_clients = 0;

pthread_mutex_t clients_mutex = PTHREAD_MUTEX_INITIALIZER;

//envia todo os bytes
int send_all(int fd, const char *data, size_t len) {
    size_t sent = 0;

    while (sent < len) {
        ssize_t n = send(fd, data + sent, len - sent,
                         MSG_NOSIGNAL);

        if (n == -1) {
            if (errno == EINTR)
                continue;
            return -1;
        }

        if (n == 0)
            return -1;

        sent += (size_t)n;
    }

    return 0;
}

ssize_t read_line(int fd, char *buffer, size_t capacity) {
    size_t pos = 0;

    while (pos < capacity - 1) {
        char c;
        ssize_t n = recv(fd, &c, 1, 0);

        if (n == 0)
            return pos == 0 ? 0 : (ssize_t)pos;

        if (n == -1) {
            if (errno == EINTR)
                continue;
            return -1;
        }

        if (c == '\n')
            break;

        if (c != '\r')
            buffer[pos++] = c;
    }

    buffer[pos] = '\0';
    return (ssize_t)pos;
}

void broadcast(const char *message, Client *sender) {
    pthread_mutex_lock(&clients_mutex);

    Client *current = clients;

    while (current != NULL) {
        if (current != sender) {
            if (send_all(current->fd, message,
                         strlen(message)) == -1) {
                fprintf(stderr,
                        "Falha ao enviar para %s.\n",
                        current->name);
            }
        }

        current = current->next;
    }

    pthread_mutex_unlock(&clients_mutex);
}

double elapsed(struct timespec a, struct timespec b) {
    return (double)(b.tv_sec - a.tv_sec) +
           (double)(b.tv_nsec - a.tv_nsec) / 1e9;
}

// benchmark: só recebe blocos e conta bytes
void handle_bench(Client *client) {
    char buffer[BENCH_BUFFER_SIZE];
    unsigned long long total = 0;
    struct timespec first = {0, 0}, last = {0, 0};
    ssize_t n;

    pthread_mutex_lock(&bench_mutex);
    bench_active++;
    if (bench_active > bench_peak)
        bench_peak = bench_active;
    pthread_mutex_unlock(&bench_mutex);

    while (1) {
        n = recv(client->fd, buffer, sizeof(buffer), 0);

        if (n == -1 && errno == EINTR)
            continue;

        if (n <= 0)
            break;

        clock_gettime(CLOCK_MONOTONIC, &last);
        if (total == 0)
            first = last;

        total += (unsigned long long)n;
        atomic_fetch_add(&live_bytes, (unsigned long long)n);
    }

    if (n == -1)
        perror("recv");

    pthread_mutex_lock(&bench_mutex);

    bench_bytes += total;

    if (total > 0) {
        if (!bench_has_data || elapsed(first, bench_start) > 0)
            bench_start = first;
        if (!bench_has_data || elapsed(bench_end, last) > 0)
            bench_end = last;
        bench_has_data = 1;
    }

    bench_active--;

    // ultimo cliente saiu: imprime o resultado 
    if (bench_active == 0) {
        double secs = bench_has_data ?
                      elapsed(bench_start, bench_end) : 0.0;
        double mbps = secs > 0 ?
                      (double)bench_bytes * 8.0 / secs / 1e6 : 0.0;

        printf("RESULT clientes=%d bytes=%llu tempo=%.3fs "
               "taxa=%.2f Mbps\n",
               bench_peak, bench_bytes, secs, mbps);

        bench_peak = 0;
        bench_bytes = 0;
        bench_has_data = 0;
    }

    pthread_mutex_unlock(&bench_mutex);
}

/* Mostra a taxa agregada instantanea a cada segundo. */
void *reporter(void *arg) {
    unsigned long long prev = 0;

    (void)arg;

    while (1) {
        sleep(1);

        unsigned long long now = atomic_load(&live_bytes);
        unsigned long long delta = now - prev;
        prev = now;

        if (delta > 0) {
            pthread_mutex_lock(&bench_mutex);
            int active = bench_active;
            pthread_mutex_unlock(&bench_mutex);

            printf("  [1s] %.2f Mbps (%d clientes ativos)\n",
                   (double)delta * 8.0 / 1e6, active);
        }
    }

    return NULL;
}

void *handle_client(void *arg) {
    Client *client = arg;
    char buffer[BUFFER_SIZE];
    char message[BUFFER_SIZE + 64];
    int registered = 0;

    /* A primeira linha recebida e o nome do usuario. */
    ssize_t n = read_line(client->fd, buffer, sizeof(buffer));

    if (n <= 0 || buffer[0] == '\0') {
        close(client->fd);
        free(client);
        return NULL;
    }

    /* "/bench" no lugar do nome ativa o modo benchmark. */
    if (strcmp(buffer, "/bench") == 0) {
        handle_bench(client);
        close(client->fd);
        free(client);
        return NULL;
    }

    snprintf(client->name, sizeof(client->name),
             "%s", buffer);

    pthread_mutex_lock(&clients_mutex);

    if (num_clients >= MAX_CLIENTS) {
        pthread_mutex_unlock(&clients_mutex);

        const char *error = "Servidor cheio.\n";
        send_all(client->fd, error, strlen(error));
        close(client->fd);
        free(client);
        return NULL;
    }

    client->next = clients;
    clients = client;
    num_clients++;
    registered = 1;

    pthread_mutex_unlock(&clients_mutex);

    snprintf(message, sizeof(message),
             "*** %s entrou no chat. ***\n",
             client->name);
    broadcast(message, client);

    printf("Usuario conectado: %s\n", client->name);

    while ((n = read_line(client->fd, buffer,
                          sizeof(buffer))) > 0) {
        if (strcmp(buffer, "/quit") == 0)
            break;

        snprintf(message, sizeof(message),
                 "%s: %s\n", client->name, buffer);

        broadcast(message, client);
    }

    if (n == -1)
        perror("recv");

    if (registered) {
        pthread_mutex_lock(&clients_mutex);

        Client **current = &clients;

        while (*current != NULL && *current != client)
            current = &(*current)->next;

        if (*current == client) {
            *current = client->next;
            num_clients--;
        }

        pthread_mutex_unlock(&clients_mutex);

        snprintf(message, sizeof(message),
                 "*** %s saiu do chat. ***\n",
                 client->name);
        broadcast(message, NULL);
    }

    printf("Usuario desconectado: %s\n", client->name);

    close(client->fd);
    free(client);

    return NULL;
}

int main(int argc, char *argv[]) {
    int server_fd;
    struct sockaddr_in server_addr;

    if (argc != 2) {
        fprintf(stderr, "Uso: %s <porta>\n", argv[0]);
        return EXIT_FAILURE;
    }

    int port = atoi(argv[1]);

    signal(SIGPIPE, SIG_IGN);
    setvbuf(stdout, NULL, _IOLBF, 0);

    pthread_t reporter_tid;
    if (pthread_create(&reporter_tid, NULL, reporter, NULL) == 0)
        pthread_detach(reporter_tid);

    server_fd = socket(AF_INET, SOCK_STREAM, 0);

    if (server_fd == -1) {
        perror("socket");
        return EXIT_FAILURE;
    }

    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR,
               &opt, sizeof(opt));

    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = htonl(INADDR_ANY);
    server_addr.sin_port = htons(port);

    if (bind(server_fd, (struct sockaddr *)&server_addr,
             sizeof(server_addr)) == -1) {
        perror("bind");
        close(server_fd);
        return EXIT_FAILURE;
    }

    if (listen(server_fd, 10) == -1) {
        perror("listen");
        close(server_fd);
        return EXIT_FAILURE;
    }

    printf("Chat TCP ativo na porta %d.\n", port);

    while (1) {
        struct sockaddr_in client_addr;
        socklen_t addr_len = sizeof(client_addr);

        int client_fd = accept(
            server_fd,
            (struct sockaddr *)&client_addr,
            &addr_len
        );

        if (client_fd == -1) {
            if (errno == EINTR)
                continue;

            perror("accept");
            continue;
        }

        Client *client = malloc(sizeof(Client));

        if (client == NULL) {
            perror("malloc");
            close(client_fd);
            continue;
        }

        client->fd = client_fd;
        client->name[0] = '\0';
        client->next = NULL;

        pthread_t tid;

        if (pthread_create(&tid, NULL,
                           handle_client, client) != 0) {
            fprintf(stderr, "Erro ao criar thread.\n");
            close(client_fd);
            free(client);
            continue;
        }

        pthread_detach(tid);
    }

    close(server_fd);
    pthread_mutex_destroy(&clients_mutex);

    return EXIT_SUCCESS;
}
