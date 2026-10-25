
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <pthread.h>
#include <signal.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <time.h>

#define BUFFER_SIZE 1024
#define DEFAULT_BLOCK_SIZE 65536

int sockfd;
pthread_mutex_t output_mutex = PTHREAD_MUTEX_INITIALIZER;

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

void *receive_messages(void *arg) {
    char buffer[BUFFER_SIZE];
    size_t pos = 0;

    (void)arg;

    while (1) {
        char c;
        ssize_t n = recv(sockfd, &c, 1, 0);

        if (n == 0) {
            pthread_mutex_lock(&output_mutex);
            printf("\nConexao encerrada pelo servidor.\n");
            fflush(stdout);
            pthread_mutex_unlock(&output_mutex);
            break;
        }

        if (n == -1) {
            if (errno == EINTR)
                continue;

            pthread_mutex_lock(&output_mutex);
            perror("recv");
            pthread_mutex_unlock(&output_mutex);
            break;
        }

        if (c == '\n') {
            buffer[pos] = '\0';

            pthread_mutex_lock(&output_mutex);
            printf("\r%s\n", buffer);
            fflush(stdout);
            pthread_mutex_unlock(&output_mutex);

            pos = 0;
        } else if (c != '\r') {
            if (pos < sizeof(buffer) - 1)
                buffer[pos++] = c;
        }
    }

    return NULL;
}

double elapsed(struct timespec a, struct timespec b) {
    return (double)(b.tv_sec - a.tv_sec) +
           (double)(b.tv_nsec - a.tv_nsec) / 1e9;
}

/* Modo benchmark: envia blocos continuamente durante 'seconds'. */
int run_bench(double seconds, size_t block_size) {
    char *block = malloc(block_size);
    unsigned long long total = 0;
    struct timespec start, now;

    if (block == NULL) {
        perror("malloc");
        return EXIT_FAILURE;
    }

    memset(block, 'x', block_size);

    if (send_all(sockfd, "/bench\n", 7) == -1) {
        perror("send");
        free(block);
        return EXIT_FAILURE;
    }

    clock_gettime(CLOCK_MONOTONIC, &start);
    now = start;

    while (elapsed(start, now) < seconds) {
        if (send_all(sockfd, block, block_size) == -1) {
            perror("send");
            break;
        }

        total += block_size;
        clock_gettime(CLOCK_MONOTONIC, &now);
    }

    double secs = elapsed(start, now);

    printf("Cliente: %llu bytes em %.3fs (%.2f Mbps)\n",
           total, secs,
           secs > 0 ? (double)total * 8.0 / secs / 1e6 : 0.0);

    shutdown(sockfd, SHUT_WR);
    close(sockfd);
    free(block);

    return EXIT_SUCCESS;
}

int main(int argc, char *argv[]) {
    struct sockaddr_in server_addr;
    char name[32];
    char buffer[BUFFER_SIZE];
    pthread_t receiver;
    int bench = argc == 4 || argc == 5;

    if (argc != 3 && !bench) {
        fprintf(stderr,
                "Uso: %s <IP> <porta>                     (chat)\n"
                "     %s <IP> <porta> <segundos> [bloco]  (benchmark)\n",
                argv[0], argv[0]);
        return EXIT_FAILURE;
    }

    int port = atoi(argv[2]);

    signal(SIGPIPE, SIG_IGN);

    sockfd = socket(AF_INET, SOCK_STREAM, 0);

    if (sockfd == -1) {
        perror("socket");
        return EXIT_FAILURE;
    }

    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(port);

    if (inet_pton(AF_INET, argv[1],
                  &server_addr.sin_addr) != 1) {
        fprintf(stderr, "Endereco IPv4 invalido.\n");
        close(sockfd);
        return EXIT_FAILURE;
    }

    if (connect(sockfd, (struct sockaddr *)&server_addr,
                sizeof(server_addr)) == -1) {
        perror("connect");
        close(sockfd);
        return EXIT_FAILURE;
    }

    if (bench) {
        double seconds = atof(argv[3]);
        long block_size = argc == 5 ? atol(argv[4])
                                    : DEFAULT_BLOCK_SIZE;

        if (seconds <= 0 || block_size <= 0) {
            fprintf(stderr, "Tempo e bloco devem ser positivos.\n");
            close(sockfd);
            return EXIT_FAILURE;
        }

        return run_bench(seconds, (size_t)block_size);
    }

    printf("Seu nome: ");

    if (fgets(name, sizeof(name), stdin) == NULL) {
        close(sockfd);
        return EXIT_FAILURE;
    }

    name[strcspn(name, "\r\n")] = '\0';

    if (name[0] == '\0' ||
        strchr(name, '\n') != NULL) {
        fprintf(stderr, "Nome invalido.\n");
        close(sockfd);
        return EXIT_FAILURE;
    }

    if (send_all(sockfd, name, strlen(name)) == -1 ||
        send_all(sockfd, "\n", 1) == -1) {
        perror("send");
        close(sockfd);
        return EXIT_FAILURE;
    }

    if (pthread_create(&receiver, NULL,
                       receive_messages, NULL) != 0) {
        fprintf(stderr, "Erro ao criar thread receptora.\n");
        close(sockfd);
        return EXIT_FAILURE;
    }

    printf("Conectado! Digite /quit para sair.\n");

    while (1) {
        pthread_mutex_lock(&output_mutex);
        printf("> ");
        fflush(stdout);
        pthread_mutex_unlock(&output_mutex);

        if (fgets(buffer, sizeof(buffer), stdin) == NULL)
            break;

        if (strcmp(buffer, "/quit\n") == 0 ||
            strcmp(buffer, "/quit") == 0) {
            send_all(sockfd, "/quit\n", 6);
            break;
        }

        size_t len = strlen(buffer);

        if (len > 0 && buffer[len - 1] != '\n') {
            int c;
            while ((c = getchar()) != '\n' && c != EOF)
                ;
            fprintf(stderr, "Mensagem muito longa.\n");
            continue;
        }

        if (send_all(sockfd, buffer, len) == -1) {
            perror("send");
            break;
        }
    }

    shutdown(sockfd, SHUT_RDWR);
    pthread_join(receiver, NULL);
    close(sockfd);

    pthread_mutex_destroy(&output_mutex);

    printf("Cliente encerrado.\n");

    return EXIT_SUCCESS;
}
