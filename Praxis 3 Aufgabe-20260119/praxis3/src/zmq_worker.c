#include <ctype.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <zmq.h>

#define MAX_MSG_SIZE 1500

typedef struct {
    char *word;
    int count;
} WordCount;

typedef struct {
    void *context;
    char *endpoint;
} WorkerThreadArgs;

static int is_letter(char c) {
    return isalpha((unsigned char)c);
}

static void to_lowercase(char *s) {
    for (; *s; ++s) {
        *s = (char)tolower((unsigned char)*s);
    }
}

static void add_word(WordCount **words, size_t *count, size_t *capacity, const char *word, int add) {
    for (size_t i = 0; i < *count; ++i) {
        if (strcmp((*words)[i].word, word) == 0) {
            (*words)[i].count += add;
            return;
        }
    }

    if (*count == *capacity) {
        size_t new_capacity = (*capacity == 0) ? 8 : (*capacity * 2);
        WordCount *new_words = realloc(*words, new_capacity * sizeof(WordCount));
        if (!new_words) {
            perror("realloc");
            exit(EXIT_FAILURE);
        }
        *words = new_words;
        *capacity = new_capacity;
    }

    (*words)[*count].word = strdup(word);
    (*words)[*count].count = add;
    (*count)++;
}

static char *map_payload(const char *payload) {
    WordCount *words = NULL;
    size_t word_count = 0;
    size_t word_capacity = 0;

    const char *ptr = payload;
    while (*ptr) {
        while (*ptr && !is_letter(*ptr)) {
            ++ptr;
        }
        if (!*ptr) {
            break;
        }
        const char *start = ptr;
        while (*ptr && is_letter(*ptr)) {
            ++ptr;
        }
        size_t len = (size_t)(ptr - start);
        char *word = malloc(len + 1);
        if (!word) {
            perror("malloc");
            exit(EXIT_FAILURE);
        }
        memcpy(word, start, len);
        word[len] = '\0';
        to_lowercase(word);
        add_word(&words, &word_count, &word_capacity, word, 1);
        free(word);
    }

    size_t out_len = 0;
    for (size_t i = 0; i < word_count; ++i) {
        out_len += strlen(words[i].word) + (size_t)words[i].count;
    }

    char *output = calloc(out_len + 1, 1);
    if (!output) {
        perror("calloc");
        exit(EXIT_FAILURE);
    }

    char *cursor = output;
    for (size_t i = 0; i < word_count; ++i) {
        size_t wlen = strlen(words[i].word);
        memcpy(cursor, words[i].word, wlen);
        cursor += wlen;
        for (int j = 0; j < words[i].count; ++j) {
            *cursor++ = '1';
        }
    }
    *cursor = '\0';

    for (size_t i = 0; i < word_count; ++i) {
        free(words[i].word);
    }
    free(words);

    return output;
}

static char *reduce_payload(const char *payload) {
    WordCount *words = NULL;
    size_t word_count = 0;
    size_t word_capacity = 0;

    const char *ptr = payload;
    while (*ptr) {
        while (*ptr && !is_letter(*ptr)) {
            ++ptr;
        }
        if (!*ptr) {
            break;
        }
        const char *start = ptr;
        while (*ptr && is_letter(*ptr)) {
            ++ptr;
        }
        size_t len = (size_t)(ptr - start);
        char *word = malloc(len + 1);
        if (!word) {
            perror("malloc");
            exit(EXIT_FAILURE);
        }
        memcpy(word, start, len);
        word[len] = '\0';
        to_lowercase(word);

        const char *num_start = ptr;
        while (*ptr && isdigit((unsigned char)*ptr)) {
            ++ptr;
        }
        int value = 0;
        if (ptr > num_start) {
            value = atoi(num_start);
        }
        if (value == 0) {
            value = (int)(ptr - num_start);
        }
        add_word(&words, &word_count, &word_capacity, word, value);
        free(word);
    }

    size_t out_len = 0;
    for (size_t i = 0; i < word_count; ++i) {
        char buf[32];
        int written = snprintf(buf, sizeof(buf), "%d", words[i].count);
        if (written < 0) {
            written = 0;
        }
        out_len += strlen(words[i].word) + (size_t)written;
    }

    char *output = calloc(out_len + 1, 1);
    if (!output) {
        perror("calloc");
        exit(EXIT_FAILURE);
    }

    char *cursor = output;
    for (size_t i = 0; i < word_count; ++i) {
        size_t wlen = strlen(words[i].word);
        memcpy(cursor, words[i].word, wlen);
        cursor += wlen;
        int written = sprintf(cursor, "%d", words[i].count);
        cursor += written;
    }
    *cursor = '\0';

    for (size_t i = 0; i < word_count; ++i) {
        free(words[i].word);
    }
    free(words);

    return output;
}

static void *worker_loop(void *arg) {
    WorkerThreadArgs *args = arg;
    void *socket = zmq_socket(args->context, ZMQ_REP);
    if (!socket) {
        perror("zmq_socket");
        return NULL;
    }

    if (zmq_bind(socket, args->endpoint) != 0) {
        perror("zmq_bind");
        zmq_close(socket);
        return NULL;
    }

    while (1) {
        char buffer[MAX_MSG_SIZE];
        int recv_size = zmq_recv(socket, buffer, sizeof(buffer) - 1, 0);
        if (recv_size < 0) {
            continue;
        }
        buffer[recv_size] = '\0';

        if (recv_size >= 3 && strncmp(buffer, "map", 3) == 0) {
            const char *payload = buffer + 3;
            char *reply_payload = map_payload(payload);
            size_t reply_len = strlen(reply_payload) + 1;
            zmq_send(socket, reply_payload, reply_len, 0);
            free(reply_payload);
        } else if (recv_size >= 3 && strncmp(buffer, "red", 3) == 0) {
            const char *payload = buffer + 3;
            char *reply_payload = reduce_payload(payload);
            size_t reply_len = strlen(reply_payload) + 1;
            zmq_send(socket, reply_payload, reply_len, 0);
            free(reply_payload);
        } else if (recv_size >= 3 && strncmp(buffer, "rip", 3) == 0) {
            zmq_send(socket, "rip\0", 4, 0);
            break;
        } else {
            zmq_send(socket, "\0", 1, 0);
        }
    }

    zmq_close(socket);
    return NULL;
}

int main(int argc, char **argv) {
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <port 1> [<port 2> ...]\n", argv[0]);
        return EXIT_FAILURE;
    }

    void *context = zmq_ctx_new();
    if (!context) {
        perror("zmq_ctx_new");
        return EXIT_FAILURE;
    }

    int port_count = argc - 1;
    pthread_t *threads = calloc((size_t)port_count, sizeof(pthread_t));
    WorkerThreadArgs *args = calloc((size_t)port_count, sizeof(WorkerThreadArgs));
    if (!threads || !args) {
        perror("calloc");
        return EXIT_FAILURE;
    }

    for (int i = 0; i < port_count; ++i) {
        char endpoint[64];
        snprintf(endpoint, sizeof(endpoint), "tcp://*:%s", argv[i + 1]);
        args[i].context = context;
        args[i].endpoint = strdup(endpoint);
        pthread_create(&threads[i], NULL, worker_loop, &args[i]);
    }

    for (int i = 0; i < port_count; ++i) {
        pthread_join(threads[i], NULL);
        free(args[i].endpoint);
    }

    free(threads);
    free(args);

    zmq_ctx_destroy(context);
    return EXIT_SUCCESS;
}
