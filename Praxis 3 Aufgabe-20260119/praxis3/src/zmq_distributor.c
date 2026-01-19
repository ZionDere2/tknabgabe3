#include <ctype.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <zmq.h>

#define MAX_MSG_SIZE 1500
#define TYPE_LEN 3
#define MAX_PAYLOAD (MAX_MSG_SIZE - TYPE_LEN - 1)

typedef struct {
    char *word;
    int count;
} WordCount;

typedef struct {
    void *context;
    char *endpoint;
    char **chunks;
    size_t chunk_count;
    char **responses;
    const char *message_type;
} WorkerTask;

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
        size_t new_capacity = (*capacity == 0) ? 16 : (*capacity * 2);
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

static char **split_text(const char *text, size_t max_payload, size_t *out_count) {
    size_t text_len = strlen(text);
    size_t capacity = 16;
    size_t count = 0;
    char **chunks = calloc(capacity, sizeof(char *));
    if (!chunks) {
        perror("calloc");
        exit(EXIT_FAILURE);
    }

    size_t pos = 0;
    while (pos < text_len) {
        size_t remaining = text_len - pos;
        size_t chunk_len = remaining < max_payload ? remaining : max_payload;

        size_t cut = chunk_len;
        if (pos + chunk_len < text_len) {
            size_t i = chunk_len;
            while (i > 0) {
                char c = text[pos + i - 1];
                if (!is_letter(c)) {
                    cut = i;
                    break;
                }
                --i;
            }
        }
        if (cut == 0) {
            cut = chunk_len;
        }

        char *chunk = malloc(cut + 1);
        if (!chunk) {
            perror("malloc");
            exit(EXIT_FAILURE);
        }
        memcpy(chunk, text + pos, cut);
        chunk[cut] = '\0';

        if (count == capacity) {
            capacity *= 2;
            char **new_chunks = realloc(chunks, capacity * sizeof(char *));
            if (!new_chunks) {
                perror("realloc");
                exit(EXIT_FAILURE);
            }
            chunks = new_chunks;
        }
        chunks[count++] = chunk;
        pos += cut;
    }

    *out_count = count;
    return chunks;
}

static char **split_kv_stream(const char *text, size_t max_payload, size_t *out_count) {
    size_t text_len = strlen(text);
    size_t capacity = 16;
    size_t count = 0;
    char **chunks = calloc(capacity, sizeof(char *));
    if (!chunks) {
        perror("calloc");
        exit(EXIT_FAILURE);
    }

    size_t pos = 0;
    while (pos < text_len) {
        size_t remaining = text_len - pos;
        size_t chunk_len = remaining < max_payload ? remaining : max_payload;
        size_t cut = chunk_len;

        if (pos + chunk_len < text_len) {
            size_t i = chunk_len;
            while (i > 0) {
                char c = text[pos + i - 1];
                if (isdigit((unsigned char)c)) {
                    if (pos + i == text_len || is_letter(text[pos + i])) {
                        cut = i;
                        break;
                    }
                }
                --i;
            }
        }

        if (cut == 0) {
            cut = chunk_len;
        }

        char *chunk = malloc(cut + 1);
        if (!chunk) {
            perror("malloc");
            exit(EXIT_FAILURE);
        }
        memcpy(chunk, text + pos, cut);
        chunk[cut] = '\0';

        if (count == capacity) {
            capacity *= 2;
            char **new_chunks = realloc(chunks, capacity * sizeof(char *));
            if (!new_chunks) {
                perror("realloc");
                exit(EXIT_FAILURE);
            }
            chunks = new_chunks;
        }
        chunks[count++] = chunk;
        pos += cut;
    }

    *out_count = count;
    return chunks;
}

static void free_chunks(char **chunks, size_t count) {
    for (size_t i = 0; i < count; ++i) {
        free(chunks[i]);
    }
    free(chunks);
}

static void *worker_task_run(void *arg) {
    WorkerTask *task = arg;
    void *socket = zmq_socket(task->context, ZMQ_REQ);
    if (!socket) {
        perror("zmq_socket");
        return NULL;
    }
    int linger = 0;
    zmq_setsockopt(socket, ZMQ_LINGER, &linger, sizeof(linger));

    if (zmq_connect(socket, task->endpoint) != 0) {
        perror("zmq_connect");
        zmq_close(socket);
        return NULL;
    }

    for (size_t i = 0; i < task->chunk_count; ++i) {
        const char *chunk = task->chunks[i];
        size_t payload_len = strlen(chunk);
        size_t msg_len = TYPE_LEN + payload_len + 1;
        char *message = malloc(msg_len);
        if (!message) {
            perror("malloc");
            exit(EXIT_FAILURE);
        }
        memcpy(message, task->message_type, TYPE_LEN);
        memcpy(message + TYPE_LEN, chunk, payload_len);
        message[msg_len - 1] = '\0';

        zmq_send(socket, message, msg_len, 0);
        free(message);

        char buffer[MAX_MSG_SIZE];
        int recv_size = zmq_recv(socket, buffer, sizeof(buffer) - 1, 0);
        if (recv_size < 0) {
            task->responses[i] = strdup("");
            continue;
        }
        buffer[recv_size] = '\0';
        task->responses[i] = strdup(buffer);
    }

    zmq_close(socket);
    return NULL;
}

static char **run_phase(void *context, char **ports, size_t port_count,
                        char **chunks, size_t chunk_count,
                        const char *message_type, size_t *out_response_count) {
    WorkerTask *tasks = calloc(port_count, sizeof(WorkerTask));
    pthread_t *threads = calloc(port_count, sizeof(pthread_t));
    size_t *counts = calloc(port_count, sizeof(size_t));

    if (!tasks || !threads || !counts) {
        perror("calloc");
        exit(EXIT_FAILURE);
    }

    for (size_t i = 0; i < chunk_count; ++i) {
        counts[i % port_count]++;
    }

    char ***assigned_chunks = calloc(port_count, sizeof(char **));
    char ***assigned_responses = calloc(port_count, sizeof(char **));
    if (!assigned_chunks || !assigned_responses) {
        perror("calloc");
        exit(EXIT_FAILURE);
    }

    size_t *offsets = calloc(port_count, sizeof(size_t));
    if (!offsets) {
        perror("calloc");
        exit(EXIT_FAILURE);
    }

    for (size_t i = 0; i < port_count; ++i) {
        assigned_chunks[i] = calloc(counts[i], sizeof(char *));
        assigned_responses[i] = calloc(counts[i], sizeof(char *));
    }

    for (size_t i = 0; i < chunk_count; ++i) {
        size_t worker_index = i % port_count;
        assigned_chunks[worker_index][offsets[worker_index]++] = chunks[i];
    }

    for (size_t i = 0; i < port_count; ++i) {
        char endpoint[64];
        snprintf(endpoint, sizeof(endpoint), "tcp://127.0.0.1:%s", ports[i]);
        tasks[i].context = context;
        tasks[i].endpoint = strdup(endpoint);
        tasks[i].chunks = assigned_chunks[i];
        tasks[i].chunk_count = counts[i];
        tasks[i].responses = assigned_responses[i];
        tasks[i].message_type = message_type;
        pthread_create(&threads[i], NULL, worker_task_run, &tasks[i]);
    }

    for (size_t i = 0; i < port_count; ++i) {
        pthread_join(threads[i], NULL);
        free(tasks[i].endpoint);
    }

    char **responses = calloc(chunk_count, sizeof(char *));
    if (!responses) {
        perror("calloc");
        exit(EXIT_FAILURE);
    }

    size_t index = 0;
    for (size_t i = 0; i < port_count; ++i) {
        for (size_t j = 0; j < counts[i]; ++j) {
            responses[index++] = assigned_responses[i][j];
        }
        free(assigned_chunks[i]);
        free(assigned_responses[i]);
    }

    free(assigned_chunks);
    free(assigned_responses);
    free(offsets);
    free(tasks);
    free(threads);
    free(counts);

    *out_response_count = chunk_count;
    return responses;
}

static char *join_strings(char **strings, size_t count) {
    size_t total_len = 0;
    for (size_t i = 0; i < count; ++i) {
        total_len += strlen(strings[i]);
    }

    char *result = calloc(total_len + 1, 1);
    if (!result) {
        perror("calloc");
        exit(EXIT_FAILURE);
    }

    char *cursor = result;
    for (size_t i = 0; i < count; ++i) {
        size_t len = strlen(strings[i]);
        memcpy(cursor, strings[i], len);
        cursor += len;
    }
    *cursor = '\0';

    return result;
}

static void parse_reduced_output(const char *text, WordCount **words, size_t *count, size_t *capacity) {
    const char *ptr = text;
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
        add_word(words, count, capacity, word, value);
        free(word);
    }
}

typedef struct {
    char *word;
    int count;
} WordSummary;

static int compare_word_summary(const void *a, const void *b) {
    const WordSummary *wa = a;
    const WordSummary *wb = b;
    if (wa->count != wb->count) {
        return (wb->count - wa->count);
    }
    return strcmp(wa->word, wb->word);
}

static void send_rip(void *context, char **ports, size_t port_count) {
    for (size_t i = 0; i < port_count; ++i) {
        void *socket = zmq_socket(context, ZMQ_REQ);
        if (!socket) {
            continue;
        }
        char endpoint[64];
        snprintf(endpoint, sizeof(endpoint), "tcp://127.0.0.1:%s", ports[i]);
        if (zmq_connect(socket, endpoint) != 0) {
            zmq_close(socket);
            continue;
        }
        zmq_send(socket, "rip\0", 4, 0);
        char buffer[MAX_MSG_SIZE];
        zmq_recv(socket, buffer, sizeof(buffer) - 1, 0);
        zmq_close(socket);
    }
}

int main(int argc, char **argv) {
    if (argc < 3) {
        fprintf(stderr, "Usage: %s <file.txt> <worker port 1> [<worker port 2> ...]\n", argv[0]);
        return EXIT_FAILURE;
    }

    const char *filename = argv[1];
    size_t port_count = (size_t)(argc - 2);
    char **ports = &argv[2];

    FILE *file = fopen(filename, "r");
    if (!file) {
        perror("fopen");
        return EXIT_FAILURE;
    }
    fseek(file, 0, SEEK_END);
    long file_size = ftell(file);
    if (file_size < 0) {
        perror("ftell");
        fclose(file);
        return EXIT_FAILURE;
    }
    fseek(file, 0, SEEK_SET);

    char *content = calloc((size_t)file_size + 1, 1);
    if (!content) {
        perror("calloc");
        fclose(file);
        return EXIT_FAILURE;
    }

    size_t read_size = fread(content, 1, (size_t)file_size, file);
    content[read_size] = '\0';
    fclose(file);

    void *context = zmq_ctx_new();
    if (!context) {
        perror("zmq_ctx_new");
        free(content);
        return EXIT_FAILURE;
    }

    size_t map_chunk_count = 0;
    char **map_chunks = split_text(content, MAX_PAYLOAD, &map_chunk_count);

    size_t map_response_count = 0;
    char **map_responses = run_phase(context, ports, port_count,
                                     map_chunks, map_chunk_count, "map", &map_response_count);

    char *map_stream = join_strings(map_responses, map_response_count);

    for (size_t i = 0; i < map_response_count; ++i) {
        free(map_responses[i]);
    }
    free(map_responses);
    free_chunks(map_chunks, map_chunk_count);

    size_t reduce_chunk_count = 0;
    char **reduce_chunks = split_kv_stream(map_stream, MAX_PAYLOAD, &reduce_chunk_count);

    size_t reduce_response_count = 0;
    char **reduce_responses = run_phase(context, ports, port_count,
                                        reduce_chunks, reduce_chunk_count, "red", &reduce_response_count);

    WordCount *final_words = NULL;
    size_t final_count = 0;
    size_t final_capacity = 0;

    for (size_t i = 0; i < reduce_response_count; ++i) {
        parse_reduced_output(reduce_responses[i], &final_words, &final_count, &final_capacity);
        free(reduce_responses[i]);
    }

    free(reduce_responses);
    free_chunks(reduce_chunks, reduce_chunk_count);
    free(map_stream);
    free(content);

    WordSummary *summary = calloc(final_count, sizeof(WordSummary));
    if (!summary) {
        perror("calloc");
        exit(EXIT_FAILURE);
    }

    for (size_t i = 0; i < final_count; ++i) {
        summary[i].word = final_words[i].word;
        summary[i].count = final_words[i].count;
    }

    qsort(summary, final_count, sizeof(WordSummary), compare_word_summary);

    printf("word,frequency\n");
    for (size_t i = 0; i < final_count; ++i) {
        printf("%s,%d\n", summary[i].word, summary[i].count);
    }

    for (size_t i = 0; i < final_count; ++i) {
        free(final_words[i].word);
    }
    free(final_words);
    free(summary);

    send_rip(context, ports, port_count);
    zmq_ctx_destroy(context);

    return EXIT_SUCCESS;
}
