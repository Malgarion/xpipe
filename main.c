#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <getopt.h>
#include <poll.h>
#include <pthread.h>
#include <signal.h>
#include <spawn.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <unistd.h>
#include <sched.h>
//==============================================================================================================
extern char **environ;
//==============================================================================================================
struct job {
	char *input;
	size_t length;
	char *output;
	size_t output_length;
	bool done;
	struct job *next_input;
	struct job *next_output;
};
//==============================================================================================================
struct state {
	pthread_mutex_t lock;
	pthread_cond_t work_ready, output_ready, slot_ready;
	struct job *input_head, *input_tail;
	struct job *output_head, *output_tail;
	size_t pending, limit;
	bool ordered, eof, failed;
	char **command;
};
//==============================================================================================================
static void die(const char *operation, int error) {
	fprintf(stderr, "xpipe: %s: %s\n", operation, strerror(error));
	exit(EXIT_FAILURE);
}
//==============================================================================================================
static void check(int error, const char *operation){ if(error) die(operation, error); }
//==============================================================================================================
static size_t positive_number(const char *text) {
	char *end;
	errno = 0;
	unsigned long long value = strtoull(text, &end, 10);
	if(*text < '0' || *text > '9' || *end || errno || !value || value > SIZE_MAX) return 0;
	return (size_t)value;
}
//==============================================================================================================
static size_t default_jobs(void){
	cpu_set_t cpus;
	if (sched_getaffinity(0, sizeof cpus, &cpus) == 0) {
		int count = CPU_COUNT(&cpus);
		if (count > 0) return (size_t)count;
	}
	long count = sysconf(_SC_NPROCESSORS_ONLN);
	return count > 0 ? (size_t)count : 1;
}
//==============================================================================================================
static int write_all(int fd, const char *data, size_t length) {
	while (length) {
		ssize_t written = write(fd, data, length);
		if (written < 0) {
			if (errno == EINTR) continue;
			return errno;
		}
		data += written;
		length -= (size_t)written;
	}
	return 0;
}
//==============================================================================================================
static void append_bytes(char **buffer, size_t *length, size_t *capacity, const char *data, size_t size) {
	if (size > SIZE_MAX - *length) die("job buffer too large", EOVERFLOW);

	size_t required = *length + size;
	if (required > *capacity) {
		size_t grown = *capacity <= SIZE_MAX / 2 ? *capacity * 2 : SIZE_MAX;
		if (grown < required) grown = required;

		char *replacement = realloc(*buffer, grown);
		if (!replacement) die("allocate job buffer", errno);
		*buffer = replacement;
		*capacity = grown;
	}

	memcpy(*buffer + *length, data, size);
	*length = required;
}
//==============================================================================================================
static void close_poll_fd(struct pollfd *fd){
	close(fd->fd);
	fd->fd = -1;
}
//==============================================================================================================
static void transfer_job_io(struct job *job, int input, int output){
	struct pollfd fds[] = {
		{.fd = input, .events = POLLOUT},
		{.fd = output, .events = POLLIN}
	};
	size_t sent = 0;
	size_t capacity = 0;
	char buffer[65536];

	while (fds[0].fd >= 0 || fds[1].fd >= 0) {
		if (poll(fds, 2, -1) < 0) {
			if (errno == EINTR) continue;
			die("poll command pipes", errno);
		}

		if (fds[1].revents) {
			ssize_t size = read(fds[1].fd, buffer, sizeof buffer);
			if (size > 0)
				append_bytes(&job->output, &job->output_length, &capacity, buffer, (size_t)size);
			else if (size == 0) close_poll_fd(&fds[1]);
			else if (errno != EINTR) die("read command stdout", errno);
		}

		if (fds[0].revents) {
			size_t remaining = job->length - sent;
			size_t chunk = remaining < sizeof buffer ? remaining : sizeof buffer;
			ssize_t size = write(fds[0].fd, job->input + sent, chunk);
			if (size > 0) {
				sent += (size_t)size;
				if (sent == job->length) close_poll_fd(&fds[0]);
			} else if (size < 0) {
				if (errno == EPIPE) close_poll_fd(&fds[0]);
				else if (errno != EINTR && errno != EAGAIN) die("write command stdin", errno);
			}
		}
	}
}
//==============================================================================================================
static bool run_job(struct job *job, char **command, const posix_spawnattr_t *attributes) {
	int input[2], output[2];
	if (pipe2(input, O_CLOEXEC) < 0) die("create stdin pipe", errno);
	if (pipe2(output, O_CLOEXEC) < 0) die("create stdout pipe", errno);
	if (fcntl(input[1], F_SETFL, O_NONBLOCK) < 0) die("set stdin pipe nonblocking", errno);
	
	posix_spawn_file_actions_t actions;
	check(posix_spawn_file_actions_init(&actions), "initialize spawn actions");
	check(posix_spawn_file_actions_adddup2(&actions, input[0], STDIN_FILENO), "redirect command stdin");
	check(posix_spawn_file_actions_adddup2(&actions, output[1], STDOUT_FILENO), "redirect command stdout");

	pid_t child;
	int error = posix_spawnp(&child, command[0], &actions, attributes, command, environ);
	posix_spawn_file_actions_destroy(&actions);
	close(input[0]);
	close(output[1]);
	if (error) {
		close(input[1]);
		close(output[0]);
		fprintf(stderr, "xpipe: %s: %s\n", command[0], strerror(error));
		return false;
	}
	transfer_job_io(job, input[1], output[0]);

	int status;
	while (waitpid(child, &status, 0) < 0) if (errno != EINTR) die("wait for command", errno);

	if (WIFSIGNALED(status)) 
		fprintf(stderr, "xpipe: %s terminated by signal %d\n", command[0], WTERMSIG(status));
	else if (WEXITSTATUS(status))
		fprintf(stderr, "xpipe: %s exited with status %d\n", command[0], WEXITSTATUS(status));
	return WIFEXITED(status) && WEXITSTATUS(status) == 0;
}
//==============================================================================================================
static void enqueue_output(struct state *state, struct job *job) {
	if (state->output_tail)	state->output_tail->next_output = job;
	else state->output_head = job;
	state->output_tail = job;
}
//==============================================================================================================
struct worker_args {
	struct state *state;
	const posix_spawnattr_t *attributes;
};
//==============================================================================================================
static void *worker(void *argument) {
	const struct worker_args *args = argument;
	struct state *state = args->state;
	pthread_mutex_lock(&state->lock);
	for (;;) {
		while (!state->input_head && !state->eof) pthread_cond_wait(&state->work_ready, &state->lock);
		struct job *job = state->input_head;
		if (!job) break;

		state->input_head = job->next_input;
		if (!state->input_head) state->input_tail = NULL; 
		pthread_mutex_unlock(&state->lock);

		bool success = run_job(job, state->command, args->attributes);
		free(job->input);

		pthread_mutex_lock(&state->lock);
		state->failed |= !success;
		job->done = true;
		if (!state->ordered)
		enqueue_output(state, job);
		pthread_cond_signal(&state->output_ready);
	}
	pthread_mutex_unlock(&state->lock);
	return NULL;
}
//==============================================================================================================
static void emit_output(const struct job * job, bool *needs_separator) {
	if (!job->output_length) return;
	if (needs_separator && *needs_separator) check(write_all(STDOUT_FILENO, "\n", 1), "write stdout");
	check(write_all(STDOUT_FILENO, job->output, job->output_length), "write stdout");
	if (needs_separator) *needs_separator = job->output[job->output_length - 1] != '\n';
}
//==============================================================================================================
static void *writer(void *argument) {
	struct state *state = argument;
	bool needs_separator = false;
	pthread_mutex_lock(&state->lock);
	while (!state->eof || state->pending) {
		struct job *job = state->output_head;
		if (!job || !job->done) {
			pthread_cond_wait(&state->output_ready, &state->lock);
			continue;
		}
		state->output_head = job->next_output;
		if (!state->output_head) state->output_tail = NULL;
		pthread_mutex_unlock(&state->lock);
		emit_output(job, state->ordered ? NULL : &needs_separator);
		free(job->output);
		free(job);
		pthread_mutex_lock(&state->lock);
		--state->pending;
		pthread_cond_signal(&state->slot_ready);
	}
	pthread_mutex_unlock(&state->lock);
	return NULL;
}
//==============================================================================================================
static struct job *read_batch(size_t lines, char **line, size_t *line_capacity, int *read_error) {
	struct job *job = calloc(1, sizeof *job);
	if (!job) die("allocate job", errno);

	size_t capacity = 0;
	for (size_t i = 0; i < lines; ++i) {
		ssize_t size = getline(line, line_capacity, stdin);
		if (size < 0) {
			if (!feof(stdin)) *read_error = errno ? errno : EIO;
			break;
		}
		if ((size_t)size > SIZE_MAX - job->length) die("input batch too large", EOVERFLOW);
		size_t required = job->length + (size_t)size;
		if (required > capacity) {
			capacity = capacity <= SIZE_MAX / 2 ? capacity * 2 : SIZE_MAX;
			if (capacity < required) capacity = required;
			char *buffer = realloc(job->input, capacity);
			if (!buffer) die("allocate input batch", errno);
			job->input = buffer;
		}
		memcpy(job->input + job->length, *line, (size_t)size);
		job->length = required;
	}
	if (job->length) return job;
	free(job);
	return NULL;
}
//==============================================================================================================
static void usage(FILE *stream) {
	fputs("Usage: xpipe [-j N] [-L N] [-o] command [args ...]\n"
		"  -j N  Concurrent jobs (default: core_count)\n"
		"  -L N  Input lines per job (default: 1)\n"
		"  -o    Emit job output in input order\n", stream);
}
//==============================================================================================================
int main(int argc, char **argv) {
	struct state state = { 
		.lock = PTHREAD_MUTEX_INITIALIZER,
		.work_ready = PTHREAD_COND_INITIALIZER,
		.output_ready = PTHREAD_COND_INITIALIZER,
		.slot_ready = PTHREAD_COND_INITIALIZER
	};
	size_t lines = 1;
	int option;
	while ((option = getopt(argc, argv, "+j:L:oh")) != -1) {
		switch (option) {
			case 'j':
				state.limit = positive_number(optarg);
				if (state.limit) continue;
				break;
			case 'L':
				lines = positive_number(optarg);
				if (lines) continue;
				break;
			case 'o':
				state.ordered = true;
				continue;
			case 'h':
				usage(stdout);
				return 0;
		}
		usage(stderr);
		return 2;
	}
	if (optind == argc) {
		usage(stderr);
		return 2;
	}

	if (fcntl(STDIN_FILENO, F_GETFD) < 0) die("check stdin", errno);
	if (fcntl(STDOUT_FILENO, F_GETFD) < 0) die("check stdout", errno);

	state.command = argv + optind;
	if (!state.limit) state.limit = default_jobs();

	char *line = NULL;
	size_t line_capacity = 0;
	int read_error = 0;
	struct job *job = read_batch(lines, &line, &line_capacity, &read_error);
	if (!job && !read_error) {
		free(line);
		return 0;
	}

	// An early stdin close is normal for commands such as head.
	if (signal(SIGPIPE, SIG_IGN) == SIG_ERR) die("ignore SIGPIPE", errno);
	sigset_t defaults;
	sigemptyset(&defaults);
	sigaddset(&defaults, SIGPIPE);
	posix_spawnattr_t attributes;
	check(posix_spawnattr_init(&attributes), "initialize spawn attributes");
	check(posix_spawnattr_setsigdefault(&attributes, &defaults), "set child signals");
	check(posix_spawnattr_setflags(&attributes, POSIX_SPAWN_SETSIGDEF), "set spawn flags");

	struct worker_args args = {&state, &attributes};
	pthread_t *workers = calloc(state.limit, sizeof *workers);
	if (!workers) die("allocate workers", errno);
	size_t worker_count = 0;
	pthread_t output_thread;
	check(pthread_create(&output_thread, NULL, writer, &state), "start writer");

	while (job) {
		pthread_mutex_lock(&state.lock);
		if (worker_count < state.limit && worker_count <= state.pending) {
			check(pthread_create(&workers[worker_count], NULL, worker, &args), "start worker");
			++worker_count;
		}
		if (state.input_tail) state.input_tail->next_input = job;
		else state.input_head = job;
		state.input_tail = job;
		if (state.ordered) enqueue_output(&state, job);
		++state.pending;
		pthread_cond_signal(&state.work_ready);
		while (state.pending == state.limit) pthread_cond_wait(&state.slot_ready, &state.lock);
		pthread_mutex_unlock(&state.lock);

		job = read_error || feof(stdin) ? NULL : read_batch(lines, &line, &line_capacity, &read_error);
	}

	pthread_mutex_lock(&state.lock);
	state.eof = true;
	pthread_cond_broadcast(&state.work_ready);
	pthread_cond_signal(&state.output_ready);
	pthread_mutex_unlock(&state.lock);

	for (size_t i = 0; i < worker_count; ++i) pthread_join(workers[i], NULL);
	pthread_join(output_thread, NULL);
	posix_spawnattr_destroy(&attributes);
	free(workers);
	free(line);
	if (read_error) die("read stdin", read_error);
	return state.failed ? EXIT_FAILURE : EXIT_SUCCESS;
}
//==============================================================================================================
