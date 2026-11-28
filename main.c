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
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>
#include <sched.h>
//==============================================================================================================
extern char **environ;
//==============================================================================================================
struct input_block {
	struct input_block *next;
	size_t length;
	char data[];
};

struct job {
	struct input_block *block_head, *block_tail;
	bool incremental, input_done;
	pthread_mutex_t input_lock;
	pthread_cond_t input_ready;
	char *input;
	size_t length;
	off_t start, end;
	char *output;
	size_t output_length;
	int output_fd;
	bool output_ready, done;
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
	int partition_fd;
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
static void publish_job_stdout(struct state *state, struct job *job, int fd) {
	// The caller holds the state lock and stops reading fd after publication.
	job->output_fd = fd;
	job->output_ready = true;
	pthread_cond_signal(&state->output_ready);
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
static int spawn_command(char **command, const posix_spawnattr_t *attributes,
			 pid_t *child, int *child_input, int *child_output, bool ordered) {
	int input[2], output[2];
	if (pipe2(input, O_CLOEXEC) < 0) die("create stdin pipe", errno);
	if (pipe2(output, O_CLOEXEC) < 0) die("create stdout pipe", errno);
	if (ordered) {
		// The kernel may limit pipe growth per user.
		if (fcntl(input[1], F_GETPIPE_SZ) < 1048576) (void)fcntl(input[1], F_SETPIPE_SZ, 1048576);
		if (fcntl(output[0], F_GETPIPE_SZ) < 1048576) (void)fcntl(output[0], F_SETPIPE_SZ, 1048576);
	}
	if (fcntl(input[1], F_SETFL, O_NONBLOCK) < 0) die("set stdin pipe nonblocking", errno);
	
	posix_spawn_file_actions_t actions;
	check(posix_spawn_file_actions_init(&actions), "initialize spawn actions");
	check(posix_spawn_file_actions_adddup2(&actions, input[0], STDIN_FILENO), "redirect command stdin");
	check(posix_spawn_file_actions_adddup2(&actions, output[1], STDOUT_FILENO), "redirect command stdout");

	int error = posix_spawnp(child, command[0], &actions, attributes, command, environ);
	posix_spawn_file_actions_destroy(&actions);
	close(input[0]);
	close(output[1]);
	if (error) {
		close(input[1]);
		close(output[0]);
		return error;
	}
	*child_input = input[1];
	*child_output = output[0];
	return 0;
}
//==============================================================================================================
static bool wait_command(pid_t child, const char *command, bool report) {
	int status;
	while (waitpid(child, &status, 0) < 0) if (errno != EINTR) die("wait for command", errno);

	if (report && WIFSIGNALED(status))
		fprintf(stderr, "xpipe: %s terminated by signal %d\n", command, WTERMSIG(status));
	else if (report && WIFEXITED(status) && WEXITSTATUS(status))
		fprintf(stderr, "xpipe: %s exited with status %d\n", command, WEXITSTATUS(status));
	return WIFEXITED(status) && WEXITSTATUS(status) == 0;
}
//==============================================================================================================
static ssize_t read_partition(int fd, off_t start, off_t end, char *buffer, size_t capacity) {
	off_t remaining = end - start;
	size_t length = (uintmax_t)remaining < capacity ? (size_t)remaining : capacity;
	ssize_t size;
	do {
		size = pread(fd, buffer, length, start);
	} while (size < 0 && errno == EINTR);
	return size;
}
//==============================================================================================================
static int send_partition(int fd, const struct job *job, int input) {
	char buffer[65536];
	for (off_t offset = job->start; offset < job->end;) {
		ssize_t size = read_partition(fd, offset, job->end, buffer, sizeof buffer);
		if (size <= 0) die("read input partition", size < 0 ? errno : EIO);
		int error = write_all(input, buffer, (size_t)size);
		if (error) return error;
		offset += size;
	}
	return 0;
}
//==============================================================================================================
struct input_feeder {
	struct job *job;
	int fd;
};

static void *feed_blocks(void *argument) {
	struct input_feeder *feed = argument;
	struct job *job = feed->job;
	int fd = feed->fd;

	pthread_mutex_lock(&job->input_lock);
	for (;;) {
		while (!job->block_head && !job->input_done)
			pthread_cond_wait(&job->input_ready, &job->input_lock);

		struct input_block *block = job->block_head;
		if (!block) break;

		job->block_head = NULL;
		job->block_tail = NULL;
		pthread_mutex_unlock(&job->input_lock);

		while (block) {
			struct input_block *next = block->next;

			if (fd >= 0) {
				int error = write_all(fd, block->data, block->length);
				if (error == EPIPE) {
					close(fd);
					fd = -1;
				} else check(error, "write command stdin");
			}

			free(block);
			block = next;
		}

		pthread_mutex_lock(&job->input_lock);
	}
	pthread_mutex_unlock(&job->input_lock);
	if (fd >= 0) close(fd);
	return NULL;
}

static void drain_ordered_job(struct state *state, struct job *job, int output) {
	char buffer[65536];
	size_t capacity = 0;

	for (;;) {
		pthread_mutex_lock(&state->lock);
		bool first = state->output_head == job;
		if (first) publish_job_stdout(state, job, output);
		pthread_mutex_unlock(&state->lock);
		if (first) return;

		ssize_t size = read(output, buffer, sizeof buffer);
		if (size < 0 && errno == EINTR) continue;
		if (size < 0) die("read command stdout", errno);
		if (!size) break;
		append_bytes(&job->output, &job->output_length, &capacity, buffer, (size_t)size);
	}
	close(output);
}
static bool run_job(struct job *job, struct state *state, const posix_spawnattr_t *attributes) {
	char **command = state->command;
	pid_t child;
	int input, output;
	int error = spawn_command(command, attributes, &child, &input, &output, state->ordered);
	if (error) {
		fprintf(stderr, "xpipe: %s: %s\n", command[0], strerror(error));
		if (job->incremental) {
			struct input_feeder feeder = {job, -1};
			feed_blocks(&feeder);
		}
		return false;
	}

	if (job->incremental) {
		if (fcntl(input, F_SETFL, 0) < 0) die("set command stdin blocking", errno);
		struct input_feeder feeder = {job, input};
		pthread_t thread;
		check(pthread_create(&thread, NULL, feed_blocks, &feeder), "start input feeder");
		drain_ordered_job(state, job, output);
		check(pthread_join(thread, NULL), "join input feeder");
	} else if (state->partition_fd >= 0) {
		if (fcntl(input, F_SETFL, 0) < 0) die("set command stdin blocking", errno);

		// The writer owns the read end and drains jobs in input order.
		pthread_mutex_lock(&state->lock);
		publish_job_stdout(state, job, output);
		pthread_mutex_unlock(&state->lock);

		error = send_partition(state->partition_fd, job, input);
		close(input);
		if (error != EPIPE) check(error, "write command stdin");
	} else {
		transfer_job_io(job, input, output);
	}

	return wait_command(child, command[0], true);
}
//==============================================================================================================
struct process_worker {
	pid_t pid;
	char *input, *output;
	size_t input_length, input_capacity, sent;
	size_t output_length, output_capacity;
};
//==============================================================================================================
static int emit_worker_lines(struct process_worker *worker, const char *data,
			      size_t length, bool eof, bool *needs_separator) {
	// Only the new bytes can contain a newline. The saved prefix is partial.
	size_t complete = length;
	while (complete && data[complete - 1] != '\n') --complete;

	if (complete || (eof && worker->output_length)) {
		int error = *needs_separator ? write_all(STDOUT_FILENO, "\n", 1) : 0;
		if (!error) error = write_all(STDOUT_FILENO, worker->output, worker->output_length);
		if (!error) error = write_all(STDOUT_FILENO, data, complete);
		if (error) return error;
		*needs_separator = !complete;
		worker->output_length = 0;
	}
	if (length > complete)
		append_bytes(&worker->output, &worker->output_length, &worker->output_capacity,
			     data + complete, length - complete);
	return 0;
}
//==============================================================================================================
static int stream_workers(char **command, size_t count, size_t lines,
			  const posix_spawnattr_t *attributes) {
	char input[65536], output[65536];
	ssize_t received;
	do {
		received = read(STDIN_FILENO, input, sizeof input);
	} while (received < 0 && errno == EINTR);
	if (received < 0) die("read stdin", errno);
	if (!received) return 0;

	if (count > (SIZE_MAX - 1) / 2) die("too many workers", EOVERFLOW);
	size_t nfds = 2 * count + 1;
	struct pollfd *fds = calloc(nfds, sizeof *fds);
	struct process_worker *workers = calloc(count, sizeof *workers);
	if (!fds || !workers) die("allocate process workers", ENOMEM);
	for (size_t i = 0; i < nfds; ++i) fds[i].fd = -1;

	const char *operation = NULL;
	int error = 0, result = 0;
	bool eof = false;
	size_t started = 0;
	for (; started < count; ++started) {
		struct pollfd *in = &fds[1 + 2 * started], *out = in + 1;
		error = spawn_command(command, attributes, &workers[started].pid, &in->fd, &out->fd, false);
		if (error) {
			fprintf(stderr, "xpipe: failed to run command '%s': %s\n", command[0], strerror(error));
			result = error == ENOENT ? 127 : 126;
			goto cleanup;
		}
		out->events = POLLIN;
	}

	size_t offset = 0, next = 0, batch_lines = 0, target = count, outputs = count;
	bool needs_separator = false;
	size_t available = (size_t)received;
	for (;;) {
		while (offset < available) {
			if (target == count) {
				// Dispatch a complete batch to the next worker with room.
				for (size_t examined = 0; examined < count; ++examined) {
					size_t i = next;
					next = (next + 1) % count;
					if (!workers[i].input_length && fds[1 + 2 * i].fd >= 0) {
						target = i;
						break;
					}
				}
				if (target == count) break;
			}

			struct process_worker *worker = &workers[target];
			const char *start = input + offset;
			const char *newline = memchr(start, '\n', available - offset);
			size_t length = newline ? (size_t)(newline - start) + 1 : available - offset;
			append_bytes(&worker->input, &worker->input_length, &worker->input_capacity,
				     start, length);
			offset += length;
			if (newline && ++batch_lines == lines) {
				fds[1 + 2 * target].events = POLLOUT;
				target = count;
				batch_lines = 0;
			}
		}

		if (eof && target < count) {
			fds[1 + 2 * target].events = POLLOUT;
			target = count;
		}
		bool accepting_input = false;
		for (size_t i = 0; i < count; ++i) {
			struct pollfd *in = &fds[1 + 2 * i];
			if (in->fd < 0) continue;
			if (eof && !workers[i].input_length) close_poll_fd(in);
			else accepting_input = true;
		}
		if (!accepting_input && offset < available) {
			operation = "all workers closed stdin with input pending";
			error = EPIPE;
			goto cleanup;
		}
		if (eof && !accepting_input && !outputs) break;
		fds[0].fd = !eof && offset == available ? STDIN_FILENO : -1;
		fds[0].events = POLLIN;
		if (poll(fds, nfds, -1) < 0) {
			if (errno == EINTR) continue;
			operation = "poll worker pipes";
			error = errno;
			goto cleanup;
		}

		for (size_t i = 0; i < count; ++i) {
			struct process_worker *worker = &workers[i];
			struct pollfd *in = &fds[1 + 2 * i], *out = in + 1;
			if (out->revents) {
				ssize_t size = read(out->fd, output, sizeof output);
				if (size < 0 && errno != EINTR) {
					operation = "read worker stdout";
					error = errno;
					goto cleanup;
				}
				if (size >= 0) {
					error = emit_worker_lines(worker, output, (size_t)size,
								  size == 0, &needs_separator);
					if (error) {
						operation = "write stdout";
						goto cleanup;
					}
					if (!size) {
						close_poll_fd(out);
						--outputs;
					}
				}
			}
			if (in->revents & (POLLERR | POLLHUP | POLLNVAL)) {
				if (worker->input_length) {
					operation = "worker closed stdin with input pending";
					error = EPIPE;
					goto cleanup;
				}
				close_poll_fd(in);
			} else if (in->revents & POLLOUT) {
				ssize_t size = write(in->fd, worker->input + worker->sent,
						     worker->input_length - worker->sent);
				if (size < 0 && errno != EINTR && errno != EAGAIN) {
					operation = "write worker stdin";
					error = errno;
					goto cleanup;
				}
				if (size > 0) worker->sent += (size_t)size;
				if (worker->sent == worker->input_length) {
					in->events = 0;
					worker->sent = worker->input_length = 0;
					if (eof) close_poll_fd(in);
				}
			}
		}
		if (fds[0].revents) {
			received = read(STDIN_FILENO, input, sizeof input);
			if (received < 0) {
				if (errno == EINTR || errno == EAGAIN) continue;
				operation = "read stdin";
				error = errno;
				goto cleanup;
			}
			eof = received == 0;
			available = (size_t)received;
			offset = 0;
		}
	}

cleanup:
	if (operation) {
		fprintf(stderr, "xpipe: %s: %s\n", operation, strerror(error));
		result = EXIT_FAILURE;
	}
	for (size_t i = 0; i < started; ++i) {
		struct pollfd *in = &fds[1 + 2 * i], *out = in + 1;
		if (in->fd >= 0) close_poll_fd(in);
		if (out->fd >= 0) close_poll_fd(out);
		if (error) kill(workers[i].pid, SIGKILL);
	}
	for (size_t i = 0; i < started; ++i) {
		if (!wait_command(workers[i].pid, command[0], !error) && !result)
			result = EXIT_FAILURE;
		free(workers[i].input);
		free(workers[i].output);
	}
	free(workers);
	free(fds);
	return result;
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

		bool success = run_job(job, state, args->attributes);
		if (job->incremental) {
			pthread_cond_destroy(&job->input_ready);
			pthread_mutex_destroy(&job->input_lock);
		}
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
static void stream_output(int fd, bool *splicing) {
	while (*splicing) {
		ssize_t size = splice(fd, NULL, STDOUT_FILENO, NULL, 1024 * 1024, 0);
		if (size > 0) continue;
		if (size == 0) {
			close(fd);
			return;
		}
		if (errno == EINTR) continue;
		if (errno != EINVAL && errno != ENOSYS && errno != EOPNOTSUPP && errno != EXDEV)
			die("splice command stdout", errno);
		*splicing = false;
	}

	char buffer[65536];
	for (;;) {
		ssize_t size = read(fd, buffer, sizeof buffer);
		if (size == 0) break;
		if (size < 0) {
			if (errno == EINTR) continue;
			die("read command stdout", errno);
		}
		check(write_all(STDOUT_FILENO, buffer, (size_t)size), "write stdout");
	}
	close(fd);
}
//==============================================================================================================
static void *writer(void *argument) {
	struct state *state = argument;
	bool needs_separator = false, splicing = true;
	pthread_mutex_lock(&state->lock);
	while (!state->eof || state->pending) {
		struct job *job = state->output_head;
		if (!job || (!job->done && !job->output_ready)) {
			pthread_cond_wait(&state->output_ready, &state->lock);
			continue;
		}
		if (state->ordered && job->output_ready) {
			int fd = job->output_fd;
			pthread_mutex_unlock(&state->lock);
			emit_output(job, NULL);
			stream_output(fd, &splicing);
			pthread_mutex_lock(&state->lock);
			while (!job->done) pthread_cond_wait(&state->output_ready, &state->lock);
		}
		state->output_head = job->next_output;
		if (!state->output_head) state->output_tail = NULL;
		pthread_mutex_unlock(&state->lock);
		if (!state->ordered) emit_output(job, &needs_separator);
		else if (!job->output_ready) emit_output(job, NULL);
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
struct batch_reader {
	char buffer[256 * 1024];
	size_t offset, length;
	int error;
	bool eof;
};
//==============================================================================================================
static bool refill_batch_reader(struct batch_reader *reader) {
	if (reader->offset < reader->length) return true;
	if (reader->eof) return false;

	ssize_t size;
	do {
		size = read(STDIN_FILENO, reader->buffer, sizeof reader->buffer);
	} while (size < 0 && errno == EINTR);
	if (size <= 0) {
		reader->eof = true;
		if (size < 0) reader->error = errno;
		return false;
	}
	reader->offset = 0;
	reader->length = (size_t)size;
	return true;
}

static void queue_batch_blocks(struct job *job,
			       struct batch_reader *reader, size_t lines) {
	while (lines && refill_batch_reader(reader)) {
		const char *start = reader->buffer + reader->offset;
		const char *end = reader->buffer + reader->length;
		const char *cursor = start;
		while (lines && cursor < end) {
			const char *newline = memchr(cursor, '\n', (size_t)(end - cursor));
			if (!newline) {
				cursor = end;
				break;
			}
			cursor = newline + 1;
			--lines;
		}
		size_t length = (size_t)(cursor - start);
		struct input_block *block = malloc(sizeof *block + length);
		if (!block) die("allocate input block", errno);
		block->next = NULL;
		block->length = length;
		memcpy(block->data, start, length);
		reader->offset += length;

		pthread_mutex_lock(&job->input_lock);
		bool was_empty = !job->block_head;
		if (job->block_tail) job->block_tail->next = block;
		else job->block_head = block;
		job->block_tail = block;
		if (was_empty) pthread_cond_signal(&job->input_ready);
		pthread_mutex_unlock(&job->input_lock);
	}
	pthread_mutex_lock(&job->input_lock);
	job->input_done = true;
	pthread_cond_signal(&job->input_ready);
	pthread_mutex_unlock(&job->input_lock);
}

static struct job *next_incremental_batch(struct batch_reader *reader) {
	if (!refill_batch_reader(reader)) return NULL;
	struct job *job = calloc(1, sizeof *job);
	if (!job) die("allocate job", errno);
	job->incremental = true;
	check(pthread_mutex_init(&job->input_lock, NULL), "initialize feeder lock");
	check(pthread_cond_init(&job->input_ready, NULL), "initialize feeder condition");
	return job;
}
//==============================================================================================================
static struct job *read_batch(size_t lines, struct batch_reader *reader) {
	struct job *job = calloc(1, sizeof *job);
	if (!job) die("allocate job", errno);

	size_t capacity = 0;
	while (lines && refill_batch_reader(reader)) {
		const char *start = reader->buffer + reader->offset;
		const char *end = reader->buffer + reader->length;
		const char *cursor = start;
		while (lines && cursor < end) {
			const char *newline = memchr(cursor, '\n', (size_t)(end - cursor));
			if (!newline) {
				cursor = end;
				break;
			}
			cursor = newline + 1;
			--lines;
		}
		size_t length = (size_t)(cursor - start);
		append_bytes(&job->input, &job->length, &capacity, start, length);
		reader->offset += length;
	}

	if (job->length) return job;
	free(job);
	return NULL;
}
//==============================================================================================================
struct partitions {
	off_t start, end, target, step;
	size_t extra;
};
//==============================================================================================================
static struct partitions prepare_partitions(size_t count) {
	struct stat info;
	if (fstat(STDIN_FILENO, &info) < 0) die("stat stdin", errno);
	if (!S_ISREG(info.st_mode)) {
		fputs("xpipe: -w -o requires a regular file on stdin. Use -o for piped input.\n", stderr);
		exit(EXIT_FAILURE);
	}

	off_t start = lseek(STDIN_FILENO, 0, SEEK_CUR);
	if (start < 0) die("seek stdin", errno);
	off_t end = info.st_size;

	if (start > end) start = end;
	uintmax_t length = (uintmax_t)(end - start);
	return (struct partitions){
		.start = start, .end = end, .target = start,
		.step = (off_t)(length / count), .extra = (size_t)(length % count)
	};
}
//==============================================================================================================
static struct job *next_partition(struct partitions *parts) {
	if (parts->start == parts->end) return NULL;

	// Advance past nominal cuts already covered by an earlier long line.
	while (parts->target <= parts->start) {
		parts->target += parts->step;
		if (parts->extra) {
			++parts->target;
			--parts->extra;
		}
	}

	char buffer[65536];
	off_t boundary = parts->target;
	if (boundary < parts->end) {
		--boundary; // Keep the cut as-is if it already follows a newline.
		while (boundary < parts->end) {
			ssize_t size = read_partition(STDIN_FILENO, boundary, parts->end, buffer, sizeof buffer);
			if (size <= 0) die("find partition boundary", size < 0 ? errno : EIO);
			const char *newline = memchr(buffer, '\n', (size_t)size);
			boundary += newline ? newline - buffer + 1 : size;
			if (newline) break;
		}
	}

	struct job *job = calloc(1, sizeof *job);
	if (!job) die("allocate partition job", errno);
	job->start = parts->start;
	job->end = boundary;
	parts->start = boundary;
	return job;
}
//==============================================================================================================
static void usage(FILE *stream) {
	fputs("Usage: xpipe [-j N] [-L N] [-o] [-w] command [args ...]\n"
		"  -j N  Concurrent jobs (default: core_count)\n"
		"  -L N  Input lines per job (default: 1)\n"
		"  -o    Emit job output in input order, feeding batches as they arrive\n"
		"  -w    Reuse command processes and merge complete stdout lines\n"
		"  -w -o Split input into contiguous partitions and stream their outputs in order\n"
		"        Requires a regular file on stdin and ignores -L\n", stream);
}
//==============================================================================================================
int main(int argc, char **argv) {
	struct state state = { 
		.lock = PTHREAD_MUTEX_INITIALIZER,
		.work_ready = PTHREAD_COND_INITIALIZER,
		.output_ready = PTHREAD_COND_INITIALIZER,
		.slot_ready = PTHREAD_COND_INITIALIZER,
		.partition_fd = -1
	};
	size_t lines = 1;
	bool persistent = false;
	int option;
	while ((option = getopt(argc, argv, "+j:L:owh")) != -1) {
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
			case 'w':
				persistent = true;
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

	// An early stdin close is normal for commands such as head.
	if (signal(SIGPIPE, SIG_IGN) == SIG_ERR) die("ignore SIGPIPE", errno);
	sigset_t defaults;
	sigemptyset(&defaults);
	sigaddset(&defaults, SIGPIPE);
	posix_spawnattr_t attributes;
	check(posix_spawnattr_init(&attributes), "initialize spawn attributes");
	check(posix_spawnattr_setsigdefault(&attributes, &defaults), "set child signals");
	check(posix_spawnattr_setflags(&attributes, POSIX_SPAWN_SETSIGDEF), "set spawn flags");

	if (persistent && !state.ordered) {
		int status = stream_workers(state.command, state.limit, lines, &attributes);
		posix_spawnattr_destroy(&attributes);
		return status;
	}

	struct batch_reader reader = {0};
	bool incremental = state.ordered && !persistent;
	struct partitions parts = {0};
	struct job *job;
	if (persistent) {
		parts = prepare_partitions(state.limit);
		state.partition_fd = STDIN_FILENO;
		job = next_partition(&parts);
	} else {
		job = incremental ? next_incremental_batch(&reader) : read_batch(lines, &reader);
	}
	if (!job && !reader.error) {
		posix_spawnattr_destroy(&attributes);
		return 0;
	}

	struct worker_args args = {&state, &attributes};
	pthread_t *workers = calloc(state.limit, sizeof *workers);
	if (!workers) die("allocate workers", errno);
	size_t worker_count = 0;
	pthread_t output_thread;
	check(pthread_create(&output_thread, NULL, writer, &state), "start writer");

	while (job) {
		pthread_mutex_lock(&state.lock);
		while (state.pending == state.limit)
			pthread_cond_wait(&state.slot_ready, &state.lock);
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
		pthread_mutex_unlock(&state.lock);

		// Prepare one batch ahead while the active children consume their own inputs.
		if (incremental) {
			queue_batch_blocks(job, &reader, lines);
			job = next_incremental_batch(&reader);
		} else if (persistent) job = next_partition(&parts);
		else job = reader.eof ? NULL : read_batch(lines, &reader);
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
	if (reader.error) die("read stdin", reader.error);
	return state.failed ? EXIT_FAILURE : EXIT_SUCCESS;
}
//==============================================================================================================
