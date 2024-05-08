// Copyright (C) 2016, 2017 Alexey Khrabrov, Bogdan Simion
//               2020, 2024 Angela Demke Brown
// Distributed under the terms of the GNU General Public License.
//
// This file is part of Assignment 3, CSC469, Sprint 2024.
//
// This is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This file is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this file.  If not, see <http://www.gnu.org/licenses/>.

// The key-value server implementation

#include <assert.h>
#include <errno.h>
#include <limits.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include <sys/types.h>
#include <sys/socket.h>
#include <sys/prctl.h> // for prctl and macro

#include "defs.h"
#include "hash.h"
#include "util.h"

#include "fcntl.h" // for writing to files

// Program arguments

// Host name and port number of the metadata server
static char coord_host_name[HOST_NAME_MAX] = "";
static uint16_t coord_port = 0;

// Ports for listening to incoming connections from clients, servers and coord
static uint16_t clients_port = 0;
static uint16_t servers_port = 0;
static uint16_t coord_in_port = 0;

// Current server id and total number of servers
static int server_id = -1;
static int num_servers = 0;

// heartbeat period (time in seconds between heartbeat messages to coordinator)
static int t_heartbeat = INT_MAX;

// Log file name
static char log_file_name[PATH_MAX] = "";

typedef enum
{
	INACTIVE,
	ONGOING,
	SWITCH_PRIMARY_RECEIVED,
} recovery_status;
static recovery_status recovery_process_status = INACTIVE;

static void usage(char **argv)
{
	printf("usage: %s -h <coord host> -m <coord port> -c <clients port> "
		   "-s <servers port> -M <coord incoming port> -S <server id> "
		   "-n <num servers> -k <keepalive (heartbeat) interval> "
		   "[-l <log file>]\n",
		   argv[0]);
	printf("If the log file (-l) is not specified, log output is written "
		   "to stdout\n");
}

// Returns false if the arguments are invalid
static bool parse_args(int argc, char **argv)
{
	char option;
	while ((option = getopt(argc, argv, "h:m:c:s:M:S:n:k:l:")) != -1)
	{
		switch (option)
		{
		case 'h':
			strncpy(coord_host_name, optarg, HOST_NAME_MAX);
			break;
		case 'm':
			coord_port = atoi(optarg);
			break;
		case 'c':
			clients_port = atoi(optarg);
			break;
		case 's':
			servers_port = atoi(optarg);
			break;
		case 'M':
			coord_in_port = atoi(optarg);
			break;
		case 'S':
			server_id = atoi(optarg);
			break;
		case 'n':
			num_servers = atoi(optarg);
			break;
		case 'k':
			t_heartbeat = atoi(optarg);
			break;
		case 'l':
			strncpy(log_file_name, optarg, PATH_MAX);
			break;
		default:
			fprintf(stderr, "Invalid option: -%c\n", option);
			return false;
		}
	}

	// Allow server to choose own ports. Uncomment extra conditions if
	// server ports must be specified on command line.
	return (coord_host_name[0] != '\0') && (coord_port != 0) &&
		   //(clients_port != 0) && (servers_port != 0) &&
		   //(coord_in_port != 0) &&
		   (num_servers >= 3) && (server_id >= 0) && (server_id < num_servers);
}

// Socket for sending requests to the coordinator
static int coord_fd_out = -1;
pthread_mutex_t coord_fd_mutex = PTHREAD_MUTEX_INITIALIZER;
// Socket for receiving requests from the coordinator
static int coord_fd_in = -1;

// Sockets for listening for incoming connections from clients, servers, and coordinator
static int my_clients_fd = -1;
static int my_servers_fd = -1;
static int my_coord_fd = -1;

// Store fds for all connected clients, up to MAX_CLIENT_SESSIONS
static int client_fd_table[MAX_CLIENT_SESSIONS];

// Store fds for connected servers
#define MAX_SERVER_SESSIONS 2
static int server_fd_table[MAX_SERVER_SESSIONS] = {-1, -1};

// Storage for this server's primary key set
hash_table primary_hash = {0};

// Storage for this server's secondary key set. (storage implemented by us - not provided in starter code)
hash_table secondary_hash = {0};

// Primary server (the one that stores the primary copy for this server's
// secondary key set)
static int primary_sid = -1;
static int primary_fd = -1;
// Mutex for synchronizing access to the shared file descriptor
pthread_mutex_t primary_fd_mutex = PTHREAD_MUTEX_INITIALIZER;

// Secondary server (the one that stores the secondary copy for this server's
// primary key set)
static int secondary_sid = -1;
static int secondary_fd = -1;
pthread_mutex_t secondary_fd_mutex = PTHREAD_MUTEX_INITIALIZER;

static void cleanup();

static const int hash_size = 65536;

void *t_send_heartbeat()
{
	// send heartbeat message every t_heartbeat seconds
	while (1)
	{
		char send_buf[MAX_MSG_LEN] = {0};
		coord_ctrl_request *req = (coord_ctrl_request *)send_buf;
		req->hdr.type = MSG_COORD_CTRL_REQ;
		req->type = HEARTBEAT;
		req->server_id = server_id;
		pthread_mutex_lock(&coord_fd_mutex);
		send_msg(coord_fd_out, req, sizeof(*req));
		pthread_mutex_unlock(&coord_fd_mutex);
		sleep(t_heartbeat);
	}
}

// Initialize and start the server
static bool init_server()
{
	for (int i = 0; i < MAX_CLIENT_SESSIONS; i++)
	{
		client_fd_table[i] = -1;
	}

	// Get the host name that server is running on
	char my_host_name[HOST_NAME_MAX] = "";
	char timebuf[TIME_STR_SIZE];

	if (get_local_host_name(my_host_name, sizeof(my_host_name)) < 0)
	{
		return false;
	}
	log_write("%s Server starts on host: %s\n",
			  current_time_str(timebuf, TIME_STR_SIZE), my_host_name);

	// Create sockets for incoming connections from clients and other servers
	uint16_t newport = 0;
	my_clients_fd = create_server(clients_port, MAX_CLIENT_SESSIONS, &newport);
	if (my_clients_fd < 0)
	{
		goto cleanup;
	}
	if (newport != 0)
	{
		clients_port = newport;
		newport = 0;
	}

	my_servers_fd = create_server(servers_port, MAX_SERVER_SESSIONS, &newport);
	if (my_servers_fd < 0)
	{
		goto cleanup;
	}
	if (newport != 0)
	{
		servers_port = newport;
		newport = 0;
	}

	my_coord_fd = create_server(coord_in_port, 1, &newport);
	if (my_coord_fd < 0)
	{
		goto cleanup;
	}
	if (newport != 0)
	{
		coord_in_port = newport;
		newport = 0;
	}

	// Determine the ids of replica servers
	primary_sid = primary_server_id(server_id, num_servers);
	secondary_sid = secondary_server_id(server_id, num_servers);

	// Initialize key-value storage
	if (!hash_init(&primary_hash, hash_size))
	{
		goto cleanup;
	}

	// Initialize secondary hash
	if (!hash_init(&secondary_hash, hash_size))
	{
		goto cleanup;
	}

	// Connect to coordinator to "register" that we are live
	if ((coord_fd_out = connect_to_server(coord_host_name, coord_port)) < 0)
	{
		goto cleanup;
	}
	// Tell coordinator about the port numbers we are using
	char send_buffer[MAX_MSG_LEN] = {0};
	coord_ctrl_request *req = (coord_ctrl_request *)send_buffer;
	req->hdr.type = MSG_COORD_CTRL_REQ;
	req->type = STARTED;
	req->server_id = server_id;
	req->ports[0] = clients_port;
	req->ports[1] = servers_port;
	req->ports[2] = coord_in_port;

	if (!send_msg(coord_fd_out, req, sizeof(*req) + 3 * sizeof(uint16_t)))
	{
		goto cleanup;
	}

	// set up thread to send heartbeat messages periodically
	pthread_t thread_id;
	if (pthread_create(&thread_id, NULL, t_send_heartbeat, NULL) != 0)
	{
		log_perror("pthread_create");
		goto cleanup;
	}

	log_write("Server initialized\n");
	return true;

cleanup:
	log_write("Server initialization failed.\n");
	cleanup();
	return false;
}

// Hash iterator for freeing memory used by values; called during storage cleanup
static void clean_iterator_f(const char key[KEY_SIZE], void *value, size_t value_sz, void *arg)
{
	(void)key;
	(void)value_sz;
	(void)arg;

	assert(value != NULL);
	free(value);
}

// Cleanup and release all the resources
static void cleanup()
{
	log_write("Cleaning up and exiting ...\n");

	close_safe(&coord_fd_out);
	close_safe(&coord_fd_in);
	close_safe(&my_clients_fd);
	close_safe(&my_servers_fd);
	close_safe(&my_coord_fd);
	close_safe(&secondary_fd);
	close_safe(&primary_fd);

	for (int i = 0; i < MAX_CLIENT_SESSIONS; i++)
	{
		close_safe(&(client_fd_table[i]));
	}
	for (int i = 0; i < MAX_SERVER_SESSIONS; i++)
	{
		close_safe(&(server_fd_table[i]));
	}

	hash_iterate(&primary_hash, clean_iterator_f, NULL);
	hash_cleanup(&primary_hash);

	hash_iterate(&secondary_hash, clean_iterator_f, NULL);
	hash_cleanup(&secondary_hash);
}

// Connection will be closed after calling this function regardless of result
static void process_client_message(int fd)
{
	char timebuf[TIME_STR_SIZE];

	log_write("%s Receiving a client message\n", current_time_str(timebuf, TIME_STR_SIZE));

	// Read and parse the message
	char req_buffer[MAX_MSG_LEN] = {0};
	if (!recv_msg(fd, req_buffer, MAX_MSG_LEN, MSG_OPERATION_REQ))
	{
		return;
	}
	operation_request *request = (operation_request *)req_buffer;

	// Initialize the response
	char resp_buffer[MAX_MSG_LEN] = {0};
	operation_response *response = (operation_response *)resp_buffer;
	response->hdr.type = MSG_OPERATION_RESP;
	uint16_t value_sz = 0;

	// Check that requested key is valid.
	// A server should only respond to requests for which it holds the
	// primary replica. For debugging and testing, however, we also want
	// to allow the secondary server to respond to OP_VERIFY requests,
	// to confirm that replication has succeeded. To check this, we need
	// to know the primary server id for which this server is the secondary.
	int key_srv_id = key_server_id(request->key, num_servers);
	if (key_srv_id != server_id && key_srv_id != primary_sid)
	{
		// This can happen if client is using old config during recovery
		response->status = INVALID_REQUEST;
		send_msg(fd, response, sizeof(*response) + value_sz);
		return;
	}

	// Process the request based on its type
	switch (request->type)
	{
	case OP_NOOP:
		if (key_srv_id != server_id && !(recovery_process_status == ONGOING || recovery_process_status == SWITCH_PRIMARY_RECEIVED))
		{
			response->status = INVALID_REQUEST;
			break;
		}
		response->status = SUCCESS;
		break;

	case OP_GET:
	{
		void *data = NULL;
		size_t size = 0;

		if (key_srv_id == server_id)
		{
			// Get the value for requested key from the hash table
			if (!hash_get(&primary_hash, request->key, &data, &size))
			{
				log_write("Key %s not found\n", key_to_str(request->key));
				response->status = KEY_NOT_FOUND;
				break;
			}
		}
		else if ((recovery_process_status == ONGOING || recovery_process_status == SWITCH_PRIMARY_RECEIVED) && key_srv_id == primary_sid)
		{
			// Get the value for requested key from the hash table
			if (!hash_get(&secondary_hash, request->key, &data, &size))
			{
				log_write("Key %s not found\n", key_to_str(request->key));
				response->status = KEY_NOT_FOUND;
				break;
			}
		}
		else
		{
			// Invalid operation
			log_write("Invalid operation: key %s sid %d\n", key_to_str(request->key), key_srv_id);
			response->status = INVALID_REQUEST;
			break;
		}

		// Copy the stored value into the response buffer
		memcpy(response->value, data, size);
		value_sz = size;

		response->status = SUCCESS;
		break;
	}

	case OP_PUT:
	{
		// Need to copy the value to dynamically allocated memory
		size_t value_size = request->hdr.length - sizeof(*request);
		void *value_copy = malloc(value_size);

		if (value_copy == NULL)
		{
			log_perror("malloc");
			log_error("sid %d: Out of memory\n", server_id);
			response->status = OUT_OF_SPACE;
			break;
		}
		memcpy(value_copy, request->value, value_size);

		void *old_value = NULL;
		size_t old_value_sz = 0;
		if (key_srv_id == server_id)
		{
			// Put the <key, value> pair into the hash table
			hash_lock(&primary_hash, request->key); // lock hash bucket for current key.
			if (!hash_put(&primary_hash, request->key, value_copy, value_size,
						  &old_value, &old_value_sz))
			{
				// unlock bucket, as we are breaking on failure.
				hash_unlock(&primary_hash, request->key);
				log_error("sid %d: Out of memory\n", server_id);
				free(value_copy);
				response->status = OUT_OF_SPACE;
				break;
			}
			operation_response replication_response = {0};
			pthread_mutex_lock(&secondary_fd_mutex);
			if (!send_msg(secondary_fd, request, sizeof(*request) + value_size) ||
				!recv_msg(secondary_fd, &replication_response, MAX_MSG_LEN, MSG_OPERATION_RESP))
				response->status = SERVER_FAILURE;
			pthread_mutex_unlock(&secondary_fd_mutex);
			hash_unlock(&primary_hash, request->key); // unlock bucket, as value has been inserted and propogated.
			if (replication_response.status != SUCCESS)
				response->status = SERVER_FAILURE;
		}
		else if ((recovery_process_status == ONGOING || recovery_process_status == SWITCH_PRIMARY_RECEIVED) && key_srv_id == primary_sid)
		{
			// if recovery process is ongoing
			// or if the server has just received the switch_primary request from coord,
			// need to finish handling current requests before refusing further client requests
			// Put the <key, value> pair into the hash table
			hash_lock(&secondary_hash, request->key); // lock hash bucket for current key.
			if (!hash_put(&secondary_hash, request->key, value_copy, value_size,
						  &old_value, &old_value_sz))
			{
				// unlock bucket, as we are breaking on failure.
				hash_unlock(&secondary_hash, request->key);
				log_error("sid %d: Out of memory\n", server_id);
				free(value_copy);
				response->status = OUT_OF_SPACE;
				break;
			}
			operation_response replication_response = {0};
			pthread_mutex_lock(&primary_fd_mutex);
			if (!send_msg(primary_fd, request, sizeof(*request) + value_size) ||
				!recv_msg(primary_fd, &replication_response, MAX_MSG_LEN, MSG_OPERATION_RESP))
			{
				pthread_mutex_unlock(&primary_fd_mutex);
				hash_unlock(&secondary_hash, request->key);
				response->status = SERVER_FAILURE;
				break;
			}
			pthread_mutex_unlock(&primary_fd_mutex);
			hash_unlock(&secondary_hash, request->key); // unlock bucket, as value has been inserted and propogated.
			if (replication_response.status != SUCCESS)
				response->status = SERVER_FAILURE;
		}
		else
		{
			// Invalid operation
			response->status = INVALID_REQUEST;
			break;
		}

		// Need to free the old value (if there was any)
		if (old_value != NULL)
			free(old_value);
		response->status = SUCCESS;
		break;
	}

	case OP_VERIFY:
	{
		// Checked for invalid OP_VERIFY earlier. Now just check
		// if we are primary or secondary for this key.
		void *data = NULL;
		size_t size = 0;
		if (key_srv_id == server_id)
		{
			// Handle just like a GET request
			// Get the value for key from the primary hash table
			if (!hash_get(&primary_hash, request->key, &data, &size))
				response->status = KEY_NOT_FOUND;
			else
			{
				// Copy the value into the response buffer
				memcpy(response->value, data, size);
				value_sz = size;
				response->status = SUCCESS;
			}
		}
		else if (key_srv_id == primary_sid)
		{
			// Get the value for key from the secondary hash table
			if (!hash_get(&secondary_hash, request->key, &data, &size))
				response->status = KEY_NOT_FOUND;
			else
			{
				// Copy the value into the response buffer
				memcpy(response->value, data, size);
				value_sz = size;
				response->status = SUCCESS;
			}
		}
		else
		{
			// Should not be possible if logic for checking
			// invalid request prior to switch is correct.
			assert(false);
		}
		break;
	}
	default:
		return;
	}

	// Send reply to the client
	send_msg(fd, response, sizeof(*response) + value_sz);
}

// Hash iterator for sending key-value; called during storage cleanup
static void recover_iterator_f(const char key[KEY_SIZE], void *value,
							   size_t value_sz, void *arg, int fd_to_use)
{
	// note that it has been sychronized
	char req_buffer[MAX_MSG_LEN] = {0};
	char resp_buffer[MAX_MSG_LEN] = {0};
	operation_request *request = (operation_request *)req_buffer;
	operation_response *response = (operation_response *)resp_buffer;
	request->hdr.type = MSG_OPERATION_REQ;
	request->type = OP_PUT;

	assert(value != NULL);
	memcpy(request->key, key, KEY_SIZE);
	memcpy(request->value, value, value_sz);

	pthread_mutex_t *fd_mutex_ptr =
		primary_fd == fd_to_use ? &primary_fd_mutex : &secondary_fd_mutex;
	pthread_mutex_lock(fd_mutex_ptr);
	if (!send_msg(fd_to_use, request, sizeof(*request) + value_sz) ||
		!recv_msg(fd_to_use, resp_buffer, MAX_MSG_LEN, MSG_OPERATION_RESP))
	{
		response->status = SERVER_FAILURE;
		*(bool *)arg = false;
	}
	pthread_mutex_unlock(fd_mutex_ptr);
}

static void recover_primary_iterator_f(const char key[KEY_SIZE], void *value,
									   size_t value_sz, void *arg)
{
	return recover_iterator_f(key, value, value_sz, arg, primary_fd);
}

static void recover_secondary_iterator_f(const char key[KEY_SIZE], void *value,
										 size_t value_sz, void *arg)
{
	return recover_iterator_f(key, value, value_sz, arg, secondary_fd);
}

void *recover_fd(void *arg)
{
	// extract mode
	bool update_primary = *(bool *)arg;
	hash_table *storage_to_use;
	static void *iterator_to_use;

	if (update_primary)
	{
		// we are updating primary
		storage_to_use = &secondary_hash;
		iterator_to_use = recover_primary_iterator_f;
	}
	else
	{
		// updating secondary
		storage_to_use = &primary_hash;
		iterator_to_use = recover_secondary_iterator_f;
	}

	bool status = true;
	hash_iterate(storage_to_use, iterator_to_use, &status);

	// Recovery Step 8/10
	char send_buffer[MAX_MSG_LEN] = {0};
	coord_ctrl_request *req = (coord_ctrl_request *)send_buffer;
	req->hdr.type = MSG_COORD_CTRL_REQ;
	if (update_primary)
		req->type = status ? UPDATED_PRIMARY : UPDATE_PRIMARY_FAILED;
	else
		req->type = status ? UPDATED_SECONDARY : UPDATE_SECONDARY_FAILED;
	req->server_id = server_id;
	pthread_mutex_lock(&coord_fd_mutex);
	if (!send_msg(coord_fd_out, req, sizeof(*req)))
	{
		log_error("Send confirmation to coordinator failed\n");
		assert(false);
	}
	pthread_mutex_unlock(&coord_fd_mutex);
	return NULL;
}

// Returns false if either the message was invalid or if this was the last message
// (in both cases the connection will be closed)
static bool process_server_message(int fd)
{
	char timebuf[TIME_STR_SIZE];

	log_write("%s Receiving a server message\n", current_time_str(timebuf, TIME_STR_SIZE));

	// Read and parse the message
	char req_buffer[MAX_MSG_LEN] = {0};
	if (!recv_msg(fd, req_buffer, MAX_MSG_LEN, MSG_OPERATION_REQ))
		return false;
	operation_request *request = (operation_request *)req_buffer;

	// NOOP operation request is used to indicate the last message in an UPDATE sequence
	if (request->type == OP_NOOP)
		return false;
	// Initialize the response
	char resp_buffer[MAX_MSG_LEN] = {0};
	operation_response *response = (operation_response *)resp_buffer;
	response->hdr.type = MSG_OPERATION_RESP;

	assert(request->type == OP_PUT); // currently, only supports receiving PUTs for replication / restoration.
	// Need to copy the value to dynamically allocated memory
	size_t value_size = request->hdr.length - sizeof(*request);
	void *value_copy = malloc(value_size);

	int key_srv_id = key_server_id(request->key, num_servers);
	hash_table *table = NULL;

	if (key_srv_id == primary_sid) // replication of kv pair
		table = &secondary_hash;
	else if (key_srv_id == server_id) // original kv pair - means we are restoring the server
		table = &primary_hash;

	assert(table != NULL); // key neither belongs as primary or secondary, we have a problem

	if (value_copy == NULL)
	{
		log_perror("malloc");
		log_error("sid %d: Out of memory\n", server_id);
		response->status = OUT_OF_SPACE;
		send_msg(fd, response, sizeof(*response));
		return true;
	}
	memcpy(value_copy, request->value, value_size);

	void *old_value = NULL;
	size_t old_value_sz = 0;
	hash_lock(table, request->key);
	// Put the <key, value> pair into the appropriate hash table
	if (!hash_put(table, request->key, value_copy, value_size, &old_value, &old_value_sz))
	{
		hash_unlock(table, request->key);
		log_error("sid %d: Out of memory\n", server_id);
		free(value_copy);
		response->status = OUT_OF_SPACE;
		send_msg(fd, response, sizeof(*response));
		return true;
	}
	hash_unlock(table, request->key);

	// Need to free the old value (if there was any)
	if (old_value != NULL)
		free(old_value);

	response->status = SUCCESS;
	send_msg(fd, response, sizeof(*response));
	return true;
}

// Hash iterator for dumping keys to a file. arg is file descriptor.
static void dump_iterator(const char key[KEY_SIZE], void *value, size_t value_sz, void *arg)
{
	(void)value;
	(void)value_sz;

	int fd = *((int *)arg);
	write(fd, key, KEY_SIZE);
}

// Returns false if the message was invalid (so the connection will be closed)
// Sets *shutdown_requested to true if received a SHUTDOWN message (so the server will terminate)
static bool process_coordinator_message(int fd, bool *shutdown_requested)
{
	char timebuf[TIME_STR_SIZE];

	assert(shutdown_requested != NULL);
	*shutdown_requested = false;

	log_write("%s Receiving a coordinator message\n",
			  current_time_str(timebuf, TIME_STR_SIZE));

	// Read and parse the message
	char req_buffer[MAX_MSG_LEN] = {0};
	if (!recv_msg(fd, req_buffer, MAX_MSG_LEN, MSG_SERVER_CTRL_REQ))
		return false;
	server_ctrl_request *request = (server_ctrl_request *)req_buffer;

	// Initialize the response
	server_ctrl_response response = {0};
	response.hdr.type = MSG_SERVER_CTRL_RESP;
	response.status = SERVER_CTRLREQ_STATUS_MAX; // Detect unset status

	char outputfile_primary[64];
	char outputfile_secondary[64];
	// Process the request based on its type
	switch (request->type)
	{
	case SET_SECONDARY:
		if ((secondary_fd = connect_to_server(request->host_name,
											  request->port)) < 0)
			response.status = CTRLREQ_FAILURE;
		response.status = CTRLREQ_SUCCESS;
		break;

	case UPDATE_PRIMARY:
		// Recovery Step 3
		// Note: assumed connection drop when server failed
		if ((primary_fd = connect_to_server(request->host_name,
											request->port)) < 0)
		{
			response.status = CTRLREQ_FAILURE;
			break;
		}
		recovery_process_status = ONGOING;
		// primary_fd should be recovery server
		// Set up backend threads for restoration
		bool update_primary = true;
		pthread_t thread_id;
		if (pthread_create(&thread_id, NULL, recover_fd, &update_primary) != 0)
		{
			log_error("Create recovery thread failed\n");
			response.status = CTRLREQ_FAILURE;
			break;
		}
		response.status = CTRLREQ_SUCCESS;
		// Respond to CO to indicate that it received the UPDATE-PRIMARY message.(end of function)
		break;

	case UPDATE_SECONDARY:
		// Recovery Step 6
		recovery_process_status = ONGOING;
		close_safe(&(secondary_fd));
		// connect to new server
		if ((secondary_fd = connect_to_server(request->host_name, request->port)) < 0)
		{
			response.status = CTRLREQ_FAILURE;
			break;
		}
		update_primary = false;
		// connection set up, setup backend threads for recovering async
		if (pthread_create(&thread_id, NULL, recover_fd, &update_primary) != 0)
		{
			log_error("Create recovery thread failed\n");
			response.status = CTRLREQ_FAILURE;
			break;
		}
		response.status = CTRLREQ_SUCCESS;
		// respond to coor to indicate it received the UPDATE_SECONDARY message, at end of func
		break;

	case SWITCH_PRIMARY:
		// Step 13: Client requests in progress will be checked in the server loop
		recovery_process_status = SWITCH_PRIMARY_RECEIVED;
		response.status = CTRLREQ_SUCCESS;
		break;

	case SHUTDOWN:
		*shutdown_requested = true;
		return true;

	case DUMP_PRIMARY:
		// Write primary keys from hash table to file "server_<sid>.primary"
		sprintf(outputfile_primary, "server_%d.primary", server_id);
		int fd_primary = open(outputfile_primary, O_WRONLY | O_CREAT, 0644);
		hash_iterate(&primary_hash, dump_iterator, &fd_primary);
		close(fd_primary);
		// No response is expected
		return true;

	case DUMP_SECONDARY:
		// Write secondary keys from hash table to file "server_<sid>.secondary"
		// No response is expected, so after dumping keys, just return.
		sprintf(outputfile_secondary, "server_%d.secondary", server_id);
		int fd_secondary = open(outputfile_secondary, O_WRONLY | O_CREAT, 0644);
		hash_iterate(&secondary_hash, dump_iterator, &fd_secondary);
		close(fd_secondary);
		// No response is expected
		return true;

	default: // impossible
		assert(false);
		break;
	}

	assert(response.status != SERVER_CTRLREQ_STATUS_MAX);
	if (!send_msg(fd, &response, sizeof(response)))
	{
		log_error("Send response to coordinator failed\n");
		return false;
	}
	return true;
}

void *t_handle_server_message()
{
	// Usual preparation stuff for select()
	fd_set rset, allset;
	FD_ZERO(&allset);
	FD_SET(my_servers_fd, &allset);

	int maxfd = my_servers_fd;
	// NOTE: server incoming connection / message check may need to be updated with primary_fd once we support recovery. Presumably, we will keep the fd table, but only check on primary_fd.
	for (;;)
	{
		rset = allset;
		int num_ready_fds = select(maxfd + 1, &rset, NULL, NULL, NULL);

		// Incoming connection from server
		if (FD_ISSET(my_servers_fd, &rset))
		{
			int fd_idx = accept_connection(my_servers_fd, server_fd_table, MAX_SERVER_SESSIONS);
			if (fd_idx >= 0)
			{
				FD_SET(server_fd_table[fd_idx], &allset);
				maxfd = max(maxfd, server_fd_table[fd_idx]);
			}

			if (--num_ready_fds <= 0)
				continue;
		}

		// Check for any messages from server
		for (int i = 0; i < MAX_SERVER_SESSIONS; i++)
		{
			if ((server_fd_table[i] != -1) && FD_ISSET(server_fd_table[i], &rset))
			{
				if (!process_server_message(server_fd_table[i]))
				{
					// Received an invalid message / final message, close the connection
					FD_CLR(server_fd_table[i], &allset);
					close_safe(&(server_fd_table[i]));
				}

				if (--num_ready_fds <= 0)
					break;
			}
		}
	}
}

// Returns false if stopped due to errors, true if shutdown was requested
static bool run_server_loop()
{
	// set up thread to handle server messages seperately
	pthread_t thread_id;
	if (pthread_create(&thread_id, NULL, t_handle_server_message, NULL) != 0)
	{
		log_perror("pthread_create for server messages");
		return false;
	}
	// Usual preparation stuff for select()
	fd_set rset, allset;
	FD_ZERO(&allset);
	FD_SET(my_clients_fd, &allset);
	FD_SET(my_coord_fd, &allset);

	int maxfd = max(my_clients_fd, my_coord_fd);

	// Server sits in an infinite loop waiting for incoming connections
	// from coordinator or clients, and for incoming messages from already
	// connected coordinator or clients
	for (;;)
	{
		rset = allset;

		int num_ready_fds = select(maxfd + 1, &rset, NULL, NULL, NULL);
		if (num_ready_fds < 0)
		{
			log_perror("select");
			return false;
		}

		if (num_ready_fds <= 0)
			continue;

		// Incoming connection from the coordinator
		if (FD_ISSET(my_coord_fd, &rset))
		{
			int fd_idx = accept_connection(my_coord_fd, &coord_fd_in, 1);
			if (fd_idx >= 0)
			{
				FD_SET(coord_fd_in, &allset);
				maxfd = max(maxfd, coord_fd_in);
			}
			assert(fd_idx == 0);

			if (--num_ready_fds <= 0)
				continue;
		}

		// Check for any messages from the coordinator
		if ((coord_fd_in != -1) && FD_ISSET(coord_fd_in, &rset))
		{
			bool shutdown_requested = false;
			if (!process_coordinator_message(coord_fd_in, &shutdown_requested))
			{
				// Received an invalid message, close the connection
				log_error("sid %d: Closing coordinator connection\n", server_id);
				FD_CLR(coord_fd_in, &allset);
				close_safe(&(coord_fd_in));
				// Since coordinator failed, unable to recover
				return false;
			}
			else if (shutdown_requested)
				return true;

			if (--num_ready_fds <= 0)
				continue;
		}

		// Incoming connection from a client
		if (FD_ISSET(my_clients_fd, &rset))
		{
			int fd_idx = accept_connection(my_clients_fd, client_fd_table, MAX_CLIENT_SESSIONS);
			if (fd_idx >= 0)
			{
				FD_SET(client_fd_table[fd_idx], &allset);
				maxfd = max(maxfd, client_fd_table[fd_idx]);
			}

			if (--num_ready_fds <= 0)
				continue;
		}

		// Check for any messages from connected clients
		for (int i = 0; i < MAX_CLIENT_SESSIONS; i++)
		{
			if ((client_fd_table[i] != -1) && FD_ISSET(client_fd_table[i], &rset))
			{
				process_client_message(client_fd_table[i]);
				// Close connection after processing (semantics are "one connection per request")
				FD_CLR(client_fd_table[i], &allset);
				close_safe(&(client_fd_table[i]));

				if (--num_ready_fds <= 0)
					break;
			}
		}

		// All in progress messages are done, recovery officially finished at server side
		if (recovery_process_status == SWITCH_PRIMARY_RECEIVED)
			recovery_process_status = INACTIVE;
	}
}

int main(int argc, char **argv)
{
	signal(SIGPIPE, SIG_IGN);

	if (!parse_args(argc, argv))
	{
		usage(argv);
		return 1;
	}

	open_log(log_file_name);

	if (!init_server())
	{
		return 1;
	}

	bool result = run_server_loop();

	cleanup();
	close_log();

	return result ? 0 : 1;
}
