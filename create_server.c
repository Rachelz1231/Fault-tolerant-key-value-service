#define _GNU_SOURCE

#include <assert.h>
#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stddef.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netdb.h>
#include "util.h"

// Start a TCP server on a given port; returns listening socket fd
// If port == 0, bind to an arbitrary port assigned by the OS;
//               new_port must != NULL in this case
// If port != 0 and bind fails, then an arbitray port is chosen
//              (and returned via *new_port) if new_port != NULL
int create_server(uint16_t port, int max_sessions, uint16_t *new_port)
{
	char host_name[HOST_NAME_MAX];
	struct addrinfo hints, *addrs, *p;
	int sockfd = -1;
	char port_str[10];
	int err;
	bool need_to_get_port = ((port == 0) && (new_port != NULL));
	
	// Obtain address information using the host name and port
	// (See get_local_host_name() in util.c to get the host name.)

	if (get_local_host_name(host_name, HOST_NAME_MAX) < 0) {
		return -1;
	}
	
	memset(&hints, 0 , sizeof(hints));
	hints.ai_family = AF_UNSPEC;     // either IPv4 or IPv6
	hints.ai_socktype = SOCK_STREAM; // TCP stream sockets
	hints.ai_protocol = IPPROTO_TCP;
	snprintf(port_str, 10, "%hu", port);

	if ((err = getaddrinfo(host_name, port_str, &hints, &addrs)) != 0) {
		log_gai_error("getaddrinfo", err);	
		return -1;
	}

	// Create a socket, set the SO_REUSEADDR socket option and bind it..
	// Remember that getaddrinfo returns a list of addresses, and you
	// need to try them in order until you find one that works.
	// Remember also to check if bind() needs to choose the port, and if
	// so, set "new_port" so that it is returned to the caller.

	for(p = addrs; p != NULL; p = p->ai_next) {	
		// Create socket fd
		sockfd = socket(p->ai_family, p->ai_socktype, p->ai_protocol);
		if (sockfd < 0) {
			perror("socket");
			continue;
		}

		// Set the SO_REUSEADDR socket option
		int opt_val = 1;
		if (setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, (void*)&opt_val, sizeof(opt_val)) < 0) {
			log_perror("setsockopt");
			close(sockfd);
			continue;
		}
		// Set the SO_REUSEADDR socket option as well
		opt_val = 1;
		if (setsockopt(sockfd, SOL_SOCKET, SO_REUSEPORT, (void*)&opt_val, sizeof(opt_val)) < 0) {
			log_perror("setsockopt 2");
			close(sockfd);
			continue;
		}

		// Try to bind to the given port
		if ((err = bind(sockfd, p->ai_addr, p->ai_addrlen)) < 0 ) {
			// Error
			log_perror("bind");
			// If bind failed because the address (port) was in use
			// then try letting bind pick the port, but only if
			//   (a) we didn't already try that (port != 0) and
			//   (b) caller gave us a place to return the port #.
			if ((errno == EADDRINUSE || errno == EACCES) &&
			    (port != 0) && (new_port != NULL)) {
				// Need to dig into addr and set port to 0. That
				// depends on whether family is IPv4 or IPv6.
				if (p->ai_family == AF_INET) {
					struct sockaddr_in *sa;
					sa = (struct sockaddr_in *)p->ai_addr;
					sa->sin_port = 0;
				} else {
					struct sockaddr_in6 *sa;
					sa = (struct sockaddr_in6 *)p->ai_addr;
					sa->sin6_port = 0;
				}
				err = bind(sockfd, p->ai_addr, p->ai_addrlen);
				if (err < 0) {
					log_perror("bind second try");
					close(sockfd);
					continue;
				}
				// Success! But need to get port number assigned
				need_to_get_port = true;
				break;
			} else {
				close(sockfd);
				continue;
			}
		} else {
			// bind() successful!
			break;
		}
	}

	if (p == NULL) { // No success.
		fprintf(stderr,"Could not bind to any address.\n");
		return -1;
	}

	// If p is not NULL, then it points to the addr used for socket & bind.
	
	// If bind() assigned a port, find out what it was
	if (need_to_get_port) {
		assert(new_port != NULL);

		if (getsockname(sockfd, p->ai_addr, &p->ai_addrlen) < 0) {
			log_perror("getsockname");
			close(sockfd);
			return -1;
		}
		if (p->ai_family == AF_INET) {
			*new_port = port = ntohs(((struct sockaddr_in *)p->ai_addr)->sin_port);
		} else { // AF_INET6
			*new_port = port = ntohs(((struct sockaddr_in6 *)p->ai_addr)->sin6_port);
		}
	}

	// Start listening for incoming connections
	if ((err = listen(sockfd, max_sessions)) < 0) {
		log_perror("listen");
		close(sockfd);
		return -1;
	}

	freeaddrinfo(addrs);
	
	log_write("Listening on TCP port %hu\n", port);	
	return sockfd;
}
