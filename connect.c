#define _GNU_SOURCE

#include <assert.h>
#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>

#include "util.h"

// Connect to a TCP server given its host name and port number.
// Returns a connected socket fd.
int connect_to_server(const char *host_name, uint16_t port)
{
	struct addrinfo hints, *addrs, *p;
	int sockfd = -1;
	char port_str[10];
	int err;

	assert(host_name != NULL);

	// Resolve the host name
	memset(&hints, 0, sizeof(hints));
	hints.ai_family = AF_UNSPEC;     // either IPv4 or IPv6
	hints.ai_socktype = SOCK_STREAM; // TCP stream sockets
	hints.ai_protocol = IPPROTO_TCP;
	snprintf(port_str, 10, "%hu", port);
	if ((err = getaddrinfo(host_name, port_str, &hints, &addrs)) != 0) {
		log_gai_error("getaddrinfo", err);
		return -1;
	}

	// Try to create a socket and connect on the returned addrs
	for(p = addrs; p != NULL; p = p->ai_next) {	
		// Create socket fd
		sockfd = socket(p->ai_family, p->ai_socktype, p->ai_protocol);
		if (sockfd < 0) {
			log_perror("socket");
			continue;
		}

		// Connect to the server
		if (connect(sockfd, p->ai_addr, p->ai_addrlen) < 0) {
			log_perror("connect");
			close(sockfd);
			sockfd = -1;
			continue;
		}
		break;
	}
	
	freeaddrinfo(addrs);
	
	return sockfd;
}
