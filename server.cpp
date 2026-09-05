#include <iostream>
#include <netdb.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <string>
#include <cerrno>
#include <fcntl.h>
#include <pthread.h>
#include <vector>
#include <queue>
#include <unistd.h>
#include <memory>
#include <mutex>
#include <utility>
#include <string.h>
#include <sys/event.h>
#include <sys/types.h>
#include <sys/time.h>



#define MAXEVENTS 20
#define MAXIDLEN 10
#define MAXDATALEN 5
#define MAXCLIENTNUM 200
#define PORT "3490"
#define BACKLOG 10
#define MAXBUFFERSIZE 100
#define MAXPACKETSIZE (2+2+MAXIDLEN+2+MAXPACKETLEN)
#define HEIGHT_LIMIT 900
#define WIDTH_LIMIT 900

/* PACKET STRUCTURE


2byte: total packet size , 2byte : size of id , MAXIDLEN byte, 8 byte : location , xloc,yloc


*/



/*
----------------------TBD-------------------------


multi-threading on mac OS vscode
using kqueue

struct taskstruct
{
    int sender_fd;
    std::string packet;
    int len;
    int* fd_list;
    int* fd_count;
};


struct consumeStruct
{
    int tid;
    int* packet_num;
    std::queue<std::unique_ptr<struct tasksturct>> t_queue;
};


packet structure: 2byte 2byte 2byte 10byte 2byte 2byte -> 20byte
*/
#pragma pack(push,1)
struct MovePacket
{
	uint16_t size;
	uint16_t type;
	uint16_t idlen;
	char id[MAXIDLEN];
	uint16_t x;
	uint16_t y;
};
#pragma pack(pop)

struct Player
{
	char user_id[MAXIDLEN+1];
	int x;
	int y;
};

int add_to_fdlist(int fd,std::vector<int> fdlist, int* fd_count)
{
    int size = fdlist.capacity();
    if(*fd_count == size)
    {
        if(size == MAXCLIENTNUM)
            return -1;
        fdlist.reserve(size * 2);
        fprintf(stdout, "Successfully expand user list\n");
    }
    fdlist[*fd_count] = fd;
    (*fd_count)++;
	return 0;
}

void delete_from_fdlist(std::vector<int> fdlist, int idx,int* fd_count)
{
    std::swap(fdlist[idx],fdlist[(*fd_count) - 1]);
	fdlist.pop_back();
	(*fd_count)--;
	if((*fd_count) == 0)
		return;
	std::swap(fdlist[idx] , fdlist[(*fd_count) - 1]);
}

int sendall(int s,char* packet,int len)
{
	int total = 0;
	int bytesleft = len;
	int n;
	while(total < len)
	{
		n = send(s,packet+total,bytesleft,0);
		if(n <= 0)
		{
			if(n == 0)
				return 0;
			else
			{
				if(errno == EAGAIN || errno == EWOULDBLOCK)
					continue;
				else
				{
					n = -1;
					break;
				}
			}
		}
		total += n;
		bytesleft -= n;
	}
	return n == -1 ? -1 : total;
}

int recvall(int s,char* buffer,int len)
{
	int total = 0;
	int bytesleft = len;
	int n;
	while(total < len)
	{
		n = recv(s,buffer+total,bytesleft,0);
		if(n <= 0)
		{
			if(n == 0)
				return 0;
			else
			{
				if(errno == EAGAIN || errno == EWOULDBLOCK)
				{
					usleep(1000);
					continue;
				}
				else
				{
					n = -1;
					break;
				}
			}
		}
		total += n;
		bytesleft -= n;
	}
	return n==-1 ? -1 : total;
}

void* get_in_addr(struct sockaddr* sa)
{
    if(sa->sa_family == AF_INET)
        return &(((struct sockaddr_in*)sa)->sin_addr);
    return &(((struct sockaddr_in6*)sa)->sin6_addr);
}


int main(int argc,char** argv)
{
    if(argc != 2)
    {
        std::cerr << "Usage: showip hostname " << "\n";
        return 1;
    }
	int yes = 1;
    int status;
	int listener_fd;
	int fd_count = 0;
	uint32_t uniquePlayerId = 1;
    char ipv6str[INET6_ADDRSTRLEN];
    char ipv4str[INET_ADDRSTRLEN];
	char s[INET6_ADDRSTRLEN];
	std::vector<int> fdlist(5);

    struct addrinfo hints, *servinfo, *p;
	struct sockaddr_storage their_addr;
	socklen_t sin_size;

	memset(&hints,0,sizeof(hints));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
	hints.ai_flags = AI_PASSIVE;

    if((status = getaddrinfo(NULL,PORT,&hints,&servinfo))==-1)
	{
		std::cerr << "getaddrinfo error: " << gai_strerror(status) << "\n";
	}
	for(p = servinfo; p != NULL ; p = p->ai_next)
	{
		if((listener_fd = socket(p->ai_family,p->ai_socktype,p->ai_protocol)) == -1)
		{
			std::cerr << "recv" << "\n";
			continue;
		}
		if(setsockopt(listener_fd , SOL_SOCKET, SO_REUSEADDR ,&yes , sizeof(int))==-1)
		{
			std::cerr << "setsockopt" << "\n";
			continue;
		}
		if(bind(listener_fd,p->ai_addr,p->ai_addrlen)==-1)
		{
			std::cerr << "bind" << "\n";
			continue;
		}
		break;
	}
	if(p == NULL)
	{
		std::cerr << "server: fail to bind." << "\n";
		exit(1);
	}
	if(listen(listener_fd,BACKLOG)==-1)
	{
		std::cerr << "listen" << "\n";
		exit(1);
	}
	fcntl(listener_fd, F_SETFL, O_NONBLOCK);
	std::cout << "server: waiting for connections..\n";
	if(add_to_fdlist(listener_fd,fdlist,&fd_count) == -1)
	{
		std::cerr << "fail to get fdlist.\n";
		exit(1);
	}
	freeaddrinfo(servinfo);
	int kq = kqueue();
	if(kq == -1)
	{
		std::cerr << "kqueue()";
		exit(1);
	}
	struct kevent ChangeList;
	struct kevent EventList[MAXEVENTS];
	EV_SET(&ChangeList,listener_fd,EVFILT_READ, EV_ADD | EV_CLEAR , 0 ,0 ,NULL);
	if(kevent(kq,&ChangeList,1,NULL,0,NULL)==-1)
	{
		std::cerr << "kevent register failed.\n";
		exit(1);
	}

	std::cout << "echo server start.\n";
	
	while(1)
	{
		int nev = kevent(kq,NULL,0,EventList,MAXEVENTS,NULL);
		if(nev == -1)
		{
			if(errno == EINTR)
				continue;
			std::cerr << "kqueue_wait\n";
			break;
		}
		for(int i = 0; i < nev ; i++)
		{
			int curr_fd = static_cast<int>(EventList[i].ident);
			if(curr_fd == listener_fd)
			{
				struct sockaddr_storage clientAddr;
				socklen_t clientAddrLen = sizeof (clientAddr);
				int newfd = accept(listener_fd,(struct sockaddr*)&clientAddr,&clientAddrLen);
				if(newfd == -1)
				{
					if(errno == EAGAIN || errno == EWOULDBLOCK)
						break;
					else
					{
						std::cerr << "accept";
						break;
					}
				}
				fcntl(newfd,F_SETFL,O_NONBLOCK);
				add_to_fdlist(newfd,fdlist,&fd_count);
				EV_SET(&ChangeList,newfd,EVFILT_READ,EV_ADD | EV_CLEAR , 0,0,NULL);
				if(kevent(kq,&ChangeList,1,NULL,0,NULL) == -1)
				{
					std::cerr << "client kqueue fail.\n";
					close(newfd);
				}
				

			}
			else
			{
				int sender_fd = static_cast<int>(EventList[i].ident);
				char buffer[20];
				int offset = 0;
				int header_res = recvall(sender_fd,buffer,2);
				offset+=2;
				if(header_res < 0)
				{
					std::cerr << "Connection Loss from " << i << "\n";
					close(sender_fd);
					delete_from_fdlist(fdlist,i,&fd_count);
					break;
				}
				if(header_res == 0)
				{
					std::cerr << "Connection closed from " << i << "\n";
					close(sender_fd);
					delete_from_fdlist(fdlist,i,&fd_count);
					break;
				}
				uint16_t size;
				memcpy(&size,buffer,2);
				int bodysize = ntohs(size);
				std::cout << "packet size: " << bodysize << "\n";
				int body_res = recvall(sender_fd,buffer+offset, bodysize-2);
				if(body_res <= 0)
				{
					std::cerr << "Connection closed from " << i << "\n";
					close(sender_fd);
					delete_from_fdlist(fdlist,i,&fd_count);
					break;	
				}
				uint16_t type;
				memcpy(&type,buffer+offset,2);
				offset+=2;
				int ptype = ntohs(type);
				uint16_t idlen;
				memcpy(&idlen,buffer+offset,2);
				offset+=2;
				int pidlen = ntohs(idlen);
				char id[MAXIDLEN+1];
				memset(&id,0,sizeof(id));
				memcpy(&id,buffer+offset,pidlen);
				offset+=10;
				id[pidlen] = '\0';
				uint16_t x;
				memcpy(&x,buffer+offset,2);
				offset+=2;
				uint16_t y;
				memcpy(&y,buffer+offset,2);
				offset+=2;
				int xlocation = ntohs(x);
				int ylocation = ntohs(y);
				std::cout << "id: " << id << "\n";
				std::cout << "Location: " << xlocation << "," << ylocation << "\n";
			}

		}

	}
}