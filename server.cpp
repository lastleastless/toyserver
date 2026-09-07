#include <iostream>
#include <netdb.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <string>
#include <cerrno>
#include <fcntl.h>
#include <vector>
#include <queue>
#include <unistd.h>
#include <memory>
#include <condition_variable>
#include <mutex>
#include <utility>
#include <string.h>
#include <sys/event.h>
#include <sys/types.h>
#include <sys/time.h>
#include <thread>
#include <unordered_map>


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

std::mutex m;
std::mutex list_m;
std::condition_variable fill;
std::condition_variable empty;
// defrecated bool isempty = false;
// defrecated bool isfill = false;

/* PACKET STRUCTURE

Header packet: 2byte Size , 2byte type
Move pakcet: header packet + 2byte : size of id , MAXIDLEN byte, 8 byte : location , xloc,yloc
Atk packet: header packet + 2byte: size of id, MAXIDLEN byte, (TBD)

*/



/*
----------------------TBD-------------------------


multi-threading on mac OS vscode
using kqueue
*/

/*  이런식으로 패킷을 하나하나 정의하는 건 각 패킷간의 결합도가 굉장히 높아짐
	팩토리 역할을 할 헤더패킷 인터페이스를 정의하고, 그 하위부들을 따로 구현해줄
	하위 클래스들을 구성해주는 방식인 팩토리 매서드가 적합해보임. */

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

struct taskPacket
{
	struct MovePacket pkt;
	int tid;
};
#pragma pack(pop)
struct ClientSession
{
	std::vector<char> buffer;
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
	if((*fd_count) == 1)
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
// Deprecated
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
void* consumer(std::queue<std::unique_ptr<struct taskPacket>>& taskQueue,std::vector<int>& fdlist)
{
	while(1)
	{
		std::unique_lock<std::mutex>lock(m);
		while(taskQueue.size() == 0)
			fill.wait(lock,[&]{return !taskQueue.empty();});
		std::cout << "worker get Packet!\n";
		std::unique_ptr <struct taskPacket> cpkt = std::move(taskQueue.front());
		taskQueue.pop();
		lock.unlock();
		empty.notify_one();
		std::unique_lock<std::mutex>list_lock(list_m);
		int fd = cpkt->tid;
		for(const auto& i : fdlist)
		{
			if(i != fd)
			{
				if(sendall(i,reinterpret_cast<char*>(&(cpkt->pkt)),20)==-1)
				{
					continue;
				}
			}
		}
		list_lock.unlock();
		std::cout << "work done, current queue size:" << taskQueue.size() << "\n";
	}
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
	int taskCount = 0;

	char s[INET6_ADDRSTRLEN];
	std::vector<int> fdlist(5);
    struct addrinfo hints, *servinfo, *p;
	struct sockaddr_storage their_addr;
	socklen_t sin_size;
	std::unordered_map<int,struct ClientSession> clients;
	memset(&hints,0,sizeof(hints));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
	hints.ai_flags = AI_PASSIVE;
	std::queue<std::unique_ptr<struct taskPacket>> consumeQueue;
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
	std::thread t(consumer,std::ref(consumeQueue),std::ref(fdlist));
	std::thread t2(consumer,std::ref(consumeQueue),std::ref(fdlist));

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
				std::unique_lock<std::mutex> list_lock(list_m);
				add_to_fdlist(newfd,fdlist,&fd_count);
				list_lock.unlock();
				EV_SET(&ChangeList,newfd,EVFILT_READ,EV_ADD | EV_CLEAR , 0,0,NULL);
				if(kevent(kq,&ChangeList,1,NULL,0,NULL) == -1)
				{
					std::cerr << "client kqueue fail.\n";
					close(newfd);
				}
				clients[newfd] = ClientSession{};
			}
			else
			{
				// header recv area
				int sender_fd = static_cast<int>(EventList[i].ident);
				char buffer[MAXBUFFERSIZE];
				int offset = 0;
				int n = recv(sender_fd,buffer,sizeof(buffer),0);
				if(n < 0)
				{
					std::cerr << "Connection Loss from " << i << "\n";
					close(sender_fd);
					std::unique_lock<std::mutex> list_lock(list_m);
					delete_from_fdlist(fdlist,sender_fd,&fd_count);
					clients.erase(sender_fd);
					list_lock.unlock();
					break;
				}
				if(n == 0)
				{
					std::cerr << "Connection closed from " << i << "\n";
					close(sender_fd);
					std::unique_lock<std::mutex> list_lock(list_m);
					delete_from_fdlist(fdlist,sender_fd,&fd_count);
					clients.erase(sender_fd);
					list_lock.unlock();
					break;
				}
				//header recv end
				auto& clientbuffer = clients[sender_fd].buffer;
				clientbuffer.insert(clientbuffer.end(), buffer, buffer+n);
				uint16_t curpktSize = 0;
				uint16_t curpktType = 0;
				if(clientbuffer.size() < 4)
					continue;
				memcpy(&curpktSize,clientbuffer.data(),2);
				memcpy(&curpktType,clientbuffer.data() + 2,2);
				curpktSize = ntohs(curpktSize);
				curpktType = ntohs(curpktType);
				std::cout << "Cur packet info: " << curpktSize << "," << curpktType << "\n";
				if(clientbuffer.size() < curpktSize)
					continue;
				// pktsize == 40, type == 2 -> ATK packet (TBD)
				// pktsize == 20, type == 1  -> Move packet 
				if(curpktSize != 20 && curpktSize != 40) // Enum class refactoring needed.
				{
					std::cerr<< "Invalid Packet Length from " << sender_fd;
					close(sender_fd);
					std::unique_lock<std::mutex> list_lock(list_m);
					delete_from_fdlist(fdlist,sender_fd,&fd_count);
					clients.erase(sender_fd);
					list_lock.unlock();
					break;
				}
				else
				{
					if(curpktSize == 20)
					{
						if(curpktType != 1)
						{
							std::cerr << "Malformed packet received from " << sender_fd;
							close(sender_fd);
							std::unique_lock<std::mutex> list_lock(list_m);
							delete_from_fdlist(fdlist,sender_fd,&fd_count);
							clients.erase(sender_fd);
							list_lock.unlock();
							break;
						}
					}
					if(curpktSize == 40)
					{
						if(curpktType != 2)
						{
							std::cerr << "Malformed packet received from " << sender_fd;
							close(sender_fd);
							std::unique_lock<std::mutex> list_lock(list_m);
							delete_from_fdlist(fdlist,sender_fd,&fd_count);
							clients.erase(sender_fd);
							list_lock.unlock();
							break;
						}
					}
				}

				while(clientbuffer.size() >= curpktSize)
				{
					char packetbuffer[curpktSize];
					memcpy(packetbuffer,clientbuffer.data(),curpktSize);
					clientbuffer.erase(clientbuffer.begin(),clientbuffer.begin()+curpktSize);
					// deserialization area
					uint16_t size;
					memcpy(&size,packetbuffer,2);
					offset+=2;
					int bodysize = ntohs(size);
					std::cout << "packet size: " << bodysize << "\n";
					uint16_t type;
					memcpy(&type,packetbuffer+offset,2);
					offset+=2;
					int ptype = ntohs(type);
					uint16_t idlen;
					memcpy(&idlen,packetbuffer+offset,2);
					offset+=2;
					int pidlen = ntohs(idlen);
					char id[MAXIDLEN+1];
					memset(&id,0,sizeof(id));
					memcpy(&id,packetbuffer+offset,pidlen);
					offset+=10;
					id[pidlen] = '\0';
					uint16_t x;
					memcpy(&x,packetbuffer+offset,2);
					offset+=2;
					uint16_t y;
					memcpy(&y,packetbuffer+offset,2);
					offset+=2;
					int xlocation = ntohs(x);
					int ylocation = ntohs(y);
					std::cout << "id: " << id << "\n";
					std::cout << "Location: " << xlocation << "," << ylocation << "\n";
					// deserialization end

					// Producer area
					auto task = std::make_unique<struct taskPacket>();
					task->tid = sender_fd;
					task->pkt.size = size;
					task->pkt.type = type;
					task->pkt.idlen = idlen;
					task->pkt.x = x;
					task->pkt.y = y;
					memset(task->pkt.id,0,10);
					memcpy(task->pkt.id,id,10);
					std::cout << "packet packaging done.\n";
					std::unique_lock<std::mutex> lock(m);
					while(consumeQueue.size() == MAXBUFFERSIZE)
						empty.wait(lock,[&]{return consumeQueue.empty();});
					consumeQueue.push(std::move(task));
					std::cout << "Producer got packet!" << "\n";
					std::cout << "current queue size: " << consumeQueue.size() << "\n";
					fill.notify_one();
					lock.unlock();
					// Producer end
				}
				
			}

		}

	}
}