#include <iostream>
#include <netdb.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <string>
#include <string.h>
#include <sys/event.h>
#include <sys/types.h>
#include <sys/time.h>
#include <stdlib.h>
#include <fcntl.h>
#include <unistd.h>
#include <thread>
#include <chrono>


#define MAXIDLEN 10
#define PORT "3490"

#pragma pack(push,1)
struct MovePacket
{
    uint16_t size; // 2 byte
    uint16_t type; // 2 byte
    uint16_t idlen; // 2 byte
    char id[MAXIDLEN]; // max 10 byte
    uint16_t x;  // 2 byte
    uint16_t y;  // 2 byte
};
#pragma pack(pop)

void* get_in_addr(struct sockaddr* sa)
{
    if(sa->sa_family == AF_INET)
    {
        return &((reinterpret_cast<sockaddr_in*>(&sa))->sin_addr);
    }
    return &((reinterpret_cast<sockaddr_in6*>(&sa))->sin6_addr);
}

int sendall(int s, char* buffer, int len)
{
    int total = 0;
    int bytesleft = len;
    int n;
    while(total < len)
    {
        n = send(s,buffer+total,bytesleft,0);
        if(n <= 0)
        {
            if(n == 0)
                return 0;
            if(n == -1)
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


int recvall(int s,char* buffer, int len)
{
    int total = 0;
    int bytesleft = len;
    int n;
    while(total < len)
    {
        n = recv(s, buffer+total, bytesleft, 0);
        if(n <= 0)
        {
            if(n==0)
                return 0;
            else
            {
                if(errno == EAGAIN || errno == EWOULDBLOCK)
                {
                    usleep(1000);
                }
                else
                {
                    n=-1;
                    break;
                }
            }
     
        }
        else
        {
            total += n;
            bytesleft -= n;
        }
    }
    return n == -1 ? -1 : total;
}
int main(int argc, char** argv)
{
    if(argc != 3)
    {
        std::cerr << "Usage: client id Hostip" << "\n";
        exit(1);
    }
    uint16_t xloc = 10;
    uint16_t yloc = 10;
    struct addrinfo hints,*servinfo,*p;
    int status;
    int sockfd;
    memset(&hints,0,sizeof(hints));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    if((status = getaddrinfo(argv[2], PORT, &hints,&servinfo))==-1)
    {
        std::cerr << "client :getaddrinfo: " << gai_strerror(status);
        exit(1);
    }
    for(p = servinfo; p != NULL ; p = p->ai_next)
    {
        if((sockfd = socket(p->ai_family,p->ai_socktype,p->ai_protocol))==-1)
        {
            std::cerr << "client: socket"<<"\n";
            continue;
        }
        if(connect(sockfd, p->ai_addr, p->ai_addrlen) == -1)
        {
            std::cerr << "client: connect" << "\n";
            continue;
        }
        break;
    }
    if(p == NULL)
    {
        std::cerr << "Client: fail to bind" << "\n";
        exit(1);
    }
    fcntl(sockfd, F_SETFL , O_NONBLOCK);
    freeaddrinfo(servinfo);
    while(1)
    {
        struct MovePacket playerPacket;
        playerPacket.size = htons(20);
        playerPacket.type = htons(1);
        playerPacket.x = htons(xloc);
        playerPacket.y = htons(yloc);
        playerPacket.idlen = htons(strlen(argv[1]));
        memset(playerPacket.id,'\0',sizeof(playerPacket.id));
        memcpy(playerPacket.id,argv[1],strlen(argv[1]));
        int bytesend = sendall(sockfd,reinterpret_cast<char*>(&playerPacket),20);
        std::cout << "send byte: " << bytesend << "\n";
        std::cout << "packet information:(" << ntohs(playerPacket.size) <<"," << ntohs(playerPacket.type) <<"," << ntohs(playerPacket.idlen) << "," << std::string(playerPacket.id,strnlen(playerPacket.id,10)) << "," << ntohs(playerPacket.x) << "," << ntohs(playerPacket.y) << ")\n";
        std::cout << "Successfully connect to server.\n";
        std::this_thread::sleep_for(std::chrono::seconds(5));
    }
    close(sockfd);
    std::cout << "GoodBye!\n";
    return 0;
}