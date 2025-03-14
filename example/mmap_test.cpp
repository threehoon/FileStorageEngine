#include <iostream>
#include <unistd.h>
#include <fcntl.h>
#include <stdlib.h>

#include "include/mmap_file.h"


using namespace tfs;
using namespace largefile;

static struct MMapOption mmap_option = { 1024000 , 4096 ,4096 } ; 

// 文件映射操作类
int main( int, char** ){

    std::string fileName = "/home/wu/Decument/TFS_FileStorageEngine/path/CodeTest.txt";
    int fd = open( fileName.c_str() , O_RDWR | O_CREAT | O_LARGEFILE );
    if( fd <0 ){
        std::cout<<"file open failed\n";
        return -1;
    }

    std::cout<<"open file successed. "<<" fd :"<< fd<<std::endl;

    MMapFile mfile( fd , mmap_option );

    if ( !mfile.mmap_file(true) ){
        std::cerr<<"mmap_file is failed!"<<std::endl;
        close(fd);
        return -2;
    }

    std::cout<<"mmap file successed. "<<" fd :"<< fd
             <<". data: "<< mfile.get_data()
             <<". file size: "<<mfile.get_size()<<std::endl ; 

    // 解除内存映射
    mfile.remap_file();

    close(fd);

    return 0;
}