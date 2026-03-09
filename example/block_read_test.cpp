#include <stdio.h>
#include <sys/mman.h>
#include <stdlib.h>
#include <iostream>
#include <unistd.h>
#include <fcntl.h>

#include "common.h"
#include "mmap_file.h"
#include "file_op.h"
#include "mmap_file_op.h"
#include "index_handle.h"

using namespace tfs;
using namespace largefile;

const static mode_t fileMode = 0644;

const static uint32_t main_bloc_size = 1024*1024*64;  
const static uint32_t bucket_size = 1000;

static uint32_t block_id = 1;

static struct MMapOption mmap_option = { 1024000 , 4096 ,4096 } ; // 


int main(int, char**){

    std::string mainblock_path;
    std::string index_path;
    int32_t  ret = TSF_SUCCESS;

    std::cout<<"pleas input blockid"<<std::endl;
    std::cin>>block_id;

    if( block_id < 0 ){
        std::cout<<"input error"<<std::endl;
        return -1;
    }

    // 1. 加载索引文件
    IndexHandle *index_handle = new IndexHandle("." , block_id );

    std::cout<<"load index file.."<<std::endl;
    
    ret = index_handle->load( block_id ,bucket_size,mmap_option );
    if( ret != TSF_SUCCESS ){
        std::cerr<<"index load failed."
                 <<", errno:"<<ret
                 <<", block_id:"<<block_id
                 <<", bucket_size:"<<bucket_size<<std::endl;
        index_handle->remove( block_id );
        delete index_handle;
        index_handle = nullptr;
        return -2;
    }

    // 2. 加载主块文件
    std::stringstream tmp_stream;
    tmp_stream <<MAINBLOCK_DIR_PREFIX << block_id;
    tmp_stream >> mainblock_path;

    FileOperation *fileOP = new FileOperation( mainblock_path ,O_CREAT |O_RDWR | O_LARGEFILE ) ;
   
   // 读取主块文件数据
    int32_t file_id = 0 ;
    std::cout<<"please input file id:"<<std::endl;  // 输入要读取的文件 id
    std::cin >> file_id;

    if( file_id < 0  ){
        std::cerr<<"input file id isvalid."<<std::endl;
        return -3;
    }
    // 从索引文件拿到小文件的索引信息 ， 再去块文件读取数据
    MetaInfo meta_info;
    ret = index_handle->read_segmen_meta( file_id ,meta_info );
    if( TSF_SUCCESS != ret ){
        std::cerr<<"index_handle read segmen meta is error. file id:" <<file_id << std::endl;
        return -4;
    }

    char *read_buf = new char[ meta_info.get_size() + 1] ;

    ret = fileOP->pread_file( read_buf , meta_info.get_size() , meta_info.get_offset() );

    read_buf[meta_info.get_size()] = '\0'; 

    if( ret != TSF_SUCCESS ){
        std::cerr<<"fileOP pread file is failed."<<std::endl;

        index_handle->remove( block_id );
        fileOP->close_file();

        delete index_handle;
        delete fileOP;
        return -5;
    }
    
    std::cout<<"read file successfully. file id:"<<file_id
             <<". read size:"<<strlen( read_buf ) 
             <<". read buf:"<< read_buf <<std::endl;

    fileOP->close_file();
    index_handle->flush();
    // index_handle->remove(block_id);

    delete index_handle;
    delete fileOP;

    return 0;
}
