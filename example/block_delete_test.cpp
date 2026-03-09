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

const static uint32_t bucket_size = 1000;   // hash桶的大小

static uint32_t block_id = 1;

static struct MMapOption mmap_option = { 1024000 , 4096 ,4096 } ; // 


int main(int, char**){

    std::string mainblock_path;
    std::string index_path;
    int32_t  ret = TSF_SUCCESS;

    std::cout<<"pleas input blockid: "<<std::endl;
    std::cin>>block_id;

    if( block_id <0 ){
        std::cout<<"input error"<<std::endl;
        return -1;
    }

    int32_t file_id = 0 ;
    std::cout<<"please input file id:"<<std::endl;  // 输入要读取的文件 id
    std::cin>>file_id;

    if( file_id < 0  ){
        std::cerr<<"input file id isvalid."<<std::endl;
        return -2;
    }

    // 1. 加载索引文件
    IndexHandle *index_handle = new IndexHandle("." , block_id );

    std::cout<<"load index file...."<<std::endl;
    
    ret = index_handle->load( block_id ,bucket_size, mmap_option );
    if( ret != TSF_SUCCESS ){
        std::cerr<<" index file load failed."
                 <<". errno:"<< ret
                 <<". block_id:"<< block_id
                 <<". bucket_size:"<< bucket_size<<std::endl;
        index_handle->remove( block_id );
        delete index_handle;
        return -3;
    }

    // 2. 加载主块文件
    std::stringstream tmp_stream;
    tmp_stream << MAINBLOCK_DIR_PREFIX << block_id;
    tmp_stream >> mainblock_path;

    FileOperation *fileOP = new FileOperation( mainblock_path ,O_CREAT |O_RDWR | O_LARGEFILE ) ;
    
    // 不会真正删除数据，而是标记为删除。
    // 索引文件维护已删除空间，以便复用。
    MetaInfo meta_info;
    ret = index_handle->delete_segmen_meta( file_id ,meta_info );
    if( ret != TSF_SUCCESS ){
        std::cerr<<"index handle  delete segmen meta falied. "
                 <<". file id:"<<file_id
                 <<". blcok id:"<<block_id<<std::endl;

        index_handle->remove(block_id);
        delete index_handle;

        fileOP->close_file();
        delete fileOP;

        return -4;
    }


    // 将索引文件数据从内存同步到磁盘
    index_handle->flush();
    std::cout<<"delete metaInfo successfully."<<std::endl;

    std::cout<<"file id:"<<file_id
             <<". blcok id:"<<block_id
             <<". size:"<<meta_info.get_size()
             <<". offset:"<<meta_info.get_offset()
             <<". meta.file_id:"<<meta_info.get_file_id()<<std::endl;
   
    fileOP->close_file();
    index_handle->flush();
    // index_handle->remove(block_id);

    delete index_handle;
    delete fileOP;

    return 0;
}
