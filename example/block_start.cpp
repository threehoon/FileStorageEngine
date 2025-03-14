#include <stdio.h>
#include <sys/mman.h>
#include <stdlib.h>
#include <iostream>
#include <unistd.h>
#include <fcntl.h>

#include "include/common.h"
#include "include/mmap_file.h"
#include "include/file_op.h"
#include "include/mmap_file_op.h"
#include "include/index_handle.h"

using namespace tfs;
using namespace largefile;

const static mode_t fileMode = 0644;

const static uint32_t main_bloc_size = 1024*1024*64;  
const static uint32_t bucket_size = 1000;

static uint32_t block_id = 1;

static struct MMapOption mmap_option = { 1024000 , 4096 ,4096 } ; 

// 加载索引文件和初始化主块文件
int main(int, char**){

    std::string mainblock_path;    // 主块文件的路径
    std::string index_path;        // 索引文件的路径

    int32_t  ret = TSF_SUCCESS;

    std::cout<<"pleas input blockid: "<<std::endl;
    std::cin>>block_id;

    if( block_id < 0 ){
        std::cout<<"input error"<<std::endl;
        return -1;
    }

    // 1. 生产主块文件
    std::stringstream tmp_stream;
    tmp_stream << MAINBLOCK_DIR_PREFIX << block_id;
    tmp_stream >> mainblock_path;

    FileOperation *fileOP = new FileOperation( mainblock_path ,O_CREAT |O_RDWR | O_LARGEFILE ) ;

    // 修改文件大小
    ret = fileOP->ftruncate_file( main_bloc_size );
    if( ret != 0 ){
        std::cerr<<"block file ftruncate file is error"
                 <<" block id:"<< block_id
                 <<" main block path:"<<mainblock_path<<std::endl;
        fileOP->close_file();
        delete fileOP;
        return ret;
    }

    // 创建一个文件索引类的对象
    IndexHandle *index_handle = new IndexHandle("." , block_id );

    std::cout<<"load index file....."<<std::endl;

    // 1. 加载索引文件
    ret = index_handle->load( block_id ,bucket_size, mmap_option );
    if( ret != TSF_SUCCESS ){
        std::cerr<<"index load failed."
                 <<". errno:"<<ret
                 <<". block_id:"<<block_id
                 <<". bucket_size:"<<bucket_size<<std::endl;

        delete index_handle;
        index_handle = nullptr;
        return -2;
    }

    // 加载完毕之后打印调试信息
    std::cout << "block data file offset: " << index_handle->get_block_data_file_offset()
              << ". get bucket size:" << index_handle->get_bucket_size()
              << ". bucket_slot:" << index_handle->get_bucket_slot()
              << ". get_free_head_offset" << index_handle->get_free_head_offset()
              << ". block id_:" << index_handle->get_block_info()->block_id_
              << ". file count:_" << index_handle->get_block_info()->file_count_
              << ". seq_no_:" << index_handle->get_block_info()->seq_no_
              << ". all file size: " << index_handle->get_block_info()->size_ << std::endl;

    index_handle->remove( block_id );

    delete index_handle;

    return 0;
}
