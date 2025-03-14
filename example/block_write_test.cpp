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

    std::string mainblock_path;    // 主块文件的路径
    std::string index_path;        // 索引文件的路径

    int32_t  ret = TSF_SUCCESS;

    std::cout<<"pleas input blockid: "<<std::endl;
    std::cin>>block_id;

    if( block_id < 0 ){
        std::cout<<"input error"<<std::endl;
        return -1;
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
    std::cout << "index file is load. block data file offset: " << index_handle->get_block_data_file_offset()
              << ". get bucket size:" << index_handle->get_bucket_size()
              << ". bucket_slot:" << index_handle->get_bucket_slot()
              << ". get_free_head_offset" << index_handle->get_free_head_offset()
              << ". block id_:" << index_handle->get_block_info()->block_id_
              << ". file count:_" << index_handle->get_block_info()->file_count_
              << ". seq_no_:" << index_handle->get_block_info()->seq_no_
              << ". all file size: " << index_handle->get_block_info()->size_ << std::endl;

    // 2. 加载主块文件
    std::stringstream tmp_stream;

    tmp_stream <<MAINBLOCK_DIR_PREFIX << block_id;
    
    tmp_stream >> mainblock_path ;

    FileOperation *fileOP = new FileOperation( mainblock_path , O_CREAT | O_RDWR | O_LARGEFILE ) ;

    std::cout << "main block file is load.."<< std::endl;

    // 3. 将数据写入到主块文件
    char buf[4096] ;
    memset( buf , '6' , sizeof( buf) );

    // 获取未写入数据的起始位置
    int32_t data_offset = index_handle->get_block_data_file_offset();

    // 下一个可分配的文件编号
    int32_t seq_no = index_handle->get_block_info()->seq_no_;

    ret = fileOP->pwrite_file( buf ,sizeof(buf) ,data_offset );
    if( ret  != TSF_SUCCESS ){
        std::cerr<<"main_block pwrite_file error."<<std::endl;
        fileOP->close_file();
        delete index_handle;
        delete fileOP;
        return -3;
    }

    // 4.数据写入到主块之后 ，将 mateInfo 写入到索引文件
    MetaInfo meta;
    
    meta.set_file_id( seq_no  );
    // set_offset 小文件在块内部的偏移
    meta.set_offset( data_offset );
    meta.set_size(  sizeof(buf) );
    
    // 5.将小文件的索引信息写入索引块对应的hash桶中
    ret  =  index_handle->write_segmen_meta( meta.get_key() ,meta);
    if( ret == TSF_SUCCESS ){
        // 更新块信息 、索引头部信息
        index_handle->commit_blcok_data_file_offset( meta.get_size() ) ;

        index_handle->update_block_info( C_OPER_INSERT , meta.get_size()  );

        // 将内存中的数据，同步到磁盘
        ret = index_handle->flush( );
        if( ret != TSF_SUCCESS ){
            std::cerr<<"index file flush error."<<" block id:"<<block_id
                     <<". file seq:"<<  seq_no  <<std::endl;
        }

    }else{
        std::cerr<<"write segmen meta  to index file  error."<<" block id:"<< block_id
                                                             <<". file seq:"<< seq_no <<std::endl;
        index_handle->remove( block_id );
        delete index_handle;
        index_handle = nullptr;

        fileOP->close_file();
        delete fileOP;
        fileOP = nullptr;

        return -4;
    }

    std::cout << "write segmen meta to index file. block data file offset: " << index_handle->get_block_data_file_offset()
              << ". get bucket size:" << index_handle->get_bucket_size()
              << ". bucket_slot:" << index_handle->get_bucket_slot()
              << ". get_free_head_offset" << index_handle->get_free_head_offset()
              << ". block id_:" << index_handle->get_block_info()->block_id_
              << ". file count:_" << index_handle->get_block_info()->file_count_
              << ". seq_no_:" << index_handle->get_block_info()->seq_no_
              << ". all file size: " << index_handle->get_block_info()->size_ << std::endl;

    fileOP->close_file();
    index_handle->remove(block_id);

    delete index_handle;
    delete fileOP;

    return 0;
}
