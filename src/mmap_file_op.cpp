#include "mmap_file_op.h"

static int debug = 1;  // 调试使用

namespace tfs{
    namespace largefile{
        
        int MMapFileOperation::pwrite_file( const char *buf ,int32_t nbytes , int64_t offset ){
            // 1. 内存已经映射
            if( is_mapped_  && ( offset + nbytes ) > mmap_file_-> get_size() ){ // 映射的内存不够
                if( debug ){
                    std::cout<<"MMapFileOperation::pwrite_file. mmap area deficiency nbytes:"<<nbytes
                             <<". offset:"<< offset
                             <<". mmap file size:"<< mmap_file_->get_size()<<std::endl;
                }
                mmap_file_->remap_file();    // 允许再次映射扩容一次
            }
            // 从映射区域直接写入数据
            if( is_mapped_  && ( offset + nbytes ) <= mmap_file_->get_size()  ){
                memmove( (char *)mmap_file_->get_data() + offset  , buf , nbytes );
                if( debug )std::cout<<"MMapFileOperation::pwrite_file write from memory."<<std::endl;
                return TSF_SUCCESS;
            }
            if( debug ) std::cout<<"MMapFileOperation::pwrite_file write from not memory."<< std::endl;
            // 2. 没有映射 直接写入磁盘，或者要写的数据太大，映射区域不够
            return FileOperation::pwrite_file( buf , nbytes ,offset );
        }

        int MMapFileOperation::pread_file( char *buf ,int32_t nbytes ,int64_t offset ){
            // 1. 内存已经映射
            if( is_mapped_  && ( offset + nbytes ) > mmap_file_->get_size() ){ // 映射的内存不够
                std::cout<<"MMapFileOperation::pread_file. mmap area deficiency nbytes:"<<nbytes
                         <<", offset:"<< offset
                         <<", mmap file size:"<< mmap_file_->get_size()<<std::endl;
                mmap_file_->remap_file();
            }

            if( is_mapped_  && ( offset + nbytes ) <= mmap_file_->get_size() ){
                memmove( buf , (char*)mmap_file_->get_data() + offset , nbytes );
                if( debug )  std::cout<<"MMapFileOperation::pread_file read from memory."<<std::endl;
                return TSF_SUCCESS;
            }
            if( debug )   std::cout<<"MMapFileOperation::pread_file read from not memory."<<std::endl;
            // 2. 没有映射 从磁盘读取，或者要读取的数据映射不全
            return FileOperation::pread_file( buf , nbytes ,offset );
        }

        int MMapFileOperation::mmap_file( const  struct MMapOption& mmap_option ){

            if( mmap_option.max_mmap_size_ <  mmap_option.first_mmap_size_ )  return TFS_ERROR;
            if( mmap_option.max_mmap_size_ <= 0 )  return TFS_ERROR;
            
            int fd  = check_file();
            if( fd < 0 ) {
                std::cerr<<"MMapFileOperation::mmap_file check_file error. desc:"<<strerror(errno)<<std::endl;
                return TFS_ERROR;
            }  

            if( !is_mapped_ ){   //  no mmap file
                if( mmap_file_ ){
                    delete mmap_file_;
                }
                mmap_file_ = new MMapFile( fd ,mmap_option );
                
                is_mapped_ = mmap_file_->mmap_file( true );
                if( !is_mapped_ ){
                    std::cerr<<"MMapFileOperation::mmap_file mmap_file failed."<< std::endl;
                    return TFS_ERROR;
                }
                if( debug )  std::cout<<"MMapFileOperation::mmap_file successfully."<<std::endl;
            }
            return TSF_SUCCESS ;
        }

        // 解除映射
        int MMapFileOperation::munmap_file(){
            if( is_mapped_ && mmap_file_ != nullptr ){
                delete mmap_file_;
                is_mapped_ = false;
            }
            return TSF_SUCCESS;
        }

        void *MMapFileOperation::get_mmap_data()const{
           if(  is_mapped_ )   return  mmap_file_->get_data();
           return nullptr;
        }

        int MMapFileOperation::flush_file(){  // 将文件立即写入到磁盘
            if( is_mapped_ ){
                if( mmap_file_->sync_file() ){
                    return TSF_SUCCESS;
                }else{
                    return TFS_ERROR;
                }
            }
            // 如果文件没有映射
            return FileOperation::flush_file();
        }
    }
}