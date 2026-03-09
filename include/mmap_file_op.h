#ifndef  _MMAP_FILE_OP_H_
#define  _MMAP_FILE_OP_H_

#include "common.h"
#include "file_op.h"
#include "mmap_file.h"

namespace tfs{

    namespace largefile{
        
        class MMapFileOperation: public FileOperation{
        public:
            MMapFileOperation( const std::string &fileName , const int open_flags = O_CREAT |O_RDWR | O_LARGEFILE )
                            :FileOperation( fileName , open_flags )
                            ,mmap_file_( nullptr )  
                            ,is_mapped_( false ){    } 
                            
            ~MMapFileOperation(){
                if( mmap_file_ ){
                    delete mmap_file_;
                    mmap_file_ = nullptr;
                    is_mapped_ = false;
                }
            }

            // 映射文件的读写，如果有映射则从内存读取，没有映射则从磁盘读取
            virtual int pwrite_file(const char *buf ,int32_t nbytes , int64_t offset);
            virtual int pread_file( char *buf ,int32_t nbytes ,int64_t offset );
            
            // 文件映射
            int mmap_file( const struct MMapOption& mmap_option );
            // 解除映射
            int munmap_file();

            void *get_mmap_data()const;
            int flush_file();   // 将文件立即写入到磁盘

        private:    
            MMapFile *mmap_file_ ; // 文件映射类
            bool is_mapped_;      // 文件是否映射
        };
    }
}



#endif //_MMAP_FILE_OP_H_