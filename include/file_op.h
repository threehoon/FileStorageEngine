#ifndef _FILE_OP_H_
#define _FILE_OP_H_

#include "common.h"

namespace tfs{
    namespace largefile{
        // 文件操作类
        class FileOperation{
        public:
            // O_CREAT（没有文件就创建一个）、O_RDWR（读写权限）、O_LARGEFILE（大文件）
            FileOperation( const std::string &file_name , const  int open_flags = O_CREAT | O_RDWR | O_LARGEFILE );
            ~FileOperation();

            int open_file();
            void close_file();

            int flush_file();  // 将文件立即写入到磁盘

            int unlink_file(); // 删除文件
            
            virtual int pread_file( char *buf ,const int32_t nbytes ,const int64_t offset );      // 精细化的读写
            virtual int pwrite_file( const char* buf ,const int32_t nbytes ,const int64_t offset );

            int write_file( const char*buf ,const int32_t nbytes );

            int64_t get_file_size( );                       // 获取文件大小

            int ftruncate_file( const int64_t lenght );     // 将文件截断
            int seek_file( const int64_t offset );          // 移动文件指针到指定的偏移位置

            int get_fd( )const{                             // 获取文件描述符
                return fd_;
            }

        protected:
            int fd_ ;   
            int open_flags_ ;                               // 文件访问的权限
            char *file_name_;                               // 文件名

            int  check_file( );                             // 检查文件是否打开

        protected:
            static const mode_t OPEN_MODE = 0644;           // 默认文件访问权限
            static const int MAX_DISK_TIMES = 5;            // 磁盘最大读取次数        
        };
    }
}


#endif // FILE_OP_H_