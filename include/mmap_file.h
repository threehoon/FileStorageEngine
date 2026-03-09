#ifndef _MMAP_FILE_H_
#define _MMAP_FILE_H_

#include <unistd.h>
#include <stdint.h>

namespace tfs{
    namespace largefile{

        struct MMapOption{
            int32_t  max_mmap_size_;   // 内存映射的最大大小
            int32_t  first_mmap_size_; // 第一次映射的大小
            int32_t  per_mmap_size_;   // 下一次映射追加的大小
        };

        class MMapFile{
        public:
            MMapFile( );
            explicit MMapFile( int fd );
            MMapFile( int fd , const  MMapOption &option );

            ~MMapFile(); 
            
            // 将文件映射到内存
            bool mmap_file( const bool wirte = false);
            // 将mmap映射的内存数据同步到磁盘
            bool sync_file( );

            void *get_data( )const;   // 获取文件映射到内存的首地址
            int32_t get_size()const;  // 获取数据大小

            bool munmap_file( );     // 解除映射
            bool remap_file();       // 重新映射, 追加映射内存
        private:    
            bool ensure_file_size(const int32_t size );  //对文件进行扩容

        private:
            int fd_;                // 文件描述符
            int32_t  file_size_;    // 文件大小
            void  *data_;           // 指向文件映射的内存地址

            struct MMapOption mmap_file_option_;   // 文件映射的选项（映射到内存的大小）
        };
    }
}



#endif  // _MMAP_FILE_H_