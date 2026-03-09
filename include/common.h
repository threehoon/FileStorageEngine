#ifndef _COMMON_H_
#define _COMMON_H_

#include <sys/mman.h>
#include <iostream>
#include <string.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include  <errno.h>
#include  <stdlib.h>
#include <stdio.h>
#include <assert.h>
#include <sstream>

namespace tfs{
    namespace largefile{
        // 
        enum OperType{
            C_OPER_INSERT = 1,
            C_OPER_DELETE = 2
        };

        const int32_t  EXIT_DISK_OPER_INCOMPLETE = -8012;       // 磁盘没有读取完整
        const int32_t  TSF_SUCCESS  = 0;
        const int32_t  TFS_ERROR =  -1;         
        const int32_t  EXIT_INDEX_ALREADY_LOADED_ERROR = -8013; // 索引文件已经被加载
        const int32_t  EXIT_META_UNEXPECT_FOUND_ERROR = -8014;  // 写入索引信息的时候，文件文件中已经存在  
        const int32_t  EXIT_INDEX_CORRUPT_ERROR = -8015;        // 索引文件损坏
        const int32_t  EXIT_BLOCKID_CONNFLICT_ERROR = -8016;    // 实际的块 id 与传入的不相符
        const int32_t  EXIT_BUCKET_SIZE_CONFLICT_ERROR = -8017; // hash桶大小冲
        const int32_t  EXIT_META_NOT_FOUND_ERROR = -8018;       // key不存在
        const int32_t  EXIT_BLOCKID_ZERO_ERROR = -8019;         // 块id为0

        /*
            这里是索引文件和块文件生成的路径
        */
        static const std::string MAINBLOCK_DIR_PREFIX = "/home/hoon/分布式核心存储引擎/src/FileStorageEngine/path/mainblock/";  // 主块文件的前缀

        static const std::string INDEX_DIR_PREFIX = "/home/hoon/分布式核心存储引擎/src/FileStorageEngine/path/index/";           // 索引文件的前缀

        static const mode_t DIR_MODE = 0755;   // 文件操作权限
    
        // 记录块信息
        struct BlockInfo{
        public:
            uint32_t block_id_;         // 块id
            int32_t  version_;          // 块当前版本号
            int32_t  file_count_;       // 当前已保存文件总数
            int32_t  size_;             // 当前已保存的文件总大小
            int32_t  del_file_count_;   // 已删除文件数量
            int32_t  del_size_;         // 删除文件的大小

            uint32_t seq_no_;           // 下一个可分配的文件编号

            // 构造函数
            BlockInfo( ){
                memset( this , 0 ,sizeof(BlockInfo) );
            }
            inline  bool operator==( const struct BlockInfo &blk ){
                return block_id_ == blk.block_id_             &&
                       version_ == blk.version_               &&
                       file_count_ == blk.file_count_         &&
                       size_ == blk.size_                     &&
                       del_file_count_ == blk.del_file_count_ &&
                       del_size_ == blk.del_size_             &&
                       seq_no_ == blk.seq_no_;
            }
        };

        // 记录块中小文件信息 ，小文件的信息( 文件编号 、偏移、大小等 )
        struct MetaInfo{
        public:
            MetaInfo( ){   init( );   }

            MetaInfo( const uint64_t file_id , const int32_t inner_offset , const int32_t size , const int32_t next_meta_offset )
            :file_id_( file_id ), location_{  inner_offset,  size  } ,
            next_meta_offset_( next_meta_offset ) {   }

            MetaInfo( const MetaInfo& meta_info ){
                memmove( this , &meta_info , sizeof( MetaInfo ) );
            }

            MetaInfo& operator= (const MetaInfo& meta_info){
                if( this == &meta_info )  {
                    return *this;
                }
                file_id_ =  meta_info.file_id_;
                location_.inner_offset_  = meta_info.location_.inner_offset_;
                location_.size_ = meta_info.location_.size_; 
                next_meta_offset_ = meta_info.next_meta_offset_; 
                return *this;
            }

            // 克隆
            MetaInfo& clone( const MetaInfo& meta_info ){
                // assert断言，用于在运行时检查某个条件是否成立。如果条件为 false，程序会终止运行 并输出错误信息。
                assert( this != &meta_info );
                *this = meta_info;
                return *this;
            }

            bool operator==(const MetaInfo& meta_info) const {
                return file_id_ ==  meta_info.file_id_ && 
                       location_.inner_offset_  == meta_info.location_.inner_offset_&&
                       location_.size_ == meta_info.location_.size_  && 
                       next_meta_offset_ == meta_info.next_meta_offset_;
            }

            uint64_t get_key() const {  // hash链表的 key 就是文件的 id
                return file_id_;
            }

            void set_key( const uint64_t  key){
                file_id_ = key;
            }

            uint64_t get_file_id()const{   // 获取文件id
                return file_id_;
            }
            void set_file_id(const  int64_t file_id ){
                this->file_id_ = file_id ; 
            }

            int32_t get_offset()const{    // 获取文件的偏移
                return location_.inner_offset_;
            }

            void set_offset(const int32_t offset ){
                location_.inner_offset_ = offset;
            }

            int32_t get_size()const{      // 获取文件的大小
                return location_.size_;
            }

            void  set_size( const int32_t size ){
                location_.size_ = size;
            }

            int32_t get_next_meta_offset()const{   // 获取当前哈希链表下一个节点在索引文件的偏移量（ 当前文件的偏移 + 文件的大小 = 下一个文件起始的的偏移 ）
                return next_meta_offset_ ;
            }

            void set_next_meta_offset(const  int32_t offset){
                next_meta_offset_ = offset;
            }
        
        private:
            void init( ){    // 初始化
                file_id_ =  0;
                location_.inner_offset_  = 0;
                location_.size_ = 0 ;
                next_meta_offset_ = 0;
            }

            uint64_t file_id_;          // 文件编号
            
            struct Location{       //记录文件在block里的位置     
                int32_t inner_offset_;  // 小文件在块文件内部的偏移量
                int32_t size_;          // 小文件的大小
            }location_;                 // 声明结构体的同时 ，创建一个实例
            int32_t next_meta_offset_;  // 当前哈希链表下一个节点在索引文件的偏移量
        };
    }
}

#endif // _COMM_H_