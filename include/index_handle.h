#ifndef _INDEX_HANDLE_H_
#define _INDEX_HANDLE_H_

#include "common.h"
#include "mmap_file_op.h"

namespace tfs{
    namespace largefile{

        struct IndexHeader{   // 索引头部
        public:
            IndexHeader(){
                memset( this , 0 ,sizeof(IndexHeader) );
            }

            BlockInfo block_info_;      // 块信息
            int32_t bucket_size_ ;      // hash桶的大小

            int32_t data_file_offset_;  // 块文件未使用数据的起始偏移

            int32_t index_file_size_;   // 索引文件当前的偏移

            int32_t free_head_offset_;  // 可重用的索引节点的偏移

        };

        // 索引文件处理类
        class IndexHandle{
        public:
            IndexHandle( const std::string  &base_path, const uint32_t main_block_id );
        
            ~IndexHandle( );

            // 索引文件的初始化（ 索引头部 + hash桶的偏移写入文件 ）并将索引文件映射到内存
            int create( const uint32_t logic_block_id ,const uint32_t  bucket_size,const MMapOption mmap_option );

            // 加载索引文件
            int load( const uint32_t logic_block_id ,const int32_t bucket_size, const MMapOption mmap_option );

            // 移除索引文件（ unmmap  and   unlink(删除文件) ）
            int remove( const uint32_t logic_block_id );

            // 从内存同步到磁盘
            int flush();

            // 将 metaInfo(小文件的信息)写入对应的hash桶中
            int32_t write_segmen_meta( const int64_t key , MetaInfo &meta );

            // 读取meta信息
            int32_t read_segmen_meta( const int64_t key ,MetaInfo  &meta);

            // 将 metaInfo(小文件的信息)从对应的hash桶中删除
            int32_t delete_segmen_meta(const  int64_t  key ,MetaInfo &meta );

            // 查找key是否存在
            int hash_find( const uint64_t key , int32_t &current_offset ,int32_t &previous_offset );

            // 获取哈希桶的起始位置
            int32_t *get_bucket_slot(){
                return reinterpret_cast<int32_t* >( reinterpret_cast<char*>( mmap_file_op_-> get_mmap_data()) + sizeof(IndexHeader) );
            }
            // 获取索引头部
            IndexHeader* get_index_header(){
                return reinterpret_cast< IndexHeader*>( mmap_file_op_->get_mmap_data());
            }

            // 获取块信息( 包含在索引头部里 )
            BlockInfo *get_block_info(){
                return static_cast<BlockInfo *>( mmap_file_op_->get_mmap_data());
            }

            // 获取hash桶的大小
            int32_t get_bucket_size(){
                return reinterpret_cast< IndexHeader*>( mmap_file_op_->get_mmap_data())->bucket_size_;
            }

            // 往对应的hash桶中插入一个 metaInfo 
            int32_t hash_insert(const uint64_t key , int32_t &previous_offset , MetaInfo& meta );
            
            // 获取块数据的偏移
            int32_t get_block_data_file_offset(){
                return reinterpret_cast< IndexHeader*>( mmap_file_op_->get_mmap_data())->data_file_offset_;
            }
            
            // 获取可重用节点的偏移
            int32_t get_free_head_offset()const{
                return reinterpret_cast< IndexHeader*>( mmap_file_op_->get_mmap_data())->free_head_offset_;
            }

            // 更新小文件在块信息中的偏移，存放下一个文件的起始地址
            void commit_blcok_data_file_offset( const int32_t file_size ){
                reinterpret_cast< IndexHeader*>( mmap_file_op_->get_mmap_data())->data_file_offset_ += file_size ;
            }

            // 更新块信息
            int update_block_info( const OperType oper_type ,const int32_t modify_size );

        private:
            MMapFileOperation *mmap_file_op_;   // 文件映射操作类
            bool is_load_;                      // 索引文件是否加载

            bool hash_compare(const uint64_t left , const uint64_t right ){
                return left == right;
            }
        };
    }
}


#endif   // _INDEX_HANDLE_H_