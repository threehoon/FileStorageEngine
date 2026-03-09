#include "index_handle.h"

static const int debug = 1;

namespace tfs{
    namespace largefile{
        
        IndexHandle::IndexHandle( const std::string  &base_path, const uint32_t main_block_id ){
            // 创建文件映射操作类
            std::stringstream tmp_stream;
            // tmp_stream<<base_path<<INDEX_DIR_PREFIX<<main_block_id;       // /root/wu/index/1
            tmp_stream<<INDEX_DIR_PREFIX<< base_path <<"/"<< main_block_id; // /home/wu/Document/FileStorageEngine/path/index/base_path/main_block_id
            std::string index_path;
            tmp_stream >> index_path;
            if( debug )
                std::cout << "index file path:" << index_path << std::endl;
                
            mmap_file_op_ = new MMapFileOperation(index_path); // 创建一个映射文件操作类
            is_load_ =  false;
        }

        IndexHandle::~IndexHandle(){
            if( mmap_file_op_ ){
                delete mmap_file_op_;
                mmap_file_op_ = nullptr;
                is_load_ = false;
            }
        }
        // 逻辑块id == 块id
        int IndexHandle::create( const uint32_t logic_block_id , const uint32_t  bucket_size, const MMapOption mmap_option ){
            
            int ret = TSF_SUCCESS;
            
            if( debug ){
                std::cout<<"IndexHandle::create ."<<" logic_block_id:"<< logic_block_id
                          <<". bucket_size:"<< bucket_size
                          <<". mmap_option first_mmap_size_:"<< mmap_option.first_mmap_size_
                          <<". mmap_option max_mmap_size_:"<< mmap_option.max_mmap_size_
                          <<". mmap_option per_mmap_size_:"<< mmap_option.per_mmap_size_ <<std::endl;
            }
            // 判断索引文件是否被加载
            if( is_load_ ){
                return EXIT_INDEX_ALREADY_LOADED_ERROR;
            }
            // 获取映射文件的大小
            int64_t file_size = mmap_file_op_-> get_file_size();

            if( file_size < 0 ){
                return TFS_ERROR;
            }else if( file_size == 0  ){                         // empty file  索引文件还为被创建
                // 索引头部 + hash桶的偏移写入文件 , 然后映射到内存
                IndexHeader i_header;                            // 索引头部
                i_header.block_info_.block_id_ = logic_block_id;
                i_header.block_info_.seq_no_ = 1;                // 文件编号从0开始

                i_header.bucket_size_ = bucket_size;             // hash桶大小    

                // 索引头部 + 存放hash桶需要的大小
                i_header.index_file_size_ = sizeof( IndexHeader ) + bucket_size*sizeof( int32_t ) ;

                char *init_data = new char[ i_header.index_file_size_ ] ; 

                memcpy( init_data , &i_header ,sizeof(IndexHeader) );

                // 将存放hash桶的位置置0 ， 因为此时还有没创建 hash桶
                memset( init_data + sizeof(IndexHeader) , 0 , bucket_size*sizeof(int32_t) );
                // 将初始化的数据写入到索引文件
                /*
                    pwrite_file 不管文件有没有映射，都会写入到文件
                */
                ret = mmap_file_op_->pwrite_file( init_data , i_header.index_file_size_ , 0 );  // 将索引头部 + 存放hash桶需要的大小，的信息写入文件，构成索引文件
                delete[]  init_data;
                init_data = nullptr;

                if( ret != TSF_SUCCESS  ){
                    if( debug )
                        std::cout << "IndexHandle::create. pwrite_file is failed. erron desc:" << strerror(errno) << std::endl;

                    return ret;
                }

                // 立即将文件同步磁盘
                ret = mmap_file_op_->flush_file();
                if( ret != TSF_SUCCESS ){
                    if( debug )
                        std::cout << "IndexHandle::create. flush_file is failed. erron desc:" << strerror(errno) << std::endl;
                    return ret;
                }
            }else{   // file size > 0 说明索引文件已经存在了
                if( debug )  std::cout<<"IndexHandle::create. index info  is aleady exist"<<std::endl;
                return  EXIT_META_UNEXPECT_FOUND_ERROR;
            }

            // 将索引文件映射到内存
            ret = mmap_file_op_->mmap_file( mmap_option );
            if( ret!= TSF_SUCCESS ){
                return ret;
            }
            is_load_ =  true;
            if(debug){
                std::cout<<"IndexHandle::create successefully."<<std::endl;
                std::cout<<"bucket_size_:"<<get_index_header()->bucket_size_
                         <<". data_file_offset_:"<<get_index_header()->data_file_offset_
                         <<". free_head_offset_:"<<get_index_header()->free_head_offset_
                         <<". index_file_size_:"<<get_index_header()->index_file_size_
                         <<". block_info_.block_id_:"<<get_index_header()->block_info_.block_id_
                         <<". block_info_.del_file_count_:"<<get_index_header()->block_info_.del_file_count_
                         <<". block_info_.del_size_:"<<get_index_header()->block_info_.del_size_
                         <<". block_info_.file_count_:"<<get_index_header()->block_info_.file_count_
                         <<". block_info_.seq_no_"<<get_index_header()->block_info_.seq_no_
                         <<". block_info_.version_:"<<get_index_header()->block_info_.version_
                         <<". block_info_.size_"<<get_index_header()->block_info_.size_<<std::endl;
            }
            return TSF_SUCCESS;
        }  

        // 加载索引文件
        int IndexHandle::load( const uint32_t logic_block_id ,const int32_t bucket_size, const MMapOption mmap_option ){
            
            int ret =  TSF_SUCCESS; 

            if( is_load_ ){
                return EXIT_INDEX_ALREADY_LOADED_ERROR ;
            } 
            // 获取映射到内存的文件大小
            int64_t file_size  = mmap_file_op_->get_file_size();
            
            if( file_size  < 0 ){
                return file_size;
            }else if( file_size == 0 ){        // index file currupt
                if( debug )
                    std::cout << "IndexHandle::load get_file_size == 0. index file is currupt." << std::endl;
                return EXIT_INDEX_CORRUPT_ERROR;
            }

            MMapOption temp_mmap_option = mmap_option;
            // 为了更新实际的文件映射大小
            if( file_size > temp_mmap_option.first_mmap_size_ && file_size <= temp_mmap_option.max_mmap_size_ ){
                 temp_mmap_option.first_mmap_size_ = file_size;
            }

            // 映射到内存
            ret = mmap_file_op_->mmap_file( temp_mmap_option  );
            if( ret !=TSF_SUCCESS ){
                return ret;
            }
            // hash桶的大小 == 0  &&  块id == 0 (初始化之后的索引文件块id ！= 0 ) ， 说明索引文件加载有问题
            if( get_bucket_size() ==0 ||  get_block_info()->block_id_ == 0 ){
                
                std::cerr<<"IndexHandle::load. index file is currput."
                         <<". bucket_size:"<<get_bucket_size()
                         <<". block_info->block_id_:"<<get_block_info()->block_id_<<std::endl;
                return  EXIT_INDEX_CORRUPT_ERROR;
            }
            // 获取索引文件的大小
            // 索引文件的理论值 sizeof( IndexHeader ) + get_bucket_size()*sizeof(int32_t);
            int32_t index_file_size = sizeof( IndexHeader ) + get_bucket_size()*sizeof(int32_t);
            
            if( file_size  < index_file_size ){
                std::cerr<<"IndexHandle::load. index file size is conflict."
                         <<". cur index file size:"<< file_size
                         <<". index file size:"<< index_file_size<<std::endl;
                return EXIT_INDEX_CORRUPT_ERROR;
            }
            
            // check bucket size
            if( get_bucket_size() != bucket_size ){
                std::cerr<<"IndexHandle::load. bucket size conflict"
                         <<". cur bucket size:"<< get_bucket_size()
                         <<". index bucket size: "<< bucket_size <<std::endl;
                return EXIT_BUCKET_SIZE_CONFLICT_ERROR;
            }

            // check block id
            if( get_block_info()->block_id_ !=  logic_block_id ){
                std::cerr<<"IndexHandle::load. block id is conflict"
                         <<". cur block_id:"<<get_block_info()->block_id_ 
                         <<". index block_id:"<<logic_block_id<<std::endl;
                return EXIT_BLOCKID_CONNFLICT_ERROR;
            }

            is_load_ = true;

            if(debug){
                std::cout<<"IndexHandle::load successed."<<std::endl;
                std::cout<<"bucket_size_:"<<get_index_header()->bucket_size_
                         <<". data_file_offset_:"<<get_index_header()->data_file_offset_
                         <<". free_head_offset_:"<<get_index_header()->free_head_offset_
                         <<". index_file_size_:"<<get_index_header()->index_file_size_
                         <<". block_info_.block_id_:"<<get_index_header()->block_info_.block_id_
                         <<". block_info_.del_file_count_:"<<get_index_header()->block_info_.del_file_count_
                         <<". block_info_.del_size_:"<<get_index_header()->block_info_.del_size_
                         <<". block_info_.file_count_:"<<get_index_header()->block_info_.file_count_
                         <<". block_info_.seq_no_"<<get_index_header()->block_info_.seq_no_
                         <<". block_info_.version_:"<<get_index_header()->block_info_.version_
                         <<". block_info_.size_:"<<get_index_header()->block_info_.size_<<std::endl;
            }
            return TSF_SUCCESS;
        }

        // 移除索引文件
        int IndexHandle::remove( const uint32_t logic_block_id ){
            if( is_load_ ){
                if( get_block_info()->block_id_ != logic_block_id ){
                    std::cerr<<"IndexHandle::remove blcok id conflict."<<std::endl;
                    return EXIT_BLOCKID_CONNFLICT_ERROR;
                }
            }
            // 解除映射
            int ret = mmap_file_op_->munmap_file();
            if( ret != TSF_SUCCESS ){
                return ret;
            }
            // 从磁盘删除文件
            return mmap_file_op_->unlink_file();
        }

        // 将映射到内存的索引文件同步到磁盘
        int IndexHandle::flush( ){  
            int ret = mmap_file_op_->flush_file();
            if( ret != TSF_SUCCESS ){
                std::cerr<<"IndexHandle::flush  flush index file error"<<std::endl;
                return ret;
            }
            return ret;
        }

        // 将 metaInfo(小文件的信息)写入对应的hash桶中
        int IndexHandle::write_segmen_meta( const int64_t key ,MetaInfo &meta  ){
            /*
                从文件哈希表中判断key是否存在   find_hash()
                不存在就将 metaInfo  写入到文件哈希表中
            */
           
           int32_t current_offset = 0 , previous_offset = 0;
           int ret = TSF_SUCCESS ;

           ret = hash_find(key ,current_offset , previous_offset );
           if( ret == TSF_SUCCESS ){ // key存在
                return EXIT_META_UNEXPECT_FOUND_ERROR;
           }else if( ret != EXIT_META_NOT_FOUND_ERROR ){
                std::cerr<<"IndexHandle::write_segmen_meta hash_find key not exist error"<<std::endl;
                return ret;
           }
           // 不存在的情况
           return hash_insert(key ,previous_offset ,meta );
        }
        
        int32_t IndexHandle::delete_segmen_meta(const  int64_t  key ,MetaInfo &meta ){
            int32_t current_offset = 0 , previous_offset = 0;
            int ret = TSF_SUCCESS;

            ret = hash_find(key ,current_offset ,previous_offset );
            if( ret!= TSF_SUCCESS ){ // key不存在
                return EXIT_META_NOT_FOUND_ERROR;
            }
            // 删除meta结点
            // 将当前结点的 meta 信息读出来
            ret  = mmap_file_op_->pread_file( reinterpret_cast<char *>( &meta ) ,sizeof( MetaInfo ), current_offset );
            if( ret != TSF_SUCCESS){
                std::cerr<<"IndexHandle::delete_segmen_meta. file_op_->pread_file error."<<", file id:"<<key<<std::endl;
                return ret;
            }
            // MetaInfo tmp_meta;
            int32_t  next_pos = meta.get_next_meta_offset();
            if( previous_offset == 0 ) {  // 说明当前的结点是hash表中的一个首节点
                if(debug){
                    std::cout<<"hash slot first data"<<std::endl;
                }
                int32_t slot =  static_cast<int32_t>( key)% get_bucket_size() ;
                get_bucket_slot()[slot] = next_pos;
            }else{
                MetaInfo pre_mete;
                ret  =  mmap_file_op_->pread_file( reinterpret_cast<char *>( &pre_mete ),sizeof(MetaInfo)  ,previous_offset );
                if(  ret != TSF_SUCCESS ){
                    return ret;
                }
                pre_mete.set_next_meta_offset( next_pos );
                // 将meta节点写回去
                ret = mmap_file_op_->pwrite_file( reinterpret_cast<const char *>( &pre_mete ),sizeof(MetaInfo),previous_offset );
                if( ret !=TSF_SUCCESS ){
                     return ret;
                }
            }
            /*
                将删除节点加入可从用的节点
            */
            // 头插法
            meta.set_next_meta_offset( get_free_head_offset() );
            if(debug){
                std::cout<<"reuse offset:"<< meta.get_offset()<<std::endl;
            }
            // 同步到磁盘
            ret = mmap_file_op_->pwrite_file( reinterpret_cast<const char *>( &meta ),sizeof(MetaInfo), current_offset );
            if( ret != TSF_SUCCESS ){
                return ret;
            }
            get_index_header()->free_head_offset_ = meta.get_offset();
            
            update_block_info(C_OPER_DELETE , meta.get_size());
            return TSF_SUCCESS;

        }
        

        int32_t IndexHandle::read_segmen_meta( const int64_t key , MetaInfo  &meta){
            int32_t current_offset = 0 , previous_offset = 0;
            int ret = TSF_SUCCESS;
            ret = hash_find( key ,current_offset, previous_offset );
            if( ret != TSF_SUCCESS ){ // key 不存在
                return EXIT_META_NOT_FOUND_ERROR ;
            }
            ret = mmap_file_op_ -> pread_file( reinterpret_cast<char*>(&meta) ,sizeof(meta) ,current_offset );
            return ret;
        }

        int IndexHandle::hash_find( const uint64_t key , int32_t &current_offset ,int32_t &previous_offset ){
            // 1. 找到在hash表中的具体位置
            // 2. 读取桶首节点存储的第一个节点的偏移，如果偏移为 0 ，说明key不存在 返回EXIT_META_NOT_FOUND_ERROR
            // 3. 根据偏移读取存储的metainfo，每次偏移都是一个metainfo的大小
            // 4. 与key进行比较，相等则设置current_offset and previous_offset  返回，不相等执行 5 
            // 5. 从meta中取出下一个meta的偏移 ， 如果偏移为 0 ，说明key不存在 返回EXIT_META_NOT_FOUND_ERROR
            uint32_t slot = ( int32_t )key % get_bucket_size() ; // 找到在那一个hash桶里面 
            int32_t  pos =  get_bucket_slot()[slot] ;            // get_bucket_slot()拿到的是索引文件中hash链表的开始地址
            
            MetaInfo meta_info ; 
            int ret = TSF_SUCCESS ;

            for( ; pos != 0 ; ){ 
                ret = mmap_file_op_-> pread_file( reinterpret_cast<char*>( &meta_info) , sizeof( MetaInfo) , pos );
                if( ret != TSF_SUCCESS ){
                    return ret;
                }

                if(  hash_compare( key , meta_info.get_key() ) ){
                    current_offset = pos;
                    return TSF_SUCCESS;
                } 
                previous_offset = pos;
                pos = meta_info.get_next_meta_offset( );
            }
    
            return EXIT_META_NOT_FOUND_ERROR;
        }

        int32_t IndexHandle::hash_insert( const uint64_t key , int32_t &previous_offset , MetaInfo& meta ){
            int ret = TSF_SUCCESS ;
            int32_t current_offset = 0;
            MetaInfo tmp_meta_info ;

            // 1. 确定key在hash桶的位置
            int32_t slot = static_cast<int32_t>( key % get_bucket_size() );
           
            int32_t free_pos =  get_free_head_offset();
            // 2. 判断是否有可重用节点的偏移
            if( free_pos != 0 ){   
                // 将可重用的结点读出放入 临时的metaInfo节点
                ret = mmap_file_op_-> pread_file( reinterpret_cast<char *>( &tmp_meta_info ), sizeof(MetaInfo), free_pos );
                if( ret!= TSF_SUCCESS ){
                    return ret;
                }

                current_offset = free_pos;   // 当前可插入的节点偏移
                // 更新下一个可重用节点的偏移
                get_index_header() -> free_head_offset_ = tmp_meta_info.get_next_meta_offset();

                if( debug ){
                    std::cout<<"IndexHandle::hash_insert. current free offset:"<<free_pos
                             <<". updated free offset:"<< tmp_meta_info.get_next_meta_offset()<<std::endl;
                }
            // 没有可重用结点
            }else{   
                // 3. 确定meta 节点存储在文件中的偏移
                current_offset = get_index_header()-> index_file_size_;
                get_index_header()->index_file_size_ += sizeof( MetaInfo );   // 跟新索引文件的偏移
            }

            // 4. 将meta写入到索引文件对应的位置
            meta.set_next_meta_offset( 0 );    // 默认作为链表尾部。

            ret = mmap_file_op_->pwrite_file( reinterpret_cast <const char *>(&meta), sizeof(MetaInfo) , current_offset );
            if( ret != TSF_SUCCESS){ 
                get_index_header()->index_file_size_ -= sizeof( MetaInfo );
                return ret;
            }
            
            // 5. 将meta节点插入到hash链表中
            if( previous_offset != 0 ){
                ret = mmap_file_op_->pread_file( reinterpret_cast<char*>(&tmp_meta_info), sizeof(MetaInfo), previous_offset );
                if( ret !=TSF_SUCCESS ){
                    get_index_header()->index_file_size_ -= sizeof( MetaInfo );
                    return ret;
                }

                tmp_meta_info.set_next_meta_offset( current_offset );

                ret = mmap_file_op_->pwrite_file( reinterpret_cast<char*>(&tmp_meta_info),sizeof(MetaInfo), previous_offset );

                if( ret!= TSF_SUCCESS){
                    get_index_header()->index_file_size_ -= sizeof( MetaInfo );
                    return ret;
                } 
            }else{  // 说明还桶中没有元素
                get_bucket_slot()[slot] = current_offset;
            }
            return TSF_SUCCESS;
        }

        int IndexHandle::update_block_info( const OperType oper_type ,const int32_t modify_size ){
            if( get_block_info()->block_id_ == 0 ){
                return EXIT_BLOCKID_ZERO_ERROR;
            }
            if( oper_type == C_OPER_INSERT ){  
                get_block_info()->file_count_ ++;
                get_block_info()->seq_no_++;
                get_block_info()->version_++;
                get_block_info()->size_ += modify_size;
            }else if( oper_type == C_OPER_DELETE ){
                get_block_info()->del_file_count_++;
                get_block_info()->del_size_ += modify_size;
                get_block_info()->size_  -= modify_size;
                get_block_info()->version_++;
                get_block_info()->file_count_ --;
            }   

            if( debug ){
                std::cout<<"update blcok info. file_count:"<< get_block_info()->file_count_ 
                         <<". seq_no:"<<get_block_info()->seq_no_
                         <<". version:"<<get_block_info()->version_
                         <<". size_:"<<get_block_info()->size_
                         <<". del_size_:"<<get_block_info()->del_size_
                         <<". del_file_count:"<<get_block_info()->del_file_count_<<std::endl;
            }
            return TSF_SUCCESS;
        }
    }
}