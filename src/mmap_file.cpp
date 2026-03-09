#include "mmap_file.h"
#include "common.h"

static int debug = 1;

namespace tfs{
    namespace largefile{
        MMapFile::MMapFile():file_size_( 0 ),fd_(-1),data_( nullptr ){

        }

        MMapFile::MMapFile( int fd ):file_size_( 0 ),fd_( fd ),data_( nullptr ){

        }

        MMapFile::MMapFile(int fd , const struct MMapOption &option ):
            file_size_( 0 ),fd_( fd ),data_(nullptr){

            mmap_file_option_.max_mmap_size_ = option.max_mmap_size_ ;
            mmap_file_option_.first_mmap_size_ =option.first_mmap_size_ ;
            mmap_file_option_.per_mmap_size_ = option.per_mmap_size_ ;

        }

        MMapFile::~MMapFile(){
            if( data_ ) {
                if( debug ) {
                    std::cout<<"MMapFile::~MMapFile mmap file destroy."
                            <<" fd: "<<fd_
                            <<" size: "<<file_size_
                            <<", data:"<< data_ <<std::endl;
                }

                // 对文件进行同步
                msync( data_ , file_size_ , MS_SYNC );
                munmap_file( ); 
            }
            data_ = nullptr;
            file_size_ = 0;
            fd_ = -1;
            memset( &mmap_file_option_ , 0 , sizeof(mmap_file_option_));
        }

        // 将文件映射到内存
        bool MMapFile::mmap_file( const bool wirte ){
            if( fd_ < 0 )   return false;
            // struct MMapOption mmap_file_option_  初始化有误
            if( mmap_file_option_.max_mmap_size_ == 0 )   return false;

            if( file_size_ < mmap_file_option_.max_mmap_size_ ){
                file_size_ = mmap_file_option_.first_mmap_size_ ;
            }else{
                file_size_ = mmap_file_option_.max_mmap_size_;
            }
            
            int flags = PROT_READ ;  // 内存映射权限 ， 表示允许读取
            if( wirte ){             // 判断是否允许写入（ 函数参数传入 ）
                flags |= PROT_WRITE ;// 在之前允许读取的基础上加上允许写入的权限
            }
            // 调整文件大小
            if( !ensure_file_size( file_size_ ) ){
                std::cerr<<"MMapFile::mmap_file ensure_file_size error. desc error:"<< strerror(errno) 
                         <<" file size:"<<file_size_<<std::endl;
                return false;
            }
            // MAP_SHARED 共享映射，多个进程可以共享修改
            data_ =  mmap( nullptr ,file_size_ , flags , MAP_SHARED , fd_, 0 );
            if( data_ == MAP_FAILED ){  // mmap()失败时返回 MAP_FAILED 
                std::cerr<<"MMapFile::mmap_file mmap file failed . "<<strerror( errno )<<std::endl;;
                data_ = nullptr;
                fd_  = -1;
                file_size_ = 0 ;
                return false ;
            }
            if( debug ){
                std::cout<<"MMapFile::mmap_file mmap file successfully  fd:"<< fd_
                        <<", file size: "<< file_size_
                        <<", data addr: "<< data_ <<std::endl;
            }
            return true;
        }

        // 将mmap映射的内存数据同步到磁盘
        bool MMapFile::sync_file( ){
            // 判读文件是否已经映射
            if( data_ && file_size_ > 0 ){                            // MS_SYNC  同步写入：等待数据完全写入磁盘后才返回
                return  msync( data_ ,file_size_ , MS_ASYNC ) == 0 ;  // MS_ASYNC 异步写入：立即返回，后台慢慢同步
        }
            return true;
        }

        void *MMapFile::get_data( )const{ 
            return data_;
        }

        // 获取数据大小
        int32_t MMapFile::get_size()const{
            return file_size_;
        }

        bool MMapFile::munmap_file( ){
            if( !data_ ){
                return true;
            }
            return munmap( data_ ,file_size_ ) ==0 ;
        }

        bool MMapFile::ensure_file_size( const int32_t size ){
            struct stat fileStat; // 用于存储文件信息，包括大小等

            if ( fstat( fd_ , &fileStat ) == -1){
                std::cerr<<"MMapFile::ensure_file_size fstat error. error desc:"<<strerror(errno)<<std::endl;
                return false;
            }
            // 获取到的文件实际大小，小于传入的文件大小
            if( fileStat.st_size < size ){
                // ftruncate 调整文件大小，扩展的部分（填充'\0'）
                if( ftruncate( fd_ , size ) == -1){
                    std::cerr<<"MMapFile::ensure_file_size ftruncate error. error desc: "<<strerror(errno)<<std::endl;
                    return false;
                }
            }
            return true;
        }

        bool MMapFile::remap_file(){
            // 判断文件是否已经映射
            if( fd_ == -1 || data_ == nullptr ){
                std::cerr<<"MMapFile::remap_file have not mmap"<<std::endl;
                return false;
            }
            if( file_size_  == mmap_file_option_.max_mmap_size_ ){
                std::cerr<<"MMapFile::remap_file is Reach max mmap size."<<std::endl;
                return false;
            }
            // 新的大小
            int32_t new_size = file_size_ + mmap_file_option_.per_mmap_size_;
            if( new_size > mmap_file_option_.max_mmap_size_  ){  // 如果追加之后的大小大于设置的映射最大大小
                new_size = mmap_file_option_.max_mmap_size_ ;    // 设置为最大的大小
            }
            // 同步扩容文件大小
            if( !ensure_file_size( new_size )){
                std::cerr<<"MMapFile::remap_file ensure_file_size  error. desc error:"<<strerror(errno)
                         <<" file size:"<<file_size_<< std::endl;
                return false;
            }
            if( debug ) {
                std::cout<<"MMapFile::remap_file fd :"<< fd_
                         <<", start file size :"<< file_size_
                         <<"  new file size: "<<new_size << std::endl;
            }
            // 重新映射
            void *new_data = mremap( data_ ,file_size_ ,new_size ,MREMAP_MAYMOVE );
            if( new_data == (void *)-1){
                std::cerr<<"MMapFile::remap_file mremap error. error desc:"<<strerror(errno)
                         <<" new size:"<<new_size
                         <<", fd :"<<fd_
                         <<", new data:"<<new_data
                         <<", old data"<<data_<<std::endl;
                return false;
            }
            // 更新属性
            file_size_ = new_size;
            data_ = new_data;
            return true;
        }   
    }
}