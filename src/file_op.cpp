#include "file_op.h"

static int debug = 1;

namespace tfs{
    namespace largefile{
        FileOperation::FileOperation( const std::string &file_name , const int open_flags ){
            // strdup( const char *s ) 复制一个字符串，并返回一个指向新内存地址的指针
            file_name_ = strdup( file_name.c_str() );
            open_flags_  =  open_flags ;
            fd_  = -1 ;
        }

        FileOperation::~FileOperation( ){ 
            if( fd_> 0 ){
                ::close( fd_ ) ;
                fd_ = -1;
            }

            if( !file_name_ ){
                free( file_name_ );
                file_name_ = nullptr;
            }
        }
        
        int FileOperation::open_file( ){
            if( fd_ > 0 ){
                /*
                ::close(fd_);
                fd_ = -1;
                */
                return  fd_;
            }
            // 文件没有打开
            fd_  = ::open( file_name_ , open_flags_, OPEN_MODE );
            if( debug )
                std::cout << "ileOperation::open_file file name:" << file_name_ << std::endl;
            
            if( fd_< 0 ){
                std::cerr << "FileOperation::open file  faile. desc: "<< strerror(errno) << std::endl;
                return -errno;
            }
            std::cout<<"open file successfully. file fd:"<< fd_ <<std::endl;
            return fd_ ;    // 返回文件句柄
        }

        void FileOperation::close_file( ){    // 关闭文件
            if( fd_!= -1){
                close( fd_ );
                fd_ = -1;
            }
        }
            
        int FileOperation::flush_file( ){ // 将文件立即写入（同步）到磁盘
            // 检查 open_flags_（文件访问权限）是否包含 O_SYNC，如果包含表示该文件会以同步模式写入磁盘
            if( open_flags_ & O_SYNC ){    // O_SYNC 同步写入
                return 0;
            }
            int fd = check_file();
            if( fd < 0 ){
                return fd;
            }
            return fsync( fd );  // 将文件写入从缓存区写入磁盘
        } 

        int FileOperation::unlink_file(){  // 删除文件 
            close( fd_ );
            return unlink( file_name_ );
        }
            
        /*
            精细化读写的三个方法
        */
        int FileOperation::pread_file( char *buf ,const int32_t nbytes ,const int64_t offset ){
            int32_t left  = nbytes;        // 需要读取的数据长度
            int64_t read_offset = offset;  // 读取的偏移
            int32_t read_len = 0;          // 已经读取的数据大小
            char *p_temp = buf;            // 用于保存读取到的数据
            int count = 0;                 // 记录读取的次数
        
            while(  left >  0 ){           // 
                if(  check_file() < 0 ){
                    return -errno;
                }
                count++;
                if(  count >= MAX_DISK_TIMES  ){   // 判断是否达到磁盘最大读取次数
                    std::cerr<<"FileOperation::pread_file file arrive MAX_DISK_TIMES."<<std::endl;
                    break;
                }
                // 开始读取数据
                read_len = ::pread64( fd_ ,  p_temp ,  left ,  read_offset  );  
                if( read_len <  0 ){  // 读取失败
                    read_len = errno;
                    if ( read_len == EINTR || read_len == EAGAIN ) // EINTR系统调用被中断系统调用被中断  EAGAIN资源暂不可用
                    {
                        continue;
                    }
                    else if (read_len == EBADF)   // 文件描述符损坏
                    { 
                        std::cerr<<" FileOperation::pread_file file fd error"<<std::endl;
                        fd_ = -1;
                        return -read_len;
                    }
                    else  // 其他错误
                    {
                        return -read_len;
                    }
                }else if( read_len == 0 ){  // 文件读取完毕
                    break;
                }
                // 更新
                left -= read_len;   
                p_temp += read_len;
                read_offset += read_len;
            }
            if( left != 0 ){
                return EXIT_DISK_OPER_INCOMPLETE ;
            }
            return TSF_SUCCESS;
        }

        int FileOperation::pwrite_file(const char* buf ,const int32_t nbytes ,const int64_t offset){
            int32_t left = nbytes ;        // 需要写入的数据长度
            int64_t write_offset = offset; // 偏移
            int32_t write_len = 0;         // 已经写入数据的长度
            int count = 0 ;                // 写的次数
            const char* cur_buf = buf;     // 存储写入数据的缓存区

            while( left > 0 ){
                if( check_file() < 0 ){
                    std::cout<<"FileOperation::pwrite_file check_file error. erron:"<< strerror(errno) <<std::endl;
                    return -errno;
                }
                count++;
                if( count >= MAX_DISK_TIMES ){
                    std::cerr<<"FileOperation::pwrite_file arrive file MAX_DISK_TIMES."<<std::endl;
                    break;
                }

                write_len = ::pwrite64( fd_ , cur_buf ,left , write_offset );

                if( write_len  < 0 ){  // 读取失败
                    std::cout<<"FileOperation::pwrite_file process into write_len < 0. desc: "<<strerror(errno)<<" .write len:"<< write_len <<std::endl;
                    write_len = errno;
                    if( write_len == EINTR || write_len ==EAGAIN ){
                        continue;
                    }else if( write_len == EBADF ){   // 文件描述符损坏
                        std::cerr<<"pwrite_file ->fd error"<<std::endl;
                        fd_ = -1;
                        continue;
                    }else{   // 其他的错误
                        return -write_len;
                    }
                }else if( write_len == 0 ){
                    std::cerr << "FileOperation::pwrite_file pwrite64 returned 0." << std::endl;
                    break;
                }
                left -= write_len;
                cur_buf += write_len;
                write_offset += write_len;
            }

            if( left != 0 ){
                return EXIT_DISK_OPER_INCOMPLETE;
            }

            return TSF_SUCCESS;
        }

        int FileOperation::write_file( const char*buf ,const int32_t nbytes ){
            int32_t left = nbytes ; 
            int32_t write_len = 0;
            int64_t write_offset =  0;

            int count = 0;
            
            while( left > 0 ){
                if(check_file( ) <0 ){
                    return -errno;
                }
                if( ++count >= MAX_DISK_TIMES ){
                    break;
                }
                write_len = ::write( fd_ , buf + write_offset , left );
                if( write_len  < 0 ){
                    write_len = errno;
                    if( write_len == EINTR || write_len ==EAGAIN ){
                        continue;
                    }else if( write_len == EBADF ){ // 文件描述符损坏
                        fd_ = -1;
                        continue;
                    }else{
                        return -write_len;
                    }
                }else if( write_len == 0 ){
                    std::cerr << "FileOperation::write_file write returned 0." << std::endl;
                    break;
                }
                write_offset += write_len;
                left -= write_len;
            }
            if( left != 0 ){
                return EXIT_DISK_OPER_INCOMPLETE;
            }
            return TSF_SUCCESS;
        }

        int64_t FileOperation::get_file_size(){  // 获取文件大小
            check_file( );
            struct stat file_stat;
            // fstat 获取已打开文件的元数据（如大小、权限、inode号)等
            if( fstat( fd_ ,&file_stat ) == -1){
                std::cerr<<"FileOperation::get_file_size fstat error. error desc: "<<strerror(errno)<<std::endl;
                return -1;
            }
            return file_stat.st_size;
        }

        /*
            如果文件没有打开  ，则打开
        */
        int  FileOperation::check_file( ){
            if( fd_ < 0 ){
                return  open_file();
            }
            return fd_;
         }

        int FileOperation::ftruncate_file( const int64_t lenght ){    // 将文件截断
            if(  check_file() < 0  )   return -1;

            if( ftruncate( fd_ , lenght ) == -1){
                std::cerr<<"ftruncate error. error desc: "<<strerror(errno)<<std::endl;
                return -1;
            }
            return 0;
        }

        int FileOperation::seek_file(const int64_t offset){  // 移动文件指针到指定的偏移位置
            if(  check_file() < 0  )   return -1;            
            return  lseek( fd_ , offset , SEEK_SET );        
        }
    }
}