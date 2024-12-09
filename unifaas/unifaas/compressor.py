SUPPORT_COMPRESSOR = ["gzip", "fpack"]


def specialized_transfer_task(file_arg):
    from funcx.sdk.file import RemoteFile
    from funcx.sdk.file import RsyncFile
    from funcx.sdk.file import GlobusFile
    import os.path

    if isinstance(file_arg, RsyncFile):
        file_path = file_arg.get_remote_file_path()
        return RsyncFile.remote_generate(os.path.basename(file_path))
    if isinstance(file_arg, GlobusFile):
        file_path = file_arg.get_remote_file_path()
        return GlobusFile.remote_generate(os.path.basename(file_path))


def decompress_func(rt_arg, compress_method):
    from funcx.sdk.file import RemoteFile
    from funcx.sdk.file import RsyncFile
    from funcx.sdk.file import GlobusFile
    import os.path

    def gzip_decompress_file(file):
        import gzip
        file_path = file.get_remote_file_path()
        new_file_path = file_path[:-1*len(".gz")]
        with open(file_path, 'rb') as f_in:
            with open(new_file_path, 'wb') as f_out:
                f_out.write(gzip.decompress(f_in.read()))
        return new_file_path

    def fpack_decompress_file(file):
        import subprocess
        file_path =file.get_remote_file_path()
        # 构造 fpack 命令
        new_file_path = file_path[:-1*len(".fz")]

        if os.path.exists(new_file_path):
            # fpack如果遇到重复的output会报error
            return new_file_path

        command = ["funpack", file_path]
        try:
            subprocess.run(command, check=True)
            return new_file_path
        except subprocess.CalledProcessError as e:
            return file_path
    
    def generate_decompress_remotefile(old_file):
        if compress_method == "gzip":
            new_path = gzip_decompress_file(old_file)
        elif compress_method == "fpack":
            new_path = fpack_decompress_file(old_file)
        else:
            #TODO we should add other compression algorithms
            return old_file
        
        if isinstance(old_file, RsyncFile):
            base_name = os.path.basename(new_path)
            return RsyncFile.remote_generate(base_name)
        if isinstance(old_file, GlobusFile):
            base_name = os.path.basename(new_path)
            return GlobusFile.remote_generate(base_name)
    
    if isinstance(rt_arg, RemoteFile):
        return generate_decompress_remotefile(rt_arg)
    
    if isinstance(rt_arg, list):
        res = []
        for arg in rt_arg:
            res.append(generate_decompress_remotefile(arg))
        return res

    if isinstance(rt_arg, tuple):
        res = []
        for arg in rt_arg:
            res.append(generate_decompress_remotefile(arg))
        return tuple(res)

        

def compress_func(rt_arg, compress_method):
    # only support list, tuple, single file 
    from funcx.sdk.file import RemoteFile
    from funcx.sdk.file import RsyncFile
    from funcx.sdk.file import GlobusFile
    import os.path
   

    def gzip_compress_file(file):
        import gzip
        file_path = file.get_remote_file_path()
        new_file_path = file_path + ".gz"

        with open(file_path, 'rb') as f_in:
            with gzip.open(new_file_path, 'wb') as f_out:
                f_out.writelines(f_in)
        return new_file_path

    def fpack_compress_file(file):
        import subprocess
        file_path =file.get_remote_file_path()
        # 构造 fpack 命令
        command = ["fpack", file_path]
        new_file_path = file_path + ".fz"

        if os.path.exists(new_file_path):
            # fpack如果遇到重复的output会报error
            return new_file_path

        try:
            subprocess.run(command, check=True)
            return new_file_path
        except subprocess.CalledProcessError as e:
            return e



    def generate_compressed_remotefile(old_file):
        if compress_method == "gzip":
            new_path = gzip_compress_file(old_file)
        elif compress_method == "fpack":
            new_path = fpack_compress_file(old_file)
        else:
            #TODO
            return old_file

        if isinstance(old_file, RsyncFile):
            base_name = os.path.basename(new_path)
            return RsyncFile.remote_generate(base_name)
        if isinstance(old_file, GlobusFile):
            base_name = os.path.basename(new_path)
            return GlobusFile.remote_generate(base_name)
        
    if isinstance(rt_arg, RemoteFile):
        return generate_compressed_remotefile(rt_arg)
    
    if isinstance(rt_arg, list):
        res = []
        for arg in rt_arg:
            res.append(generate_compressed_remotefile(arg))
        return res

    if isinstance(rt_arg, tuple):
        res = []
        for arg in rt_arg:
            res.append(generate_compressed_remotefile(arg))
        return tuple(res)
    
