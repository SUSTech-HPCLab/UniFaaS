SUPPORT_COMPRESSOR = ["gzip"]

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

    def generate_compressed_remotefile(old_file):
        if compress_method == "gzip":
            new_path = gzip_compress_file(old_file)
        else:
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
    
