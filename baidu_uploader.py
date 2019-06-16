# -*- coding: utf-8 -*-
import concurrent.futures
import progressbar
import shutil
import os, json, sys, tempfile
from baidupcsapi import PCS

# 本脚本会把local_root_dir目录下的will_upload_paths文件同步到网盘的target_dir目录中, 且自动新建目录


# 网盘账号
user = ''
# 网盘密码
password = ''

# 能同时上传多少文件
max_uploader = 5
# 指定即将上传的文件/目录的全路径数组, 优先使用外部传进来的文件路径参数
will_upload_paths = (sys.argv[1:]) if len(sys.argv) > 1 else [
    unicode('D:/123', 'utf-8'),
]
# 本地的根目录全路径(末尾不带/或\分隔符)
local_root_dir = unicode('D:/123', 'utf-8')
# 上传到的网盘目录(不包括文件名, 末尾不带/分隔符), 网盘根目录为/
target_dir = '/test'
# 错误日志文件(记录上传失败的文件路径)
error_log_file = './error.txt'
# 成功日志文件(记录上传成功的文件路径)
success_log_file = './success.txt'

pcs = PCS(user, password)  # 登录百度网盘
local_root_dir = local_root_dir.replace('\\', '/')  # 统一处理文件路径分隔符


# 进度条类
class ProgressBar():
    def __init__(self):
        self.first_call = True

    def __call__(self, *args, **kwargs):
        if self.first_call:
            self.widgets = [progressbar.Percentage(), ' ', progressbar.Bar(marker=progressbar.RotatingMarker('>')),
                            ' ', progressbar.ETA()]
            self.pbar = progressbar.ProgressBar(widgets=self.widgets, maxval=kwargs['size']).start()
            self.first_call = False

        if kwargs['size'] <= kwargs['progress']:
            self.pbar.finish()
        else:
            self.pbar.update(kwargs['progress'])


# 打印对象下的属性值
def prn_obj(obj):
    print '\n'.join(['%s:%s' % item for item in obj.__dict__.items()])


class Result():
    def __init__(self, success, data='', error='', message=''):
        self.success = success
        self.data = data
        self.error = error
        self.message = message


# 普通上传
def normal_upload(file_full_path, target_dir):
    try:
        with open(file_full_path, 'rb') as file:
            print('start upload [ %s ]...\n' % (file_full_path))
            ret = pcs.upload(target_dir, file, os.path.basename(file_full_path),
                             callback=ProgressBar())
            content = json.loads(ret.content)
            if (content.has_key('md5')):
                return Result(success=True, data=content)
            else:
                return Result(success=False, error=content,
                              message='normal_upload error, file_full_path: %s' % file_full_path)
    except BaseException as e:
        return Result(success=False, error=e,
                      message='normal_upload unknown error, file_full_path: %s' % file_full_path)


# 极速上传
def rapid_upload(file_full_path, target_dir):
    try:
        with open(file_full_path, 'rb') as file:
            print('start upload [ %s ]...\n' % (file_full_path))
            ret = pcs.rapidupload(file, target_dir + '/' + os.path.basename(file_full_path))
            content = json.loads(ret.content)
            if (content['errno'] == 0 or content['errno'] == -8):
                return Result(success=True, data=content)
            else:
                return Result(success=False, error=content,
                              message='rapid_upload error, file_full_path: %s' % file_full_path)
    except BaseException as e:
        return Result(success=False, error=e, message='rapid_upload unknown error, file_full_path: %s' % file_full_path)


# 分片上传(一般针对大于2G的大文件上传)
def large_file_upload(file_full_path, target_dir):
    try:
        chinksize = 1024 * 1024 * 16  # 对于大文件上传, 每次上传16MB
        fid = 1
        md5list = []
        tmpdir = tempfile.mkdtemp('baidu_uploader')
        print('start upload [ %s ]...\n' % (file_full_path))
        with open(file_full_path, 'rb') as infile:
            while 1:
                data = infile.read(chinksize)
                if len(data) == 0: break
                smallfile = os.path.join(tmpdir, '%s.tmp%d' % (os.path.basename(file_full_path), fid))
                with open(smallfile, 'wb') as f:
                    f.write(data)
                fid += 1
                with open(smallfile, 'rb') as fsmall:
                    ret = pcs.upload_tmpfile(fsmall, callback=ProgressBar())
                    content = json.loads(ret.content)
                    if (content.has_key('md5')):
                        md5list.append(content['md5'])
                    else:
                        return Result(success=False, error=content,
                                      message='upload_tmpfile error, file_full_path: %s' % file_full_path)
                os.remove(smallfile)

        shutil.rmtree(tmpdir, True)  # 递归删除文件夹
        ret = pcs.upload_superfile(target_dir + '/' + os.path.basename(file_full_path), md5list)
        content = json.loads(ret.content)
        if (content.has_key('md5')):
            return Result(success=True, data=content)
        else:
            return Result(success=False, error=content,
                          message='large_file_upload error, file_full_path: %s' % file_full_path)
    except BaseException as e:
        shutil.rmtree(tmpdir, True)  # 递归删除文件夹
        return Result(success=False, error=e,
                      message='large_file_upload unknown error, file_full_path: %s' % file_full_path)


# 智能上传文件, 会根据文件智能选择极速上传/普通上传/分片上传
# 把file_full_path文件上传到网盘target_dir目录
def smart_upload(file_full_path, target_dir):
    try:
        LARGE_FILE_SIZE = 1024 * 1024 * 1024 * 2  # 百度网盘中大于2G的文件需要分片上传
        result = rapid_upload(file_full_path, target_dir)
        if (result.success == False):
            if (os.path.getsize(file_full_path) < LARGE_FILE_SIZE):
                result = normal_upload(file_full_path, target_dir)
                if (result.success == False):
                    result = large_file_upload(file_full_path, target_dir)
            else:
                result = large_file_upload(file_full_path, target_dir)
        return result

    except BaseException as e:
        return Result(success=False, data=file_full_path, error='',
                      message='smart_upload unknown error, file_full_path: %s' % file_full_path)


# 遍历目录, 返回包含所有文件的数组
def walk_dir(dir_full_path):
    files_full_path = []
    for root, dirs, files in os.walk(dir_full_path):
        for file in files:
            file_full_path = os.path.join(root, file).replace('\\', '/')  # 统一处理文件路径分隔符
            files_full_path.append(file_full_path)
    return files_full_path


# 遍历文件/目录数组, 返回包含所有文件的数组
def list_all_files_full_path(paths):
    all_files_full_path = []
    for path in paths:
        if os.path.isdir(path):
            all_files_full_path.extend(walk_dir(path))
        elif os.path.isfile(path):
            all_files_full_path.append(path.replace('\\', '/'))  # 统一处理文件路径分隔符
    return all_files_full_path


# 上传线程完成执行后的回调
def done_uploader_callback(future):
    result = future.result()
    if (result.success == True):
        with open(success_log_file, 'a+') as f:
            f.write(result.message + '\n')
    else:
        with open(error_log_file, 'a+') as f:
            f.write(result.message + '\n')


def main():
    all_files_full_path = list_all_files_full_path(will_upload_paths)
    futures = set()
    thread_pool = concurrent.futures.ThreadPoolExecutor(max_uploader, 'uploader')
    with thread_pool as executor:
        for file_full_path in all_files_full_path:
            remote_dst_dir = target_dir + os.path.dirname(file_full_path).replace(local_root_dir, '', 1)  # 该文件在网盘中的最终目录
            future = executor.submit(smart_upload, file_full_path, remote_dst_dir)
            future.add_done_callback(done_uploader_callback)
            futures.add(future)

    try:
        for future in concurrent.futures.as_completed(futures):
            err = future.exception()
            if err is not None:
                raise err

    except KeyboardInterrupt:
        print("stopped by hand")


if __name__ == '__main__':
    main()
