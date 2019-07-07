# -*- coding: utf-8 -*-
import getpass
import json
import os
import shutil
import sys
import tempfile
import time
from Queue import Queue
import concurrent.futures
import progressbar
import ConfigParser
import shelve
import zlib
from baidupcsapi import PCS

reload(sys)
sys.setdefaultencoding("utf-8")

# 配置文件读取路径
config_file = './config.txt'

success_log_file = ''
error_log_file = ''


class Config():
    def __init__(self):
        self.read_config()
        global error_log_file
        error_log_file = self.error_log_file
        global success_log_file
        success_log_file = self.success_log_file

    def read_config(self):
        cf = ConfigParser.ConfigParser()
        cf.read(config_file)
        section = 'config'
        # 网盘账号
        self.username = cf.get(section, 'username') or raw_input('input username:')
        # 网盘密码
        self.password = cf.get(section, 'password') or getpass.getpass('input password:')
        # 默认上传的本地文件/目录全路径, 优先使用外部传进来的文件路径参数
        self.default_local_upload_path = unicode(cf.get(section, 'default_local_upload_path') or '', 'utf-8')
        # 默认上传到的远程目录
        self.default_remote_dir = unicode(cf.get(section, 'default_remote_dir') or '', 'utf-8')
        # 能同时上传多少文件
        self.max_uploader = cf.getint(section, 'max_uploader') or 5
        # 本地的根目录全路径
        self.local_root_dir = ''
        # 上传的本地文件/目录全路径, 优先使用外部传进来的文件路径参数
        self.local_upload_path = ''
        # 上传到的网盘目录(不包括文件名, 末尾不带/分隔符), 网盘根目录为/
        self.remote_dir = ''
        # 错误日志文件(记录上传失败的文件路径)
        self.error_log_file = unicode(cf.get(section, 'error_log_file') or './error.txt', 'utf-8')
        # 成功日志文件(记录上传成功的文件路径), 默认成功上传不输出日志文件
        self.success_log_file = unicode(cf.get(section, 'success_log_file') or '', 'utf-8')
        # 进度条更新频率(N秒/次), 当任务很多时, 建议设大一点, 避免频繁刷新进度条导致看不清
        self.bar_update_interval = cf.getint(section, 'bar_update_interval') or 5
        # 错误重试次数
        self.retry_times = cf.getint(section, 'retry_times') or 2
        # 本地缓存文件(记录上传成功文件的路径, 不填表示不使用缓存)
        # 上传文件时优先检测本地缓存, 若本地文件路径和远程上传目录完全一致则不上传)
        self.cache_file = unicode(cf.get(section, 'cache_file') or '', 'utf-8')


# 记录文件上传的参数类
class UploadConfig():
    def __init__(self, file_full_path, remote_dst_dir):
        self.file_full_path = file_full_path
        self.remote_dst_dir = remote_dst_dir


class BaiduUploader():
    thread_pool = None  # 线程池
    pcs = None  # 百度网盘上下文
    config = None  # 配置实例
    file_queue = None  # 待上传文件的队列
    futures = []  # 记录所有的future的数组
    retry_map = {}  # 记录所有的错误重试记录
    cache = None  # 本地缓存实例

    def login(self, config):
        self.pcs = PCS(config.username, config.password)

    def init_cache(self):
        if (self.config.cache_file != ''):
            self.cache = Cache(self.config.cache_file, self.config.username)

    def has_cache(self, file_full_path, remote_dst_dir):
        if (self.cache != None):
            return self.cache.has_key(file_full_path, remote_dst_dir)
        else:
            return False

    def set_cache(self, file_full_path, remote_dst_dir):
        if (self.cache != None):
            return self.cache.set_if_not_exist(file_full_path, remote_dst_dir)
        else:
            return True

    def start_upload(self):
        self.file_queue = Queue()
        self.config = Config()
        # 登录百度网盘
        self.login(self.config)
        # 配置合法性判断
        self.validity_check(self.config)
        # 初始化缓存
        self.init_cache()

        all_files_full_path = self.list_all_files_full_path(self.config.local_upload_path)
        self.thread_pool = concurrent.futures.ThreadPoolExecutor(self.config.max_uploader, 'uploader')
        for file_full_path in all_files_full_path:
            remote_dst_dir = self.config.remote_dir + os.path.dirname(file_full_path).replace(
                self.config.local_root_dir, '', 1)  # 该文件在网盘中的最终目录
            if (self.has_cache(file_full_path, remote_dst_dir) == False):
                # 若不命中本地缓存, 则待上传文件入队列
                self.file_queue.put(UploadConfig(file_full_path, remote_dst_dir))
        with self.thread_pool as executor:
            for file_full_path in all_files_full_path:
                remote_dst_dir = self.config.remote_dir + os.path.dirname(file_full_path).replace(
                    self.config.local_root_dir, '', 1)  # 该文件在网盘中的最终目录
                if (self.has_cache(file_full_path, remote_dst_dir) == False):
                    future = executor.submit(self.smart_upload, file_full_path, remote_dst_dir)
                    self.futures.append(future)
            # 必须写在executor生效的上下文中
            while self.futures:
                # 如果上传完成
                for future in concurrent.futures.as_completed(self.futures):
                    self.futures.remove(future)
                    self.done_uploader(future, executor)

        # 等待所有文件上传完毕
        self.file_queue.join()
        print ('\n\n-------------- finish all upload --------------\n\n')

    # 配置合法性判断
    def validity_check(self, config):
        if len(sys.argv) > 2:
            my_raw_input('only support one upload directory at most\n')
            os._exit(-1)
        # 指定即将上传的本地文件/目录的全路径数组, 优先使用外部传进来的文件路径参数
        config.local_upload_path = to_unicode(sys.argv[1], 'utf-8') if len(
            sys.argv) == 2 else config.default_local_upload_path
        while True:
            answer = my_raw_input(u'[ %s ] will upload (Y / Other path):' % config.local_upload_path)
            config.local_upload_path = self.path_process(
                config.local_upload_path if answer.strip() in ['y', 'Y', ''] else answer)
            if os.path.isfile(config.local_upload_path):
                config.local_root_dir = self.path_process(os.path.dirname(config.local_upload_path)).rstrip('\\/')
                break
            elif os.path.isdir(config.local_upload_path):
                config.local_root_dir = self.path_process(config.local_upload_path).rstrip('\\/')
                break

        config.remote_dir = config.default_remote_dir
        answer = my_raw_input(u'Will upload to [ %s ] (Y / Other path):' % config.default_remote_dir)
        config.remote_dir = self.path_process(config.remote_dir if answer.strip() in ['y', 'Y', ''] else answer)

    # 统一把路径中的\替换成/
    def path_process(self, path):
        return path.replace('\\', '/')

    # 普通上传
    def normal_upload(self, file_full_path, target_dir):
        try:
            with open(file_full_path, 'rb') as file:
                ret = self.pcs.upload(target_dir, file, os.path.basename(file_full_path),
                                      callback=ProgressBar(file_full_path, self.config))
                content = json.loads(ret.content)
                if (content.has_key('md5')):
                    return Result(success=True, data=UploadConfig(file_full_path, target_dir),
                                  message='normal_upload success, file_full_path: %s' % file_full_path)
                else:
                    return Result(success=False, data=UploadConfig(file_full_path, target_dir), error=content,
                                  message='normal_upload error, file_full_path: %s' % file_full_path)
        except Exception as e:
            return Result(success=False, data=UploadConfig(file_full_path, target_dir), error=e,
                          message='normal_upload unknown error, file_full_path: %s' % file_full_path)

    # 极速上传
    def rapid_upload(self, file_full_path, target_dir):
        try:
            with open(file_full_path, 'rb') as file:
                ret = self.pcs.rapidupload(file, target_dir + '/' + os.path.basename(file_full_path))
                content = json.loads(ret.content)
                if (content['errno'] == 0 or content['errno'] == -8):
                    print ('rapid_upload success, file_full_path: %s\n' % file_full_path)
                    return Result(success=True, data=UploadConfig(file_full_path, target_dir),
                                  message='rapid_upload success, file_full_path: %s' % file_full_path)
                else:
                    return Result(success=False, data=UploadConfig(file_full_path, target_dir), error=content,
                                  message='rapid_upload error, file_full_path: %s' % file_full_path)
        except Exception as e:
            return Result(success=False, data=UploadConfig(file_full_path, target_dir), error=e,
                          message='rapid_upload unknown error, file_full_path: %s' % file_full_path)

    # 分片上传(一般针对大于2G的大文件上传)
    def large_file_upload(self, file_full_path, target_dir):
        try:
            chinksize = 1024 * 1024 * 16  # 对于大文件上传, 每次上传16MB
            fid = 1
            md5list = []
            tmpdir = tempfile.mkdtemp('baidu_uploader')
            with open(file_full_path, 'rb') as infile:
                while 1:
                    data = infile.read(chinksize)
                    if len(data) == 0: break
                    smallfile = os.path.join(tmpdir, '%s.tmp%d' % (os.path.basename(file_full_path), fid))
                    with open(smallfile, 'wb') as f:
                        f.write(data)
                    fid += 1
                    with open(smallfile, 'rb') as fsmall:
                        ret = self.pcs.upload_tmpfile(fsmall, callback=ProgressBar(file_full_path, self.config))
                        content = json.loads(ret.content)
                        if (content.has_key('md5')):
                            md5list.append(content['md5'])
                        else:
                            return Result(success=False, error=content,
                                          message='upload_tmpfile error, file_full_path: %s' % file_full_path)
                    os.remove(smallfile)

            shutil.rmtree(tmpdir, True)  # 递归删除文件夹
            ret = self.pcs.upload_superfile(target_dir + '/' + os.path.basename(file_full_path), md5list)
            content = json.loads(ret.content)
            if (content.has_key('md5')):
                return Result(success=True, data=UploadConfig(file_full_path, target_dir),
                              message='large_file_upload success, file_full_path: %s' % file_full_path)
            else:
                return Result(success=False, data=UploadConfig(file_full_path, target_dir), error=content,
                              message='large_file_upload error, file_full_path: %s' % file_full_path)
        except Exception as e:
            shutil.rmtree(tmpdir, True)  # 递归删除文件夹
            return Result(success=False, data=UploadConfig(file_full_path, target_dir), error=e,
                          message='large_file_upload unknown error, file_full_path: %s' % file_full_path)

    # 遍历目录, 返回包含所有文件的数组
    def walk_dir(self, dir_full_path):
        files_full_path = []
        for root, dirs, files in os.walk(dir_full_path):
            for file in files:
                # 统一处理文件路径分隔符
                file_full_path = self.path_process(os.path.join(root, file))
                files_full_path.append(file_full_path)
        return files_full_path

    # 遍历文件/目录数组, 返回包含所有文件的数组
    def list_all_files_full_path(self, path):
        all_files_full_path = []
        if os.path.isdir(path):
            all_files_full_path.extend(self.walk_dir(path))
        elif os.path.isfile(path):
            # 统一处理文件路径分隔符
            all_files_full_path.append(self.path_process(path))
        return all_files_full_path

    # 智能上传文件, 会根据文件智能选择极速上传/普通上传/分片上传
    # 把file_full_path文件上传到网盘target_dir目录
    def smart_upload(self, file_full_path, target_dir):
        try:
            # 维护待上传文件队列准确, 手工get
            upload_config = self.file_queue.get()
            print('%s | start upload, and %d files left ...\n' % (file_full_path, self.file_queue.qsize()))
            LARGE_FILE_SIZE = 1024 * 1024 * 1024 * 2  # 百度网盘中大于2G的文件需要分片上传
            result = self.rapid_upload(file_full_path, target_dir)
            if (result.success == False):
                if (os.path.getsize(file_full_path) < LARGE_FILE_SIZE):
                    result = self.normal_upload(file_full_path, target_dir)
                    if (result.success == False):
                        result = self.large_file_upload(file_full_path, target_dir)
                else:
                    result = self.large_file_upload(file_full_path, target_dir)
            self.file_queue.task_done()
            return result

        except Exception as e:
            self.file_queue.task_done()
            return Result(success=False, data=UploadConfig(file_full_path, target_dir), error=e,
                          message='smart_upload unknown error, file_full_path: %s' % file_full_path)

    # 上传线程完成执行后的回调
    def done_uploader(self, future, executor):
        err = future.exception()
        result = future.result()
        upload_config = result.data
        # 上传成功记录日志和成功缓存
        if (result.success == True):
            self.cache.set_if_not_exist(upload_config.file_full_path, upload_config.remote_dst_dir)
            if (success_log_file != ''):
                with open(success_log_file, 'a+') as f:
                    f.write(result.message + '\n')
            return
        # 上传失败则错误重试和打印日志
        if (result.success != True or err is not None):
            retry_key = "%s %s" % (upload_config.file_full_path, upload_config.remote_dst_dir)
            error_count = self.retry_map.get(retry_key, 0)
            print (result.message + '. and %d files left; retry %d; done_uploader error: %s\n' % (
                self.file_queue.qsize(), error_count, result.error))
            if (error_count < self.config.retry_times):
                # 进行错误重试
                self.retry_map[retry_key] = error_count + 1
                self.file_queue.put(upload_config)
                new_future = executor.submit(self.smart_upload, upload_config.file_full_path,
                                             upload_config.remote_dst_dir)
                self.futures.append(new_future)
            else:
                # 重试次数用完则打印日志
                with open(error_log_file, 'a+') as f:
                    f.write(
                        result.message + '. and %d files left. error: %s\n' % (self.file_queue.qsize(), result.error))


def to_unicode(str_or_unicode, encode):
    try:
        return unicode(str_or_unicode, encode)
    except UnicodeDecodeError as e:
        # 解决不同系统不同shell环境下编码不同导致的编码转换出错问题
        return unicode(str_or_unicode, 'gbk' if encode == 'utf-8' else 'utf-8')


def my_raw_input(unicode_prompt=''):
    encode = 'gbk' if sys.platform.startswith('win') else 'utf-8'
    str = raw_input(unicode_prompt.encode(encode))
    return to_unicode(str, encode)


# 本地缓存类
# 上传文件时优先检测本地缓存, 若本地文件路径和远程上传目录完全一致则不上传)
class Cache():
    fcache = None  # 缓存文件句柄
    username = ''  # 百度帐号用户名
    user_dict = {}  # 该用户下的成功缓存
    sync_interval = 80  # 同步缓存到硬盘的间隔次数

    def __init__(self, cache_file, username):
        try:
            self.fcache = shelve.open(cache_file)
        except:
            os.remove(cache_file)
            self.fcache = shelve.open(cache_file)
        self.username = username
        self.init_username_key()

    def init_username_key(self):
        # 初始化用户名的key
        if (self.fcache.has_key(self.username) == False):
            self.fcache[self.username] = {}
            self.fcache.sync()
        self.user_dict = self.fcache[self.username]

    def create_key(self, file_full_path, remote_dst_dir):
        # 使用crc算法加密缓存key, 且使用如下key减少碰撞可能性
        key = '%s %s %s %s %s' % (file_full_path, remote_dst_dir, self.username, file_full_path, remote_dst_dir)
        return zlib.crc32(key)

    def set_if_not_exist(self, file_full_path, remote_dst_dir):
        if (self.has_key(file_full_path, remote_dst_dir) == False):
            self.user_dict[self.create_key(file_full_path, remote_dst_dir)] = True
            if (self.sync_interval <= 0):
                self.fcache[self.username] = self.user_dict
                self.fcache.sync()
                self.sync_interval = 80  # 同步缓存到硬盘的间隔次数
            self.sync_interval = self.sync_interval - 1
        return True

    def has_key(self, file_full_path, remote_dst_dir):
        return self.user_dict.has_key(self.create_key(file_full_path, remote_dst_dir))

    def __del__(self):
        self.fcache[self.username] = self.user_dict
        self.fcache.close()


# 进度条类
class ProgressBar():
    file_full_path = ''
    config = None

    def __init__(self, file_full_path, config):
        self.config = config
        self.first_call = True
        self.file_full_path = file_full_path

    def __call__(self, *args, **kwargs):
        if self.first_call:
            self.widgets = ['\n' + self.file_full_path, ' |', progressbar.Percentage(), ' ',
                            progressbar.Bar(marker=progressbar.RotatingMarker('>')),
                            ' ', progressbar.ETA()]
            self.pbar = progressbar.ProgressBar(widgets=self.widgets, maxval=kwargs['size']).start()
            self.first_call = False

        if kwargs['size'] <= kwargs['progress']:
            self.pbar.finish()
        else:
            last_interval = time.time() - self.pbar.last_update_time
            if (last_interval >= self.config.bar_update_interval):
                self.pbar.update(kwargs['progress'])


# 打印对象下的属性值
def prn_obj(obj):
    print '\n'.join(['%s:%s' % item for item in obj.__dict__.items()])


class Result():
    def __init__(self, success, data=UploadConfig('', ''), error='', message=''):
        self.success = success
        self.data = data
        self.error = error
        self.message = message


if __name__ == '__main__':
    baidu_uploader = BaiduUploader()
    try:
        baidu_uploader.start_upload()
    except KeyboardInterrupt:
        print("stopped by hand")
        baidu_uploader.thread_pool.shutdown(False)
        os._exit(-1)
