#! /usr/bin/env python
# coding:utf-8
"""
Finding duplicate files
重复文件查找

python  dupFinder.py dir1 dir2
"""
__author__ = 'sdm'
import hashlib
import sys
import os
from os.path import isfile
from os.path import exists
import zlib
# todo 多进程

# 1MB
blocksize = 1024 ** 2


def sizeof_fmt(num, suffix='B'):
    for unit in ['', 'Ki', 'Mi', 'Gi', 'Ti', 'Pi', 'Ei', 'Zi']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)


def sha1_file(path):
    """
    hashfile
    :param path:
    :type path:
    :param blocksize:
    :type blocksize:
    :return:
    :rtype:
    """
    fp = open(path, 'rb')
    md5 = hashlib.sha1()
    buf = fp.read(blocksize)
    while len(buf) > 0:
        md5.update(buf)
        buf = fp.read(blocksize)
    fp.close()
    return md5.hexdigest()


# CRC32 计算 CRC32
def crc32_file(filename):
    """提供要计算 CRC32 值的文件路径、文件大小及每次读取的文件块大小
    返回整型类型的 CRC32 值
    """
    crc = 0
    fp = open(filename, 'rb')
    buf = fp.read(blocksize)
    while len(buf) > 0:
        crc = zlib.crc32(buf, crc)
        buf = fp.read(blocksize)
    fp.close()
    return crc


# 快速 sample hash
def sample_hash_file(filename):
    """快速 sample hash
    """
    crc = 0
    fp = open(filename, 'rb')
    buf = fp.read(blocksize)

    while len(buf) > 0:
        crc = zlib.crc32(buf, crc)
        buf = fp.read(blocksize)
    fp.close()
    return crc


import time

start_time = time.time()
stats = {
    'total': 0,
    'process': 0,
    'process_total': 0,
    'total_size': 0,
    'same_size': 0,
    'empty': 0,
    'crc': 0,
    'count_sha1': 0,
    'sha1': 0,
    'time': 0,
}

debug_crc = False
size_map = {}

empty_file = []


def find_files(folders):
    all_files = get_all_files(folders)
    for size, files in find_same_size_files(all_files):
        # 先进行sha1 计算

        if debug_crc:
            print 'dup crc files,size:', size, 'len:', len(files)

        for crc, files2 in find_dup_crc_files(files):

            if debug_crc:
                print '===crc:%11s==' % crc
                for f in files2:
                    print '  %s' % f
                print

            for sha1, files3 in find_dup_sha1_files(files2):
                print '===size:%5s,sha1:%11s==' % (sizeof_fmt(size), sha1)
                for f in files3:
                    print '  %s' % f
                print

            print


def show_files(c):
    sys.stderr.write('\rfind file:%s' % c)
    sys.stderr.flush()


def get_progress(total, p1):
    col = 40
    len1 = col * p1 / total
    len2 = col - col * p1 / total
    s = '%2d%%' % (100 * p1 / total)
    pass_time = time.time() - start_time
    v = pass_time / (1.0 * p1 / total)
    need_time = '剩余:%.1fs,消耗:%.1fs' % ( v * (1.0 - 1.0 * p1 / total), pass_time)
    return '\r%s%s[%s]time:[%s]' % ( '>' * len1, '=' * len2, s, need_time)


def show_process():
    sys.stderr.write(get_progress(stats['process_total'], stats['process']))

    # time.sleep(1)
    sys.stderr.flush()


def find_dup_sha1_files(files):
    sha1_files = {}

    for f in files:
        sha1 = sha1_file(f)
        stats['count_sha1'] += 1

        # sys.stderr.write('%.3f' % (1.0*stats['count_sha1'] / stats['crc'],))

        if not sha1 in sha1_files:
            sha1_files[sha1] = []
        sha1_files[sha1].append(f)

    for sha1, files2 in sha1_files.items():
        # print 'debug sha1:', sha1, 'len', len(files2), files2
        if len(files2) == 1:
            continue
        stats['sha1'] += len(files2)
        yield sha1, files2


def find_dup_crc_files(files):
    sha1_files = {}
    for f in files:
        crc = crc32_file(f)
        stats['process'] += 1
        show_process()
        if not crc in sha1_files:
            sha1_files[crc] = []
        sha1_files[crc].append(f)
        stats['crc'] += 1

    for crc, files2 in sha1_files.items():
        if debug_crc:
            print 'debug crc:', crc, 'len', len(files2), files2
        if len(files2) == 1:
            continue

        yield crc, files2


def get_all_files(folders):
    """
    列出这些文件夹的文件
    :param folders:
    :type folders:
    :return:
    :rtype:
    """
    for path in folders:
        if not exists(path):
            continue

        for dirName, subdirs, fileList in os.walk(path):

            for filename in fileList:

                # Get the path to the file
                path = os.path.join(dirName, filename)
                if not isfile(path):
                    continue
                # 获取文件大小,大小不同的不用计算大小
                size = os.path.getsize(path)
                stats['total'] += 1
                stats['total_size'] += size
                if size == 0:
                    empty_file.append(path)
                    stats['empty'] += 1

                    continue

                yield size, path
            show_files(stats['total'])


def find_same_size_files(all_files):
    """
    找到文件相同的文件
    :param path:
    :type path:
    :return:
    :rtype:
    """

    for size, path in all_files:
        if not size in size_map:
            size_map[size] = []
        size_map[size].append(path)
    sort_size = []
    for size, files in size_map.items():
        # 文件大小唯一,没有相同的
        if len(files) == 1:
            continue
        stats['same_size'] += len(files)
        stats['process_total'] += len(files)
        sort_size.append(size)

    sorted(sort_size, reverse=True)
    for size in sort_size:
        yield size, size_map[size]


def printResults(dict1):
    results = list(filter(lambda x: len(x) > 1, dict1.values()))
    if len(results) > 0:
        print('Duplicates Found:')
        print('The following files are identical. The name could differ, but the content is identical')
        print('___________________')
        for result in results:
            for subresult in result:
                print('\t\t%s' % subresult)
            print('___________________')

    else:
        print('No duplicate files found.')


def main():
    if len(sys.argv) > 1:

        folders = sys.argv[1:]

        find_files(folders)
        print 'empty file count:', len(empty_file)
        for f in empty_file:
            print "  %s" % f

        stats['time'] = '%.3fs' % (time.time() - start_time)
        stats['total_size_human'] = sizeof_fmt(stats['total_size'])
        print 'stats:'
        print stats
    else:
        print('Usage: python dupFinder.py folder or python dupFinder.py folder1 folder2 folder3')


def test():
    from time import time

    for i in range(2):
        for f in sys.argv[1:]:
            t1 = time()
            print crc32_file(f)
            t2 = time()

            t3 = time()
            print sha1_file(f)
            t4 = time()
            print 'crc:', t2 - t1, 'sha1:', t4 - t3


if __name__ == '__main__':
    main()

