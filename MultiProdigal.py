import os
import logging
import subprocess
from concurrent.futures import ThreadPoolExecutor
from shutil import rmtree
import re
import sys
import time

def make_dir(directory):
    if not os.path.exists(directory):
        os.mkdir(directory)


def remove_dir(directory):
    if os.path.exists(directory):
        rmtree(directory)

#获取fasta文件的序列名称        
def gain_reads(ASSEMBLY_DIR):

    basenames = []
    reads_suf = ''
    interleaved_reads = None
    pattern = re.compile(r'(.*)(\.fa|\.fasta)(\.gz)?')
    for read in os.listdir(ASSEMBLY_DIR):
        # 获取完整路径
        full_path = os.path.join(ASSEMBLY_DIR, read)
        # 如果它是一个文件夹，跳过这个循环
        if not os.path.isfile(full_path):
            continue
        match_res = pattern.match(read)
        reads_suf = ''.join(match_res.groups('')[1:])
        basename = match_res.group(1)
        basenames.append(basename)
    return basenames

def prodigal_meta(fasta, basename, outdir):
    cmd_para = [
                'prodigal', '-q',
                '-i', fasta,
                '-p', 'meta',
                '-a', os.path.join(outdir, basename + '.faa'),
                '-d', os.path.join(outdir, basename + '.ffn'),
                '-o', os.path.join(outdir, basename + '.gbk')
                ]
    cmd = ' '.join(cmd_para)
    try:
        logging.info('ORFs prediction by prodigal')
        logging.info(cmd)
        os.system(cmd)
    except:
        logging.exception("Something wrong with prodigal annotation!")

def multiProdigal(ASSEMBLY_DIR,OutDir,max_workers):
        
        #获取fasta文件的序列名称
        BASENAMES = gain_reads(ASSEMBLY_DIR)

        # 定义一个线程池
        executor = ThreadPoolExecutor(max_workers)  # 可以更改 max_workers 以设置最大线程数

        # 存储多个线程任务
        tasks = []
        logging.info('ORFs prediction'.center(50, '*'))
        make_dir(OutDir)
        for bn in BASENAMES:
            logging.info("Prediction ORFs for {}".format(bn))
            fasta = os.path.join(ASSEMBLY_DIR, bn + '.fa')
            
            # 将任务提交到线程池
            task = executor.submit(prodigal_meta, fasta, bn, OutDir)
            tasks.append(task)

        # 等待所有任务完成
        for task in tasks:
            try:
                task.result()
            except Exception as e:
                logging.error(e)

        # 关闭线程池
        executor.shutdown()

def main():
    ASSEMBLY_DIR = sys.argv[1]#输入文件夹
    OutDir = sys.argv[2]#输出文件夹
    max_workers = int(sys.argv[3])#线程数
    start_time = time.time()
    multiProdigal(ASSEMBLY_DIR,OutDir,max_workers)
    end_time = time.time()
    run_time = end_time - start_time
    print(f"Time：{run_time/60} mins")
    print("Prodigal is done!")

#传入三个参数 第一个是输入文件夹 第二个是输出文件夹 第三个是线程数
if __name__ == '__main__':
    main()




