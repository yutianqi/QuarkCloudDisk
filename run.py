import time

from quark_client import QuarkClient


def get_time_str():
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())


def progress_callback(progress, message=""):
    print(f"{get_time_str()} [{progress}] {message}")


# 创建客户端（首次使用会自动引导登录）
with QuarkClient() as client:
    # 检查登录状态
    if not client.is_logged_in():
        client.login()  # 自动打开二维码登录

    # client.upload_file("QrCode.jpg", progress_callback=progress_callback)
    client.upload_file("/home/ubuntu/code/FilesCompare/data/remote.dajun.db.gz", progress_callback=progress_callback)
    print("finished uploading")




