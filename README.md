* 某app推荐系统

1. process主要声明项目的基函数
2. svdpp.py为项目离线主进程，负责应用svd++算法将推荐结果序列化到本地
3. web.py为推荐服务，应用python为服务flask，接收用户id，为后端服务器返回推荐结果
4. uwsgi为高可用高并发的服务器托管项目，test.ini为uwsgi的配置文件
