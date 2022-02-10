如何构建自己的镜像
1： 当我们写好Dockerfile后
 - docker build -t xxx:xxx .
 - docker images // 查看镜像
 - docker save e7e5f02d5440 > xxx.tar // 将镜像导出为文件
   - dockerfile 里面要 ENTRYPOINT ["/opt/sql-gateway/bin/sql-gateway.sh"]这样执行，不在前端执行任务是启动不了容器的
 - docker run --name sql-gateway -p 8083:8083 -d sql-gateway:1.13

2: 配置里面不要出现localhost，要写ip，例如 sql-gateway里面的hivecatalog需要连接宿主机的mysql，那就写宿主机的ip，不要写localhost等
3: core-site 的 fs.default.name 地址启动集群的时候不要填localhost，要用 0.0.0.0代替 。 在容器里面要用 宿主机的ip代替
