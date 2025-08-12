#!/bin/bash

# 设置环境变量
ALIYUN_REGISTRY="registry.cn-hangzhou.aliyuncs.com"
ALIYUN_NAMESPACE="liufeng666"
IMAGE_NAME="ifeng-service-python"
TAG="latest"

# 登录阿里云容器镜像服务
echo "登录阿里云容器镜像服务..."
docker login --username=tb456403_00 --password=FengLiu@24 ${ALIYUN_REGISTRY}

# 强制删除旧容器（如果存在）
echo "删除旧容器..."
docker rm ${IMAGE_NAME} -f || true

# 拉取最新镜像
echo "拉取最新镜像..."
docker pull ${ALIYUN_REGISTRY}/${ALIYUN_NAMESPACE}/${IMAGE_NAME}:${TAG}

# 运行新容器
echo "启动新容器..."
docker run --name ${IMAGE_NAME} \
    -d \
    --restart=always \
    -v $(pwd)/logs:/app/logs \
    ${ALIYUN_REGISTRY}/${ALIYUN_NAMESPACE}/${IMAGE_NAME}:${TAG}

echo "部署完成！"