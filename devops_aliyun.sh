#./gradlew clean assemble
#now=$(date +%Y%m%d%H%M%S)
image_name=registry.cn-hangzhou.aliyuncs.com/liufeng666/ifeng-service-python
echo $image_name
docker login --username=tb456403_00 --password=FengLiu@24 registry.cn-hangzhou.aliyuncs.com
docker buildx build --platform linux/amd64 --no-cache -t $image_name .
docker push $image_name