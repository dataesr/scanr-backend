#!/bin/bash
# this script can be executed as is, or docker commands copied from it and executed independently as required

for container in `docker ps -a -f "name=scanr-nginx" --format "{{.ID}}"`; do
    echo "Destroying previously created container" ${container}
    docker stop ${container} || true
    docker rm -f ${container}
done
# Do not use docker-gen in preprod because of proxy used in SI Sword; We need a nginx configuration managed by hand and not autogenerated - Else http redirect to https and loop indefinitely
#for container in `docker ps -a -f "name=nginx-gen" --format "{{.ID}}"`; do
#    echo "Destroying previously created container" ${container}
#    docker stop ${container} || true
#    docker rm -f ${container}
#done
for container in `docker ps -a -f "name=nginx-letsencrypt" --format "{{.ID}}"`; do
    echo "Destroying previously created container" ${container}
    docker stop ${container} || true
    docker rm -f ${container}
done

sudo docker run -d \
--name scanr-nginx \
--net=scanr \
-p 80:80 -p 443:443 \
-v /usr/share/nginx/html:/usr/share/nginx/html \
-v /etc/nginx/certs:/etc/nginx/certs:ro \
-v /etc/nginx/vhost.d:/etc/nginx/vhost.d \
-v /etc/nginx/conf.d:/etc/nginx/conf.d \
-v /etc/nginx/htpasswd:/etc/nginx/htpasswd \
-v /var/log/scanr-nginx:/var/log/nginx \
--restart=always \
nginx:1.21.5

# See comment above
#sudo docker run -d --restart=always --name nginx-gen --volumes-from nginx -v /etc/docker-gen/templates/nginx.tmpl:/etc/docker-gen/templates/nginx.tmpl:ro -v /var/run/docker.sock:/tmp/docker.sock:ro jwilder/docker-gen -notify-sighup nginx -watch -only-exposed -wait 5s:30s /etc/docker-gen/templates/nginx.tmpl /etc/nginx/conf.d/default.conf

sudo docker run -d \
--name nginx-letsencrypt \
-e "NGINX_DOCKER_GEN_CONTAINER=nginx-gen" \
--volumes-from scanr-nginx \
-v /etc/nginx/certs:/etc/nginx/certs:rw \
-v /var/run/docker.sock:/var/run/docker.sock:ro \
--restart=always \
jrcs/letsencrypt-nginx-proxy-companion

sudo docker network create scanr
sudo docker network connect scanr scanr-nginx
sudo docker network connect scanr-private scanr-nginx
