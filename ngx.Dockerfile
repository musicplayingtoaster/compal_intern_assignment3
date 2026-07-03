FROM nginx:latest

COPY --chmod=0744 ./nginx/nginx.conf /etc/nginx/nginx.conf

COPY --chmod=0744 ./nginx/src/static /usr/share/nginx/html

EXPOSE 3000
