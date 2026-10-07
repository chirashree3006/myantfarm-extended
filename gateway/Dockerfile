# Gateway image: Nginx with the config, the Side B shop and the ops console baked in
# (no bind mounts, so it works the same on Docker Desktop, Codespaces and cloud VMs).
FROM nginx:1.27-alpine
COPY nginx.conf /etc/nginx/nginx.conf
COPY www /usr/share/nginx/html
# nginx workers run as the unprivileged "nginx" user: make everything readable
RUN chmod -R a+rX /usr/share/nginx/html && chmod a+r /etc/nginx/nginx.conf
