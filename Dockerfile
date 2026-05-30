FROM alpine:3.11
COPY bin/app /app
EXPOSE 7000
CMD ["/app"]