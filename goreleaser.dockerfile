FROM gcr.io/distroless/static:nonroot

COPY external-dns-pscloud-webhook /external-dns-pscloud-webhook
ENTRYPOINT ["/external-dns-pscloud-webhook"]

