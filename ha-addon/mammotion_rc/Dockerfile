# Mammotion RC add-on image.
#
# Supervisor passes BUILD_FROM (an HA Alpine base). We deliberately don't use
# it: PyMammotion 0.8.9 requires Python >=3.13,<3.15, which the HA bases don't
# ship. python:3.13-slim has prebuilt aarch64/amd64 wheels for every runtime
# dependency (cryptography, uvloop, httptools), so no compiler is needed.
ARG BUILD_FROM
FROM python:3.13-slim

# Cache-bust token: GitHub's tarball responses carry no Last-Modified, so the
# classic Docker builder may silently reuse a cached ADD layer and "rebuild"
# with stale code. Changing this ARG's value invalidates the cache from here
# down, forcing a fresh download. Bump it (any new value — we use the repo
# short SHA) together with the config.yaml version on every deploy.
ARG CODE_REV=e26fb8a

# Snapshot of the repo's main branch (re-fetched whenever CODE_REV changes).
ADD https://github.com/karlisakis/mammotion-rc/archive/refs/heads/main.tar.gz /tmp/src.tar.gz

RUN mkdir -p /opt/mammotion-rc \
 && tar -xzf /tmp/src.tar.gz -C /opt/mammotion-rc --strip-components=1 \
 && rm /tmp/src.tar.gz \
 && pip install --no-cache-dir -r /opt/mammotion-rc/web-server/requirements.txt

COPY run.sh /run.sh
RUN chmod +x /run.sh
CMD ["/run.sh"]
