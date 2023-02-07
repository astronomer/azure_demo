FROM quay.io/astronomer/astro-runtime:5.0.6

USER root
RUN apt-get update -y && apt-get install -y git
RUN apt-get install -y --no-install-recommends \
        build-essential \
        libsasl2-2 \
        libsasl2-dev \
        libsasl2-modules \
        gnupg

RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
&& curl https://packages.microsoft.com/config/debian/11/prod.list >> /etc/apt/sources.list.d/mssql-release.list \
&& apt update && ACCEPT_EULA=Y apt install -y msodbcsql18

USER astro