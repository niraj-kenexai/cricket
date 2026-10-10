FROM astrocrpublic.azurecr.io/runtime:3.3-9

USER root
RUN apt-get update && apt-get install -y git && apt-get clean && rm -rf /var/lib/apt/lists/*
USER astro
