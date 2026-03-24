FROM python:3.12-slim
ARG PIP_INDEX_URL

USER root

ADD dockerbuild/api-requirements.txt /api-requirements.txt

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    python3-dev \
    && pip install --no-cache-dir -r /api-requirements.txt \
    && pip install --no-cache-dir "https://github.com/moodysanalytics/cappy.git" \
    && apt-get clean -y \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /Hanmi
ENV PYTHONPATH=/Hanmi

COPY model /Hanmi/model
COPY config /Hanmi/config
COPY commonfastapi /Hanmi/commonfastapi
COPY runner /Hanmi/runner
COPY conf /Hanmi/conf
COPY meta /Hanmi/meta
COPY requirements.txt /Hanmi/requirements.txt

RUN chmod -R a+rX /Hanmi

EXPOSE 8080
CMD ["uvicorn", "runner.api.main:app", "--host", "0.0.0.0", "--port", "8080"]
