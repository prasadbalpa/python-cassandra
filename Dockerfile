FROM python:2-onbuild
RUN apt-get update && apt-get install -y uuid-runtime && apt-get install -y net-tools
RUN apt-get update && apt-get install -y vim
RUN mkdir /usr/src/tests
WORKDIR /usr/src/tests
ADD . /usr/src/tests
CMD ["python", "forever.py"]
